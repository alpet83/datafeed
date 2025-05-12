<?php    
    require_once "loader_proto.php";    
    require_once "blocks_loader.php";
    

    # NOTE: most timestamps are in ms
    
    const TICK_TMS = 0;
    const TICK_BUY = 1;
    const TICK_PRICE = 2;    
    const TICK_AMOUNT = 3;
    const TICK_TRADE_NO = 4;

    class  TicksCache extends DataBlock  {

        /**
         * candle_map - карта минутных свечей, для контроля заполнения по объему и времени. Ключом является время в МИНУТАХ от начала блока
         * @var array
         */
        public     array $candle_map = [];  


        public     $daily_candle = null;  // для хранения дневной свечи
        public     $minutes_volume = 0; // for reconstruction need check

        public     array $refilled_vol = [];
        public     array $unfilled_vol = [];

        public function __toString() {
            $res = parent::__toString();
            $res .= ', CM: '.count($this->candle_map);
            $res .= ', UF: '.count($this->unfilled_vol); 
            return $res;
        }

        public  function AddRow(int $tms, bool $buy, float $price, float $amount, string $trade_no, string $key = null) {
            $rec = [$tms];
            $rec[TICK_BUY] = $buy;
            $rec[TICK_PRICE] = $price;
            $rec[TICK_AMOUNT] = $amount;
            $rec[TICK_TRADE_NO] = $trade_no; // standard row includes timestamp, due non-unique            
            if (null == $key)
                $key = $trade_no;          
            $this->SetRow($key, $rec);
        }
       
        /** @param float $t - timestamp in seconds         
         *  return minute from start of block
         */
        private function candle_key(float $t): int {
            return floor(($t - $this->lbound) / 60);
        }

        private function candle_tms(int $mk): int {
            return ($this->lbound + $mk * 60) * 1000;
        }

        protected function CheckFillMinute(int $m, float $amount, bool $is_dup) {        
            if (!isset($this->unfilled_vol[$m]))
                return;

            $cm = $this->candle_map;
            if (!isset($cm[$m])) 
                log_cmsg("~C91#WARN:~C00 CheckFillMinute no candle record for minute %d", $m);                           

            $this->refilled_vol[$m] = ($this->refilled_vol[$m] ?? 0) + $amount; // учитывается заполнение в т.ч. дублирующее
            if ($is_dup) return;            
            $this->unfilled_vol[$m] = ($this->unfilled_vol[$m] ?? 0) - $amount;                         
            if ($this->unfilled_vol[$m] <= 0) 
                unset($this->unfilled_vol[$m]);
        }
        public function Count(): int {
            return count($this->cache_map);
        }

        public function IsFullFilled(): bool {            
            if (0 == count($this->unfilled_vol) && count($this->candle_map) > 0) return true;
            return parent::IsFullFilled();
        }

        public function LeftToDownload(): float {
            if (count ($this->unfilled_vol) > 0)
                return array_sum($this->unfilled_vol);
            return parent::LeftToDownload();
        }

        /** загрузка минутных свечей для контроля консистентности */
        public function LoadCandles() {
            if ($this->index <= -1) return;

            $mysqli = sqli();
            $mysqli_df = sqli_df();
            $loader = $this->owner;
            $table = $loader->table_name;
            $c_table = str_replace('ticks', 'candles', $table);
            if (!$mysqli_df->table_exists($c_table)) return;                                                

            $this->history_bwd = [];
            $this->history_fwd = [];
            
            $inaccuracy = 0.999; // 0.1% погрешность по объему, чтобы не зацикливаться на заполнении минуток 

            $days = round( ($this->rbound - $this->lbound) / SECONDS_PER_DAY);
            if ($days > 1) 
                throw new ErrorException("LoadCandles: too long range for ticks block, days = $days");

            $saldo_cv = 0;
            $bounds = "Date(ts) = '{$this->key}'";
            $m_candles = $mysqli->select_map('ts,open,close,high,low,volume', $c_table, "WHERE $bounds", MYSQLI_NUM); // load from MySQL
            $this->daily_candle = $daily =  $mysqli->select_rows('ts,open,close,high,low,volume', "{$c_table}__1D", "WHERE $bounds", MYSQLI_OBJECT); // load from MySQL

            if ($this->minutes_volume < $this->target_volume) {
                $this->minutes_volume = 0;
                foreach ($m_candles as $ts => $candle)
                    $this->minutes_volume += $candle[CANDLE_VOLUME]; // calc saldo
            }
            
            if (is_object($daily) && $daily->volume > $this->target_volume)                 
                $this->target_volume = $daily->volume;                
            
            //  один из типовых сценариев проблемных данных, когда минуток по объему не хватает до дневок. В этом случае надо дополнить фейковые минутки, хоть это и замедлит потенциально загрузку            
            $vdiff = $this->target_volume - $this->minutes_volume;
            $vdiff_pp = 100 * $vdiff / $this->target_volume;
            $full_scan = $vdiff_pp > 0.01; 
            $full_unfilled = [];

            if (!$m_candles) {
                log_cmsg("~C91 #WARN(LoadCandles):~C00 no 1m data for %s [%s] in MySQL", $c_table, $this->key);
                return;
            }            

            foreach ($m_candles as $ts => $candle) {
                $t = strtotime($ts);
                if ($t < EXCHANGE_START_SEC) {
                    log_cmsg("~C91 #ERROR:~C00 loaded candle with outbound timestamp %s", format_ts($t));
                    continue;
                }
                $key = $this->candle_key($t);
                $this->candle_map[$key] = $candle;
                $cv = $candle[CANDLE_VOLUME];
                $saldo_cv += $cv;
                $full_unfilled[$key] = $cv * $inaccuracy; 
                $this->refilled_vol[$key] = 0; // учитывается заполнение в т.ч. дублирующее
            }
            if ($full_scan) {    
                $fake = 0;            
                for ($m = 0; $m < 1440; $m ++) {
                    if (isset($full_unfilled[$m])) continue;
                    $full_unfilled[$m] = 0.001;
                    $this->candle_map[$m] = [0, 0, 0, 0, 0.0]; // fake candle
                    $this->refilled_vol[$m] = 0;
                    $fake ++;
                }
                if ($fake > 0)
                    log_cmsg("~C33 #FULL_DAY_SCAN:~C00 due insuffcient minutes volume = %.2f%% in %d candles, added %d fake candles for %s", $vdiff_pp, count($m_candles), $fake, $this->key);
            }

            $this->unfilled_vol = $full_unfilled;

            if ($this->db_need_clean) return; // no optimize, need full refill
            $vol_map = $mysqli_df->select_map("(toHour(ts) * 60 + toMinute(ts)) as minute, SUM(amount) as volume", $table, 
                                               "FINAL WHERE $bounds GROUP BY minute ORDER BY minute");
            $abnormal = 0;
            $overhead = 0;
            $saldo = 0;
            $suspicous = [];
            if (0 == count($vol_map)) return; // no ticks preloaded            
            
            foreach ($vol_map as $m => $v) {
                $saldo += $v;
                if (!isset($this->unfilled_vol[$m])) continue;      
                $uv = $this->unfilled_vol[$m] / $inaccuracy;                           
                $cm = $this->candle_map[$m] ?? [0, 0, 0, 0, $uv];                
                $cv = $cm[CANDLE_VOLUME];                                
                if ($v > $cv * 1.005)  {                    
                    $abnormal ++;
                    $overhead += $v - $cv;
                    $this->unfilled_vol[$m] = $cv;   // try full reload
                    $ts = format_ts($m * 60 + $this->lbound);
                    $suspicous [$ts]= [$v, $cv, $uv]; 
                }
                else 
                    $this->unfilled_vol[$m] -= $v;    // для этой минуты тики загружены хотя-бы частично                
                if (!$this->TestUnfilled($m)) 
                    unset($this->unfilled_vol[$m]); // сокращение работы
            }
            
            if (0 == $saldo_cv) return;
            $this->db_need_clean |= $saldo > $saldo_cv; // force reload            
            $can_replace = $loader->data_flags & DL_FLAG_REPLACE;
            $this->db_need_clean  &= $can_replace;
            $pp = 100 * $overhead / $saldo_cv;
            if ($abnormal > 0 && $this->db_need_clean) { // TODO: use volume_tolerance
                $mgr = $this->owner->get_manager();
                $ticker = $this->owner->ticker;
                $fname = "{$mgr->tmp_dir}/overhead_{$ticker}_{$this->key}.txt";
                $info = json_encode($suspicous, JSON_PRETTY_PRINT)."\n";                               
                file_put_contents($fname, $info);
                $this->db_need_clean = true;
                $this->unfilled_vol = $full_unfilled; 
                log_cmsg("~C91#WARN:~C00 %3d abnormal minutes for %s, where volume of ticks > volume of candle, and total overhead %.2f%%. Saldo candle vol %s vs saldo ticks vol %s. Block reload planned",    
                                $abnormal, $bounds, $pp, format_qty($saldo_cv), format_qty($saldo)); // TODO: patch candles table with reset volume?  */                

            }
        }


        public function FindRow(int $t, bool $nearest = true): int {                        
            $start = 0;
            $end = count($this->cache_map) - 1;
            $mid = 0;
            $keys = array_keys($this->cache_map);
            while ($start <= $end) {
                $mid = floor(($start + $end) / 2);
                $key = $keys[$mid];
                $row = $this->cache_map[$key];

                if ($row[TICK_TMS] == $t) return $key;
                if ($row[TICK_TMS] < $t) 
                    $start = $mid + 1;
                else 
                    $end = $mid - 1;                    
            }
            $start = min($start, $end); // prevent outbound
            if ($nearest && isset($keys[$start])) 
                return $keys[$start];            
            return -1; // not found
        }

        public function FormatRow (string $key){
            global $mysqli_df;
            $row = $this->cache_map[$key];
            if (!is_array($row)) return "#ERROR: wrong offset #$key";
            $tms = format_tms($row[TICK_TMS]);
            $tms = str_replace(' ', 'T', $tms);
            $row[TICK_TMS] = $tms;
            $cols = array_keys($row);
            $row = $mysqli_df->pack_values($cols, $row, "'");
            return "($row)"; // for INSER INTO
        }
        

        public function SaldoVolume(): float {
            return $this->CalcSummary(TICK_AMOUNT);
        }

        public function SetRow(mixed $key, array $row): int {            
            $is_dup = isset($this->cache_map[$key]);  
            if ($row[TICK_PRICE] <= 0) 
                throw new ErrorException("Invalid tick record, price field must > 0 ".json_encode($row));

            $this->cache_map[$key] = $row;                    
            $t = floor($row[TICK_TMS] / 1000);                            
            if ($is_dup)
                $this->duplicates ++;
            else 
                $this->set_filled($t);            

            if ($this->index >= -1) {  // в общем кэше нет смысла фиксировать минуты
                $m = $this->candle_key($t);
                $this->CheckFillMinute($m, $row[TICK_AMOUNT], $is_dup);                
            }               
                      
            return $this->Count();
        }
    
        public function Store(TicksCache $target): int {
            $min = $target->lbound_ms;
            $max = $target->rbound_ms;
            $stored = 0;
            foreach ($this->cache_map as $key => $rec) {
                $mts      = $rec[TICK_TMS];                
                if ($mts >= $min && $mts <= $max) {                    
                    $target[$key] = $rec;
                    $stored ++;
                }
            }
            $target->OnUpdate();
            return $stored;
        }

        protected function TestUnfilled($m) {
            $cm = $this->candle_map[$m] ?? [0, 0, 0, 0, 0];
            $mv = $cm[CANDLE_VOLUME];
            if ($mv > 0) {
                $rv = $this->refilled_vol[$m] ?? 0;            
                return ($this->unfilled_vol[$m]  > 0) && ($rv < $mv * 1.5);  // чрезмерное покрытие дупликатами, тоже означает заполнение
            }
            else
                return $this->unfilled_vol[$m]  > 0; // это странный вариант, но оставить пока
        } 

        public function UnfilledAfter(): int {
            foreach ($this->unfilled_vol as $key => $v)
                if ($this->TestUnfilled($key)) {
                    $tms = $this->candle_tms($key);
                    if ($this->LoadedForward ($tms, 5000)) continue; // предотвращение повторных результатов
                    if ($tms >= $this->lbound_ms) return $tms;
                    log_cmsg("~C91#ERROR(UnfilledAfter):~C00 for minute key %d produced timestamp %s ", $key, format_tms($tms));
                    break;
                }

            return $this->get_lbound_ms();  // full reload suggest
        }

        public function UnfilledBefore(): int {         
            $rv = array_reverse($this->unfilled_vol, true); // need maximal key
            foreach ($rv as $key => $v) {                
                if ($this->TestUnfilled($key)) {
                    $tms = $this->candle_tms($key + 1); // download need from next minute
                    if ($this->LoadedBackward($tms, 5000)) continue; // предотвращение повторных результатов
                    return $tms;
                }
            }
            return $this->get_rbound_ms(); // full reload suggest
        }
        public function VoidLeft(string $unit = 's'): float {
            if (0 == count($this->candle_map)) return parent::VoidLeft();
            $first = array_key_first($this->candle_map);
            $void = max(0, $this->min_avail - $first * 60); 
            return seconds2u($void, $unit);
        }
        public function VoidRight(string $unit = 's'): float {
            if (0 == count($this->candle_map)) return parent::VoidLeft();
            $last = array_key_last($this->candle_map);
            $void = max(0,  $last * 60 - $this->max_avail); 
            return seconds2u($void, $unit);
        }        

    };


    function sqli_df(): ?mysqli_ex {
        global $mysqli_df;
        return $mysqli_df;
    }

    /** 
     * Базовый класс (прототип) для загрузчика истории тиков/трейдов.
     * Подразумевается основная база хранения ClickHouse, через интерфейс MySQL. 
     * Предназначение предполагает в т.ч. высокочастотную торговлю, поэтому данные WebSocket пишутся в БД без кэширования.
     */

    abstract class TicksDownloader 
        extends BlockDataDownloader {

        protected $scan_year = 0;

        protected bool $reconstruct_candles = true;
            
        public function __construct(DownloadManager $mngr, stdClass $ti) {
            $this->import_method = 'ImportTicks';
            $this->load_method = 'LoadTicks';    
            $this->block_class = 'TicksCache';     
            $this->data_name = 'ticks';       
            parent::__construct($mngr, $ti);                
            $this->cache = new TicksCache($this);
            $this->days_per_block = 1; // в каждом блоке могут быть тысячи тиков, даже за час            
            $this->table_proto = 'clickhouse_ticks_table.sql';                                
            $this->time_precision = 3;
            $this->blocks_at_once = 5;
            $this->max_blocks = 10; // lazy recovery for better latency
        }



        public function CorrectTables (){
            global $mysqli_df;
            // $mysqli_df->try_query("ALTER TABLE {$this->table_name} MODIFY ORDER BY trade_no");
            $table_name = $this->table_name;
            parent::CorrectTables();
            $code = $this->table_create_code_ch;
            if (!is_string($code) || !str_in($code, 'CREATE')) {
                log_cmsg("~C91#WARN(CorrectTable):~C00 not set create code for %s", $table_name);
                return;
            }
            if (str_in($code, 'ReplacingMergeTree(ts)'))
                log_cmsg("~C92#TABLE_OK:~C00 Used actual Engine ");
            else  {
                log_cmsg("~C31#WARN_UPGRADE:~C00 changing engine for table %s from %s", $table_name, $this->table_engine);
                $query = "REPLACE TABLE $table_name ENGINE  ReplacingMergeTree(ts) ORDER BY trade_no PARTITION BY toStartOfMonth(ts)  AS SELECT * FROM $table_name";
                if ($mysqli_df->try_query($query)) 
                    log_cmsg("~C93 #UPGRADED:~C00 %s ", $mysqli_df->show_create_table($table_name));
                else
                    log_cmsg("~C91#FAILED:~C00 query %s", $query);
            }                
            $mysqli_df->try_query("ALTER TABLE $table_name ADD INDEX IF NOT EXISTS ts ts TYPE set(0)  GRANULARITY 16;");
            $mysqli_df->try_query("ALTER TABLE $table_name MATERIALIZE INDEX ts");
            $this->table_corrected = true;
            // $mysqli_df->try_query("ALTER TABLE $table_name ADD INDEX IF NOT EXISTS date DATE(ts) TYPE minmax  GRANULARITY 2");
        }    


        public function db_first(bool $as_str = true, int $tp = 0) {
            if (!is_object($this->table_info))
                $this->QueryTableInfo(sqli_df());  // load metadata                                     
            
            $start = $this->table_info->start_part;
            $params = "WHERE _partition_value.1 <= Date('$start')";
            $ts = strlen($start) >= 10 ? sqli_df()->select_value('MIN(ts)', $this->table_name, $params) : null;
            return $this->db_timestamp($ts, $as_str, $tp);
        }
        public function db_last(bool $as_str = true, int $tp = 0) {
            if (!is_object($this->table_info))
                $this->QueryTableInfo(sqli_df());  // load metadata 
            $end = $this->table_info->end_part;
            $params = "WHERE _partition_value.1 >= Date('$end')";
            $ts = strlen($end) >= 10 ? sqli_df()->select_value('MAX(ts)', $this->table_name, $params) : null;    
            return $this->db_timestamp($ts, $as_str, $tp);
        }

        protected function DayBlock(string $day, float $target_vol = 0, bool $load_candles = true): TicksCache {
            $count = count($this->blocks);
            $sod_t = strtotime($day);    
            $eod_t = $sod_t + SECONDS_PER_DAY - 1;
            $block = $this->CreateBlock($sod_t, $eod_t);               
            $block->code = BLOCK_CODE::NOT_LOADED;            
            $block->index = $count;
            $block->target_volume = $target_vol;
            if ($load_candles)
                $block->LoadCandles();
            return $block;
        }
        
        public function FlushCache(bool $rest = true) {
            $cache_size = count($this->cache);
            $start = $this->cache->oldest_ms();
            $saved = $this->SaveToDB($this->cache);
            $this->rest_loads += $saved;
            $part = date_ms('Y-m-01', $start);
            sqli_df()->try_query("OPTIMIZE TABLE {$this->table_name} PARTITION ('$part') FINAL DEDUPLICATE");
            log_cmsg("~C97#DB_PERF:~C00 saved to DB %d / %d records", $saved, $cache_size);
        }
        public function Elapsed(): float {
            return max(0, time_ms() - $this->newest_ms) / 1000;         
        }

        public   function  get_manager(): ?TicksDownloadManager {
            return $this->manager instanceof TicksDownloadManager ? $this->manager : null;
        }    

        /**
         * Summary of InitBlocks
         * @param int $start  - ms
         * @param int $end    - ms             
         */
        protected function InitBlocks(int $start, int $end) {            

            

            if ($this->data_flags & DL_FLAG_HISTORY == 0) 
                return;            

            if ($start < strtotime(HISTORY_MIN_TS) && !$this->initialized) {
                log_cmsg("~C31#WARN_INIT_BLOCKS:~C00 start timestamp %d: %s, will be increased", $start, gmdate(SQL_TIMESTAMP, $start));
                $start = strtotime(HISTORY_MIN_TS); 
            }

            /*  Сканирование проблем в таблице с тиками дело достаточно долгое, поэтому каждый запуск скрипта оно не обязательно. 
                Все потенциально неправильные дни будут добавлены в расписание, из которого при каждом запуске выбирается столько, сколько успеется. 
            */
            $mgr = $this->get_manager();
            $mysqli = sqli();            
            $mysqli_df = sqli_df();
            $strict = "(ticker = '{$this->ticker}') AND (kind = 'ticks') AND (target_volume > 0)";
            $this->total_scheduled = $mysqli->select_value('COUNT(*)', 'download_schedule', "WHERE $strict");                                            
            $schedule = [];

            $actual_ts = substr(HISTORY_MIN_TS, 0, 10); // date only
            if ($this->total_scheduled > 0)                 
                $schedule = $mysqli->select_map('date, target_volume', 'download_schedule', 
                                                  "WHERE $strict AND (date >= '$actual_ts') ORDER BY date LIMIT {$this->max_blocks}");

            if ($this->total_scheduled > 0 && 0 == count($schedule ?? []))  // scan all scheduled, after actual part loaded
                $schedule = $mysqli->select_map('date, target_volume', 'download_schedule', 
                                             "WHERE $strict ORDER BY date LIMIT {$this->max_blocks}");                                                           
                    
            
            $count = count($schedule);
            if (0 == $count)
                $this->ScanIncomplete($start, $end);
            else {                   
                
                foreach ($schedule as $date => $tv) {                    
                    $this->DayBlock($date, $tv);                                                                      
                    $mgr->ProcessWS(false);
                }
                krsort($this->blocks_map); // oldest block will download at least
                if (0 == $this->BlocksCount())
                    throw new ErrorException(format_color("~C31#ERROR:~C00 no blocks added for %s from %d", $this->ticker, $count));

                $k_first = array_key_first($this->blocks); 
                $k_last = array_key_last($this->blocks);
                $last = $this->blocks[$k_first]->key;
                $first = $this->blocks[$k_last]->key;
                log_cmsg("~C97#INIT_BLOCKS:~C00 selected from schedule %d / %d blocks, from %s to %s ", 
                            count($this->blocks), count($schedule), $first, $last);
            }
            


        }

            

        abstract public function     ImportTicks(array $data, string $source, bool $is_ws = true): ?TicksCache;

        public function ImportWS(mixed $data, string $context): int {
            if (is_string($data))
                $data = json_decode($data);

            if (!is_array($data) || 0 == count($data)) {
                log_cmsg("~C91#IMPORT_RAW_FAILED:~C00 invalid data type %s", var_export($data, true));
                return 0;
            }
            $ticks = $this->ImportTicks($data, "WebSocket$context");
            $imp = is_object($ticks)? count($ticks) : 0;
            $msg = format_color("from WebSocket imported %d:%s with source size %d ticks, first: %s\n", $imp, gettype($data), count($data), json_encode($data[0]));
            if (0 == $imp && count($data) > 0)
                log_cmsg("~C91#IMPORT_WARN~C00: $msg");
            
            file_put_contents( $this->ws_log_file, $msg);
            $this->ws_loads += $imp;
            return $imp;
        }

            // function LoadBlock ----------------------------------------------------------------------------

        abstract public function LoadTicks(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 10000): ?array; 

        protected function OnBeginDownload(DataBlock $block) {                
            $max = 0;
            $min = 1440;
            foreach ($block->unfilled_vol as $m => $v) {
                $cr = $block->candle_map[$m] ?? [0, 0, 0, 0, 0];
                $mv = $cr[CANDLE_VOLUME] ?? 0;
                if ($v > $mv * 0.05) {
                    $max = max($max, $m);
                    $min = min($min, $m);
                }                   
            }
            $id = "{$block->index}:{$block->key}";

            $tag = $block->db_need_clean ? '~C04~C97#BLOCK_START_RELOAD' : '~C93#BLOCK_START_LOAD';
            if ($block->index >= 0)
                log_cmsg(" $tag({$this->ticker}):~C00 %s filled_vol min %d / max %d ", $id, $min, $max);
            $uf = ( $block->UnfilledBefore() - $block->lbound_ms ) / 1000;
            if ($max > 120 && $uf < 3600) {
                $vals = array_values($block->unfilled_vol);
                $vals = array_reverse($vals);                    
                $vals = array_slice($vals, 0, 10);
                log_cmsg(" ~C91#WARN:~C00 UnfilledBefore returned too small gap %.3f, reversed volume vector (10): %s ", $uf, json_encode($vals));                    
                // throw new ErrorException("Critical");
            }
        }

        public function OnBlockComplete(DataBlock $block) {
            if ($block->code == $block->reported) return;
            $this->loaded_blocks ++;                           
            $block->reported = $block->code;
            $filled = '~C43~C30['.$block->format_filled().']~C00';                    
            $lag_left = $block->min_avail - $block->lbound;
            $index = $block->index;
            if ($index >= 0 && $lag_left > 60)
                $block->FillDummy();
            $this->last_error = '';
            $tmp_dir = $this->get_manager()->tmp_dir.'/blocks';
            check_mkdir($tmp_dir);
            $prefix = ' ~C93#BLOCK_COMPLETE';
            if ($block->covers($this->history_first)) {
                $this->head_loaded = true;
                $prefix = ' ~C04~C93#HISTORY_COMPLETE';
            }
            $symbol = $this->symbol;                                
            $day = $block->key;                
            if (count($this->cache) > 0) {
                log_cmsg(" ~C31#WARN_HAVE_CACHED:~C00 expected all cache must be flushed, before procesinng block completion. Called from: %s ", 
                            format_backtrace());
                $this->FlushCache(); // TODO: check completion calling after
            }

            $saldo_volume = $block->SaldoVolume();                
            $check = sqli_df()->select_row('DATE(ts) as date, SUM(amount) as volume, MIN(price) as low, MAX(price) as high, COUNT(*) as count', $this->table_name, 
                                "FINAL WHERE DATE(ts) = '$day' AND amount > 0 GROUP BY DATE(ts) LIMIT 1; -- check", MYSQLI_OBJECT);

                                             
            $check_volume = is_object($check) ? $check->volume : $saldo_volume;                
            $check_date = is_object($check) ? $check->date : '<null>';
            $count = is_object($check) ? $check->count : count($block);           

            $s_info = 'postponed for verify/retry, ';

            $attempts = sqli()->select_value('COUNT(*)', 'download_history', 
                                              "WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks') AND (date = '$day')"); 

                                               
        
            $db_diff = $saldo_volume - $check_volume;                 
            $db_diff_pp = 100 * $db_diff / max($check_volume, $saldo_volume, 0.01);
            if (is_object($check) && $db_diff_pp > 0.5) 
                log_cmsg(" ~C31#WARN_DB_PROBLEM:~C00 %s, count in DB %d vs count in block %d, volume %s vs %s (diff %.1f%%)",
                            $check_date, $count, count($block),
                            format_qty($check_volume), format_qty($saldo_volume), $db_diff_pp);                
                                        
            $diff = $block->target_volume - $check_volume;                 
            $diff_pp = 100 * $diff / max($block->target_volume, $check_volume, 0.01);
            $whole_load = false;
            $recv = 0;
            $need_recovery = $check_volume > min($block->target_volume, $block->minutes_volume);
            if ($this->reconstruct_candles && $block->index >= 0 && $need_recovery)
                $recv = $this->RecoveryCandles($block, $check_volume,true);  // recovery absent minute candles

            // избавиться от повтора загрузки, при наличии приличий...    
            if (abs($diff_pp) < $this->volume_tolerance || $attempts >= 2 || $recv > 0)  {
                sqli()->try_query("DELETE FROM `download_schedule` WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks') AND (date = '$day')");                
                $s_info = '';
            }    

           
            $load_result = 'full-filled';
            // для тиков целевой объем должен быть меньше или равен набранному, плюс минус погрешность-толерантность
            // исключение: удалось произвести апгрейд свечей 
            $inaccuracy = 0.01; // погрешность суммирования float с очень разным порядком 0.01%
            if (abs($diff_pp) < $this->volume_tolerance && ($recv > 0 || $diff_pp + $inaccuracy >= 0)) {
                $whole_load = true;
                log_cmsg("$prefix($symbol/$index/$attempts):~C00 %s, lag left %5d, %s,\n\t\ttarget_volume reached %s in %d ticks, block summary %s, filled in session: %s ", 
                            strval($block), $lag_left, $block->info, format_qty($check_volume), $count, json_encode($check), $filled);    
                if (-1 == $block->index)
                     $this->RecoveryCandles($block, $check_volume);  // upgrade latest
                
            } elseif ($block instanceof TicksCache) {
                $whole_load = $diff < 0;
                $problem = $diff > 0 ? 'PARTIAL' : 'EXCESS';
                if (0 == $check_volume)
                    $problem = 'VOID';
                $load_result = sprintf('%s diff %.2f%%', $problem, $diff_pp);

                log_cmsg("~C04{$prefix}_$problem($symbol/$index/$attempts):~C00 %s, lag left %d, %s, target_volume reached %s (saldo %s) instead %s daily (diff %.2f%%) and %s minutes,\n\t\t $s_info block summary %s, filled in session: %s ", 
                            strval($block), $lag_left, $block->info, 
                                format_qty($check_volume), format_qty($saldo_volume),
                                format_qty($block->target_volume), $diff_pp, format_qty($block->minutes_volume),
                                json_encode($check), $filled);                    
                $fk = [];
                $uk = [];
                $vk = [];
                $ukc = count($uk);
                $saldo_uf = 0;                          
                $threshold = $block->target_volume / 144000; // 1 % from average minute volume                
                foreach ($block->candle_map as $mk => $cr) {
                    $rf = $block->refilled_vol[$mk] ?? 0;
                    if (isset($block->unfilled_vol[$mk])) {
                        $v = $block->unfilled_vol[$mk];
                        if ($v > $threshold)
                            $uk [$mk]= format_qty($v);
                        $saldo_uf += $v;
                    }
                    elseif ($rf > $threshold)
                        $fk [$mk]= format_qty($rf);  
                }                    

            
                $mdiff = $check_volume - $block->minutes_volume;
                $mdiff_pp = 100 * $mdiff / max($check_volume, $block->minutes_volume, 0.01);
                if (abs($mdiff_pp) < 0.1) {
                    log_cmsg("~C31 #API_HISTORY_GAP:~C00 ticks volume looks near to minute candles volume, diff = %.2f%%", $mdiff_pp);
                    $whole_load = true;
                    $load_result = 'same as minutes';
                }
                elseif ($ukc > 0 && $diff_pp > 30) {
                    $load_result .= " UF:$ukc";
                    foreach ($block->candle_map as $mk => $cr)
                        $vk [$mk]= $cr[CANDLE_VOLUME];                                                             
                    log_cmsg(" ~C94#FILLED_DUMP: ~C00 F: %s \n U: %s = %s \n CM: %s",
                                    json_encode($fk), format_qty($saldo_uf), json_encode($uk), json_encode($vk));                                
                }
                if (abs($diff_pp) >= 1) {                    
                    $info = format_color("Problem: volume diff = %s %.2f%%, relative target %s and minutes %s", 
                                        format_qty($diff), $diff_pp, format_qty($block->target_volume), format_qty($block->minutes_volume));
                    $info .= "\nUnfilled: ".print_r($uk, true);
                    $info .= "\nRefilled: ".print_r($fk, true);
                    $info .= "\nCandles: ".print_r($vk, true);
                    file_put_contents("$tmp_dir/bad_ticka-{$this->ticker}_{$day}.txt", $info);
                }
            }

            if (is_object($check)) {
                $load_result = substr($load_result, 0, 32);
                $query = "INSERT INTO `download_history` (`ts`, `date`, `kind`, `ticker`, `count`, `volume`, `result`)\n VALUES";
                $query .= sprintf("(NOW(), '$day', 'ticks', '%s', %d, %f, '%s')", 
                                 $this->ticker, $check->count, $check->volume, $load_result);
                sqli()->try_query($query); // по сути это журнал загрузки. Сверка с ним, позволит избежать повторов без ручной очистки                                
            }
            $block->Reset($whole_load); // clean memory
        }

        protected function OnCacheUpdate(DataBlock $default, DataBlock $cache) {         
            $ublocks = [];           
            if (0 == count($cache)) return;

            $bmp = $this->blocks_map;

            foreach ($cache->keys as $key) {
                $tick = $cache[$key];
                $tms = $tick[0];                    
                $this->oldest_ms = min($this->oldest_ms, $tms); // in ms
                $this->newest_ms = max($this->newest_ms, $tms); // in ms
                $day = gmdate ('Y-m-d', $tms);                
                $block = $bmp[$day] ?? $default;                                          
                if ($block && $block->covers_ms($tms)) {
                    $block[$key] = $tick; // add             
                    $ublocks["{$block->index}:{$block->key}"] = $block; // save updated
                }            
            }
            
            $oldest_ms = $cache->oldest_ms();
            $newest_ms = $cache->newest_ms();
            if (count($ublocks) > 1)
                log_cmsg("\t~C96#CACHE_UPDATE:~C00 %s catched blocks %d: %s", $cache->key, count($ublocks), implode(", ", array_keys($ublocks)));

            // тут проводятся проверки на маленьком кэше в основном, с последних загруженных данных
            foreach ($ublocks as $k => $block)
                if (BLOCK_CODE::FULL != $default->code && $block->IsFullFilled()) {
                    $block->code = BLOCK_CODE::FULL;
                    $block->info = "fullfilled by cache {$cache->key}";
                    unset($ublocks[$k]);
                }

            foreach ($ublocks as $k => $block) 
                if (BLOCK_CODE::FULL != $default->code && $block->Covered_by($oldest_ms, $newest_ms)) {
                    $block->code = BLOCK_CODE::FULL;
                    $block->info = "overlap by cache {$cache->key}";
                    unset($ublocks[$k]);
                }            
        }

        public function QueryTimeRange(): array {
            global $mysqli_df;                
            $info = $this->QueryTableInfo($mysqli_df);   
            $result = [false, false];
            if (is_object($info) && $info->size > 0) {
                $this->table_info = $info;
                $result = [$info->min_time, $info->max_time];
                $this->saved_min = strtotime($info->min_time);
                $this->saved_max = strtotime($info->max_time);                    
            }
            else {
                $this->table_info = null;
                $this->saved_min = 0;
                $this->saved_max = time();
            }                     
            return $result;                            
        }

        protected function ReconstructCandles(array $source): array {
            $c_map = []; // candles result            
            foreach ($source as $tick) {
                $tk = floor($tick[TICK_TMS] / 60000) * 60; // round to minute into seconds                
                $tp = $tick[TICK_PRICE];
                $tv = $tick[TICK_AMOUNT];
                if (!isset($c_map[$tk])) 
                    $c_map[$tk] = [$tp, $tp, $tp, $tp, $tv]; // all prices equal OCHL
                else { 
                    $c_map[$tk][CANDLE_HIGH] = max($c_map[$tk][CANDLE_HIGH], $tp);
                    $c_map[$tk][CANDLE_LOW] = min($c_map[$tk][CANDLE_LOW], $tp);
                    $c_map[$tk][CANDLE_CLOSE] = $tp;
                    $c_map[$tk][CANDLE_VOLUME] += $tv;
                }
            } // aggregation cycle
            ksort($c_map);
            return $c_map;
        }

        protected function RecoveryCandles(TicksCache $block, float $ticks_volume, bool $reconstruct = false) {
            global $mysqli, $mysqli_df;
            if (0 == count($block)) return;
            $start = $block->oldest_ms();
            $end  = $block->newest_ms();
            $date = $block->key;
            $candles_table = str_replace('ticks', 'candles', $this->table_name);
            $source = $block->Export();            
            $m_start = floor(time_ms() / 60000) * 60000;            
            $updated = 0;

            if (-1 == $block->index) {
                $latest = [];
                foreach ($source as $tick) 
                    if ($tick[TICK_TMS] >= $m_start) 
                        $latest []= $tick;                    
                $c_map = $this->ReconstructCandles($latest);                                
                if (count($c_map) > 0) {              
                    [$o, $c, $h, $l, $v] = $c_map[0];    
                    $ts = date_ms(SQL_TIMESTAMP, $m_start);
                    $set = "`close` = $c, `volume` = $v  WHERE `ts` = '$ts' ";                    
                    $um = $mysqli->try_query("UPDATE $candles_table SET $set");  
                    $uc = $mysqli_df->try_query("ALTER TABLE $candles_table UPDATE $set");                        
                    log_cmsg("~C92#LAST_CANDLE_UPGRADE:~C00 %s: MySQL %s, ClickHouse %s", $candles_table, b2s($um), b2s($uc));
                    $updated = 1;
                }
            }
            
            if (!$reconstruct) return $updated;
            $t_start = pr_time();
            $source = $block->Export();
            $ticks_count = count($source);
            $c_map = $this->ReconstructCandles($source);
            $start_ts = date_ms(SQL_TIMESTAMP, $start);
            $end_ts = date_ms(SQL_TIMESTAMP, $end);
            $exists = $mysqli->select_map('ts,volume', $candles_table, "WHERE ts >= '$start_ts' AND ts <= '$end_ts'");
            $rows = [];            
            $info = "EXISTS {$block->key}: ". print_r($exists, true);
            $info .= "UPGRADE: ".print_r($c_map, true);
            $mgr = $this->get_manager();

            file_put_contents("{$mgr->tmp_dir}/candles_recovery_{$this->ticker}.txt", $info);                        
            foreach ($c_map as $tk => $rec) {
                $ts = format_ts($tk);
                [$o, $c, $h, $l, $v] = $rec;
                if (isset($exists[$ts])) $updated ++;              
                $rows []= "('$ts', $o, $c, $h, $l, $v)"; // insert/update lost candle
            }
            
            
            if (count($rows) > 0) {
                $query = "INSERT INTO $candles_table (`ts`, `open`, `close`, `high`, `low`,`volume`) VALUES ";
                $query .= implode(",\n", $rows);                
                $res_c = $mysqli_df->try_query($query); // always overwrite existed candles via ReplacingMergeTree
                $query .= "\n ON DUPLICATE KEY UPDATE `open` = VALUES(`open`), `close` = VALUES(`close`), `high` = VALUES(`high`), `low` = VALUES(`low`), `volume` = VALUES(`volume`)";
                $res_m = $mysqli->try_query($query);         
                $elps = pr_time() - $t_start;           
                $inserted = count($rows) - $updated;
                if ($inserted > 0)
                    log_cmsg("~C92#RECOVERY_CANDLES:~C00 %d missed candles inserted, %d updated in %s, for MySQL %s, ClickHouse %s, time %.1f sec", 
                                $inserted, $updated, $candles_table, b2s($res_m), b2s($res_c), $elps);
                else
                    log_cmsg("~C92#RECOVERY_CANDLES:~C00 %d candles updated in %s", $updated, $candles_table);            
                
                if ($ticks_volume > $block->target_volume ) {
                    $query = "UPDATE {$candles_table}__1D SET `volume` = $ticks_volume WHERE Date(`ts`) = '$date';";
                    if ($mysqli->try_query($query))
                        $diff_pp = 100 * $ticks_volume / max(1, $block->target_volume);
                        log_cmsg("~C92#RECOVERY_CANDLES:~C00 %s__1D: daily candle %s volume upgraded to %s %.2f%% ", 
                                    $candles_table, $date, format_qty($ticks_volume), $diff_pp);
                    else
                        log_cmsg("~C31#WARN_FAILED_UPDATE:~C00 %s__1D: daily candle volume still unchanged", $candles_table);
                }

                if ($ticks_count > $block->target_count) {
                    $query = "UPDATE {$candles_table}__1D SET `trades` = $ticks_count  WHERE Date(`ts`) = '$date';";    
                    $mysqli->try_query($query);
                }
                $query = "INSERT IGNORE INTO `download_schedule` (`date`, `kind`, `ticker`, `target_volume`, `target_count`)\n VALUES";
                $query .= sprintf("('%s', 'sync-c', '%s', %f, 0)", 
                                 $block->key, $this->ticker, $ticks_volume);
                $mysqli->try_query($query); // загрузчик свечей в свою очередь осуществит синхронизацию данных в ClickHouse
                $stats_table = $this->stats_table;
                // INSERT INTO $stats_table (`date`, `day_start`, `day_end`, `volume_minutes` TODO: надо видимо вставлять запись, для предупреждения загрузчика свечей
                $query = "UPDATE $stats_table SET repairs = repairs + 1 WHERE `date` = '$date' "; 
                $mysqli->query($query);
            }
            else    
                log_cmsg("~C33#RECOVERY_CANDLES:~C00 no candles generated for update %s from %d ticks", $candles_table, $ticks_count);
            return $updated;
        }


        protected function SaveToDB(TicksCache $cache, bool $reset = true): int {                
            // multi-line insert
            global $mysqli_df, $verbose;    
            if (0 == count($cache)) return 0;

            $dummies = 0;
            $query = "INSERT INTO {$this->table_name} (`ts`, `buy`, `price`, `amount`, `trade_no`)\n VALUES";
            $lines = [];
            $keys = $cache->get_keys();
            

            foreach ($keys as $key) {
                $row = $cache[$key];
                if (!is_array($row) || count($row) != 5) 
                    throw new Exception("FATAL: SaveToDB: invalid row ".var_export($row, true));                                        
                if (0 == $row[TICK_AMOUNT]) $dummies ++;                                            
                $lines []= $cache->FormatRow($key);                    
            }
            $query .= implode(",\n", $lines);
            $res = 0;
            $q_start = pr_time();
            if ($mysqli_df->try_query($query)) {
                if ($dummies > 0)
                    log_cmsg("~C94#INFO({$this->symbol}):~C00 %d dummies from %d rows inserted into %s, affected %d", 
                                $dummies, count($cache), $this->table_name, $mysqli_df->affected_rows);                    
                $res = $mysqli_df->affected_rows;
                $oldest = $cache->oldest_ms();
                if ($oldest > EXCHANGE_START)
                    $this->db_oldest = min($this->db_oldest, $oldest);
                $newest = $cache->newest_ms();
                $this->db_newest = max($this->db_newest, $newest);
                if ($res >= 5000)
                    log_cmsg("~C96 #DB_PERF:~C00 %d ticks saved in %.3f seconds, db_range now [%s..%s]",
                                        $res, pr_time() - $q_start,
                                        format_tms($this->db_oldest), format_tms($this->db_newest));
                if ($reset) 
                    $cache->Reset(); // cleanup array, prevent to save again
            }
            
            $tmp_dir = $this->get_manager()->tmp_dir;
            $fname = "$tmp_dir/ticks_save_stat.json";
            $map = new stdClass();
            if (file_exists($fname)) {
                $map = file_load_json($fname, null ,false) ?? $map; // possible return null if file damaged                 
            }
            $symbol = $this->symbol;
            $map->$symbol = ['ts' => date(SQL_TIMESTAMP), 'rows' => count($cache), 'oldest' => $this->db_oldest, 'newest' => $this->db_newest];
            file_save_json($fname, $map);
            return $res;
        }   
        
        protected function ScanIncomplete(int $start, int $end) {
            global $mysqli, $mysqli_df;

            $day_start = time();
            $back = $this->initialized ? 5 : 0;                
            $day_start = floor($day_start / SECONDS_PER_DAY - $back) * SECONDS_PER_DAY; // полный набор блоков от сегодня и в прошлое. Потом отсев заполненных
    
            // SELECT * FROM  (SELECT  FROM bitmex__candles__gmtusd  WHERE ts >= '2024-01-01' GROUP BY DATE(ts) ORDER BY volume) LIMIT 50;
            $table_name = $this->table_name;

            $candle_tab = str_replace('ticks', 'candles', $table_name);
            $control_map = false;
            $today = gmdate('Y-m-d');
            $mgr = $this->get_manager();
            
            if (0 == $this->scan_year)
                $this->scan_year = date('Y');
            else    
                $this->scan_year --;
            
            $year = $this->scan_year;

            $month = date('H') % 12 + 1; // scan single partition on DB - better efficiency. Full history covered twice per day
            $start = max($start, strtotime(HISTORY_MIN_TS));                                  
            if (strtotime("$year-$month-01") < $start) // ненужно сканировать глубже
                return;     
            
            log_cmsg("~C96#PERF(ScanIncomplete):~C00 gathering stats for %s, checking period %d-%02d ", $table_name, $year, $month);
RESTART:                
            $ts_start = format_ts($start);                
            $period = "( YEAR(ts) = $year ) AND ( MONTH(ts) = $month ) AND ( ts >= '$ts_start' )";

            $control_map = [];
            // control ClickHouse candles by MySQL daily candles
            $columns = 'DATE(ts) as date, MIN(ts) as min, MAX(ts) as max, SUM(volume) as volume, COUNT(*) as count';            
            $ref_map = $mysqli->select_map($columns, $candle_tab, "WHERE $period AND volume > 0 GROUP BY DATE(ts)", MYSQLI_OBJECT) ?? [];                
            // контрольная таблица - минутные свечи, что позволяет определить диапазон времени, в котором есть данные
            if ($mysqli_df->table_exists($candle_tab))  {                                     
                $control_map = $mysqli_df->select_map($columns, $candle_tab, "FINAL WHERE $period AND volume > 0 GROUP BY DATE(ts) -- control_map", MYSQLI_OBJECT) ?? [];                
                foreach ($ref_map as $day => $rec) 
                    if (isset($control_map[$day])) {
                        $diff = $rec->volume - $control_map[$day]->volume; // positive means ClickHouse has less volume
                        $diff_pp = 100 * $diff / max($rec->volume, $control_map[$day]->volume, 0.01);
                        if ($diff_pp > 0.1) {
                            log_cmsg("~C31 #WARN:~C00 candles volume mismatch for ClickHouse %s: %s vs MySQL %s, diff = %.1f%%", $day, format_qty($control_map[$day]->volume), format_qty($rec->volume), $diff_pp);
                            $control_map[$day] = $rec;
                        }
                    }
                    else  {  
                        log_cmsg("~C31 #WARN:~C00 in ClickHouse absent day %s, need resync", $day);
                        $control_map[$day] = $rec;
                    }
            }
            elseif (count($ref_map) > 0) {
                log_cmsg("~C31 #WARN_NOT_SYNC:~C00 table %s not exists in ClickHouse, will used MySQL", $candle_tab);
                $control_map = $ref_map;
            }

            if (0 == count($control_map) && str_in($mysqli_df->error, 'Unknown codec family code')) {                    
                $this->table_need_recovery= true;    
                $this->CorrectTable();
                goto RESTART;
            }
            $extremums = $mysqli->select_map('DATE(ts), open, close, high, low, volume, trades', "{$candle_tab}__1D", "WHERE volume > 0 ", MYSQLI_OBJECT);
            ksort($extremums);

            // таблица статистики по тикам: начало и конец истории внутри дня, сумма объема           
            // на большой выборке данных (в моем случае 1.3 млрд. строк) следующий запрос займет мягко говоря время (минуты!). Поэтому внедрено ограничение месяц за раз     
            $days_map = $mysqli_df->select_map('DATE(ts) as date, MIN(ts) as min, MAX(ts) as max, MIN(price) as low, MAX(price) as high, SUM(amount) as volume, COUNT(ts) as count', 
                                                        $table_name, "FINAL WHERE $period AND (amount > 0) GROUP BY DATE(ts);  -- days_map", MYSQLI_OBJECT);
            // история загрузок позволяет оценить сколько уже было сделано попыток. Если меньше 2, то ещё раз попробовать допустимо                                                       
            $dh_rows = $mysqli->select_rows('ts,date,count,volume', 'download_history', 
                                            "WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks')", MYSQLI_OBJECT) ?? [];   // OPT? AND (YEAR(date) = $year) AND (MONTH(date) = $month)
            $history = []; // attempts count map
            foreach ($dh_rows as $rec) {
                $day = $rec->date;
                $history = ($history[$day] ?? 0) + 1;
            }
            unset($control_map[$today]);                                                              
            unset($days_map[$today]);
            $unchecked = [];
            $rescan = [];
            $this->blocks = [];
            $this->blocks_map = [];
            $full = $incomplete = 0;

            $voids = 0;
            if ($voids <= 0) {
                // первичный поиск - оценка неполноты блоков, по всей истории по заполнению разделов. Если в разделе данных нет, при наличии хотя-бы одной дневной свечи, нужно добавить в расписание
                // каждый раздел охватывает месяц, если ничего не поменялось, и имеет название в формате даты первого числа
                $db_name = DB_NAME;
                $parts = $mysqli_df->select_map('partition, rows', 'system.parts', "WHERE `table` = '$table_name' AND `active` = 1 AND `database` = '$db_name' ", MYSQLI_OBJECT);
                foreach ($extremums as $date => $rec) {
                    $yms = substr($date, 0, 7).'-01'; // year-month
                    if (isset($parts[$yms]) || array_value($history, $date, 0) >= 2) continue;
                    $unchecked [$date] = $rec;                      
                    $voids ++;
                }                
                if ($voids > 0) goto SKIP_SCAN;
            }

            $unchecked = $control_map;

            $dark_zone = strtotime(HISTORY_MIN_TS); // дальше лучше не пробовать восстановление данных через биржу, т.к. trdMatchID рандомный
            // стратегия поменялась: теперь загрузка с самого начала, до победного конца. Чтобы в БД все ложилось последовательно и доступ был оптимальный. Так собирать сложные свечи будет быстрее
            ksort($control_map);  
            $first_day = array_key_first($control_map);            
            
            foreach ($control_map as $day => $cstat) {                    
                if (!$mgr->active) return;     // stop yet now
                $mgr->ProcessWS(false);  // support WebSocket minimal latency
                unset($unchecked[$day]);
                $block = $this->DayBlock($day, $cstat->volume, false);
                $sod_t = strtotime($day);                        
                if ($sod_t <= $dark_zone) continue;
                if (!isset($days_map[$day])) {
                    log_cmsg("~C33 #VOID_BLOCK_DETECTED($this->symbol):~C00 at %s volume = %s", $day, $cstat->volume);                                             
                    $rescan []= $block;
                    continue;
                }                    

                $bad_candles = false;
                $block->minutes_volume = $candles_vol = $cstat->volume;                       
                $meta = $days_map[$day];
                $last_price = 0;
                $target_trades = 0;
                $day_ex = $extremums[$day] ?? null;
                if (is_object($day_ex)) {                    
                    $target_trades = $day_ex->trades;
                    $extra = $day_ex->volume - $candles_vol;
                    $extra_pp = 100 * $extra / max($candles_vol, $day_ex->volume);
                    if ($extra_pp >= 0.1)
                        log_cmsg("~C31 #VOLUME_MISMATCH({$this->ticker}):~C00 for %s candles: saldo %s < daily %s, diff = %s (%.2f%%). Using from higher timeframe",
                                $day, format_qty($candles_vol), format_qty($day_ex->volume), format_qty($extra), $extra_pp); 
                    $candles_vol =  $day_ex->volume; // 1D table can be manually patched, means have max priority                    
                    $bad_candles = true;
                }

                $attempts = array_value($history, $date, 0); // well fast get attempts count                                    
                $min = $meta->min;
                $max = $meta->max;                    
                $void_left  = max(0, $block->min_avail - strtotime($cstat->min)) / 60.0;
                $void_right = max(0, strtotime($cstat->max) + 59 - $block->max_avail) / 60.0;

                $vdiff = $candles_vol - $meta->volume;
                $inacuracy = $candles_vol * 0.001;  // float inaccuracy
                $vdiff_pp = 100 * $vdiff / max($candles_vol, $meta->volume, 0.1);                                                                            
                $vol_info = '';
                $excess = false;
                $incomplete = true;

                $price_info = '';

                if ($candles_vol > 0) {                   
                    if ($candles_vol == $meta->volume || $candles_vol * 2 == $meta->volume) {  // double volume?
                        $candles_vol = $meta->volume;
                        $vdiff = 0;
                        $vdiff_pp = 0;
                    }                                               
                    
                    // TRICK: толерантность включать только при недостаточном объеме
                    if ($vdiff >= -$inacuracy && abs($vdiff_pp) < $this->volume_tolerance) {
                        $vol_info = '~C00, same candles'; 
                        $incomplete = false;
                    }
                    else {
                        $incomplete |= true;
                        if ($vdiff > 0) 
                            $vol_info = sprintf('~C00, ~C93less~C00 relative candles =~C95 %8.1f%%~C00, diff +', $vdiff_pp);                                                            
                        else {
                            $vol_info = sprintf('~C00, ~C04~C93excess~C00 relative candles  ~C95 %8s~C00, %8.2f%% diff +',  format_qty($candles_vol), $vdiff_pp);
                            $excess = true; // если эта проблема сохраняется, значит минутки не пересобрались в прошлый раз. Надо проверять логику...                                
                        }
                        $vol_info .= format_qty($vdiff) ;
                    }                       
                    if ($meta->count < $target_trades) {
                        $incomplete = true;
                        $vol_info .= sprintf('~C00, ~C91insufficiently~C00 trades count %d < %d', $meta->count, $target_trades);
                    }
                }                   

                if (is_object($day_ex) && $incomplete) {
                    $meta->close = $last_price = $mysqli_df->select_value('price', $table_name, "WHERE DATE(ts) = '$day' ORDER BY ts DESC");
                    $diff_list = [];
                    foreach (['low', 'high', 'close'] as $field) {
                        if ($day_ex->$field != $meta->$field) 
                            $diff_list []= format_color("%s %f != %f", $field, $day_ex->$field, $meta->$field);                            
                    }

                    if (count($diff_list) > 0) {
                        //  $incomplete = true;
                        log_cmsg ('~C31 #PRICE_MISTMACH:~C00 ~C91inconsistent~C00 prices '.implode(', ', $diff_list));                                        
                    } 
                    else
                        log_cmsg ('~C32 #PRICES_CHECKED:~C00 all daily equal low = %f, high = %f, close = %f', 
                                            $meta->low, $meta->high, $last_price);
                }               
                                
                $incomplete |= $bad_candles;
                $block->code = $incomplete ? BLOCK_CODE::PARTIAL : BLOCK_CODE::FULL;
                
                if ($block->code == BLOCK_CODE::FULL) {
                    $full ++; // not add to rescan
                    continue;
                }
                if (0 == $meta->volume) 
                    $block->code = BLOCK_CODE::NOT_LOADED;

                $incomplete ++;                    

                if ($attempts > 1) {
                    log_cmsg("~C33 #SCAN_BLOCK_SKIPPED({$this->ticker}):~C00 %s, already %d attempts to load", $day, $attempts);
                    continue; 
                }                                    

                $block->index = count($rescan);                    
                $block->target_volume = $candles_vol;
                $block->target_close = $cstat->close;
                $block->target_count = $target_trades;
                $block->db_need_clean = $excess;
                $rescan []= $block;
                $tag = $excess ? '~C04~C97#BLOCK_NEED_RELOAD' : '~C94#BLOCK_NEED_FINISH';
                log_cmsg("$tag({$this->symbol}):~C00 index %4d, day %s [%s..%s, volume %10s%s in %d ticks]; %s margins [%.3f, %.3f] hours, code %s",
                                $block->index, $day, $min, $max, 
                                format_qty($meta->volume), $vol_info, $meta->count, $price_info,                                            
                                    $void_left, $void_right, $block->code->name);
                   
            } // foreach control_map - main scan loop
SKIP_SCAN:            
            ksort($unchecked);
            foreach ($unchecked as $day => $meta) {
                // if (count($rescan) >= $this->max_blocks) break;
                log_cmsg("~C33#VOID_BLOCK_ADD({$this->ticker}):~C00 schedudled %s with target volume %s", $day, $meta->volume);
                $rescan []= $block = $this->DayBlock($day, $meta->volume, false);                                                  
                if (isset($meta->close))
                    $block->target_close = $meta->close;
                if (isset($meta->trades))
                    $block->target_count = $meta->trades;
            }                                               

            $query = "INSERT IGNORE INTO `download_schedule` (date, kind, ticker, target_volume, target_close) VALUES ";
            $rows = [];
            // полное отложенное расписание
            foreach ($rescan as $block) {
                if ($block->IsFull()) continue; 
                $rows []= sprintf("('%s', 'ticks', '%s', %f, 0)", 
                                    $block->key, $this->ticker, $block->target_volume);
            }
            if (count($rows) > 0) {
                $query .= implode(",\n", $rows);
                if ($mysqli->query($query)) // WARN: need ignore replica
                    log_cmsg("~C97#SCHEDULE:~C00 added %d / %d rows to schedule", $mysqli->affected_rows, count($rows));
            }

            $this->blocks_map = [];

            $rescan = array_slice($rescan, -$this->max_blocks); // ограничение на текущее сканирование
            foreach ($rescan as $idx => $block)  {               
                $block->LoadCandles();
                $this->blocks_map[$block->key] = $block;
            }
            
            ksort($this->blocks_map);  // не упорядоченные блоки могут привести к нежелательным пропускам (перекрытиям)
            $this->blocks = array_values($this->blocks_map); 
            foreach ($this->blocks as $idx => $block)
                $block->index = $idx; 

            
            $first = $this->TimeStampEncode($start, 1);
            $last =  $this->TimeStampEncode( $end, 1);
            if (count($this->blocks_map) > 0) {
                // WARN: blocks_map have backsorting 
                $first = array_key_last($this->blocks_map);
                $last = array_key_first($this->blocks_map);
            }

            $tag = $this->initialized ? '#RELOAD_BLOCKS' : '#INIT_BLOCKS';
            log_cmsg("~C97$tag:~C00 for symbol %s will used range %s .. %s, expected download %d blocks, %d incomplete, %d full from %d", $this->symbol,
                    $first, $last, $this->BlocksCount(), $incomplete, $full, count($control_map));            
        }   
    

    } // class TicksDownloader


    function RunConsoleSession(string $prefix) {
        global $argc, $argv, $tmp_dir, $mysqli, $mysqli_df, $db_servers, $hour, $hstart, $log_file, $manager, $verbose;        
        date_default_timezone_set('UTC');
        set_time_limit(15);    
        
        $db_name_active = 'nope'; 
        $symbol = 'all';

        if ($argc && isset($argv[1])) {
            $symbol = $argv[1];
            if (isset($argv[2]))
                $verbose = $argv[2];
        }  
        else
            $symbol = rqs_param("symbol", 'all');         

        $pid_file = sprintf("$tmp_dir/ticks_dl@%s.pid", $symbol);
        $pid_fd = setup_pid_file($pid_file, 300);        
        $hour = date('H');
        $log_name = sprintf('/logs/%s_ticks_dl@%s-%d.log', $prefix, $symbol, $hour); // 24 logs rotation
        $log_file = fopen(__DIR__.$log_name, 'w');
        flock($log_file, LOCK_EX);

        if (file_exists(REST_ALLOWED_FILE)) {
            $rest_allowed_t = file_get_contents(REST_ALLOWED_FILE);
            log_cmsg("#DBG: RestAPI allowed after %s", gmdate(SQL_TIMESTAMP, $rest_allowed_t));    
            if (time() < $rest_allowed_t) {
                $rest_allowed_t - time();
                log_cmsg("#WARN: RestAPI BAN applied up to %s", color_ts($rest_allowed_t));                 
                set_time_limit(60);                      
            }
        }    

        echo ".\n";
        log_cmsg("~C97 #START:~C00 trying connect to DB...");
        $mysqli = init_remote_db(DB_NAME);
        if (!$mysqli) {
            log_cmsg("~C91 #FATAL:~C00 cannot initialze DB interface! ");
            die("ooops...\n");
        }   

        $mysqli_df = ClickHouseConnectMySQL(null, null, null, DB_NAME);  
        if ($mysqli_df)
            log_cmsg("~C93 #START:~C00 MySQL interface connected to~C92 %s@$db_name_active~C00 ", $db_servers[0] ); 
        else
            error_exit("~C91#FATAL:~C00 cannot connect to ClickHouse DB via MySQL interface! ");

        $mysqli_df->replica = ClickHouseConnectMySQL('db-remote.lan:9004', null, null, DB_NAME);
        if (is_object($mysqli_df->replica)) {
            log_cmsg("~C103~C30 #WARN_REPLICATION:~C00 %s connected", $mysqli_df->replica->host_info);        
        }        

        $mysqli->try_query("SET time_zone = '+0:00'");
        log_cmsg("~C93 #START:~C00 connected to~C92 localhost@$db_name_active~C00 MySQL");         
        $hstart = floor(time() / 3600) * 3600;
        $manager = new TicksDownloadManager($symbol);
        main($manager);    
        fclose($log_file);
        flock($pid_fd, LOCK_UN);
        fclose($pid_fd);  
        system("bzip2 -f --best $log_name");
        $log_file = false;
        unlink($pid_file);
    }
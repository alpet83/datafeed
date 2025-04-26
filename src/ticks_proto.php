<?php    
    require_once "loader_proto.php";    
    require_once "blocks_loader.php";
    

    # NOTE: most timestamps are in ms
    const HISTORY_MIN_TS = '2024-01-01 00:00:00'; // minimal history start for initial download
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
        public     array $filled_vol = [];

        public function __toString() {
            $res = parent::__toString();
            $res .= ', CM: '.count($this->candle_map);
            $res .= ', UF: '.count($this->filled_vol); 
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

        protected function CheckFillMinute(int $m, float $amount) {        
            $cm = $this->candle_map;
            if (!isset($cm[$m])) 
                log_cmsg("~C91#WARN:~C00 CheckFillMinute no candle record for minute %d", $m);                           

            $this->filled_vol[$m] = ($this->filled_vol[$m] ?? 0) - $amount;                         

            if (0 == $this->filled_vol[$m]) 
                unset($this->filled_vol[$m]);
        }
        public function Count(): int {
            return count($this->cache_map);
        }

        public function IsFullFilled(): bool {            
            if (0 == count($this->filled_vol) && count($this->candle_map) > 0) return true;
            return parent::IsFullFilled();
        }

        /** загрузка минутных свечей для контроля консистентности */
        public function LoadCandles() {
            if ($this->index < -1) return;

            $mysqli_df = sqli_df();
            $table = $this->owner->table_name;
            $c_table = str_replace('ticks', 'candles', $table);
            if (!$mysqli_df->table_exists($c_table)) return;                                                

            $this->last_fwd = $this->lbound_ms - 1;
            $this->last_bwd = $this->rbound_ms + 1;

            $start = format_ts( max($this->lbound, EXCHANGE_START_SEC));            
            $end = format_ts($this->rbound);                        

            $map = $mysqli_df->select_rows('ts,open,close,high,low,volume', $c_table, "WHERE ts >= '$start' AND ts <= '$end'", MYSQLI_NUM);
            foreach ($map as $candle) {
                $t = strtotime(array_shift($candle));
                if ($t < EXCHANGE_START_SEC) {
                    log_cmsg("~C91 #ERROR:~C00 loaded candle with outbound timestamp %s", format_ts($t));
                    continue;
                }
                $key = $this->candle_key($t);
                $this->candle_map[$key] = $candle;
                $this->filled_vol[$key] = $candle[CANDLE_VOLUME] * 0.995;  // 0.5% объема закладывается как погрешность, чтобы быстрее фиксировать заполнение минуты
            }

            $vol_map = $mysqli_df->select_map("(toHour(ts) * 60 + toMinute(ts)) as minute, SUM(amount) as volume", $table, 
                                               "WHERE date(ts) = '{$this->key}' GROUP BY minute ORDER BY minute");
            $abnormal = 0;
            foreach ($vol_map as $m => $v) {
                if (!isset($this->filled_vol[$m])) continue;                                
                if ($v > $this->filled_vol[$m] * 1.05) 
                    $abnormal ++;
                $this->filled_vol[$m] -= $v;    // для этой минуты тики загружены хотя-бы частично                
                if (!$this->TestUnfilled($m)) 
                    unset($this->filled_vol[$m]); // сокращение работы
            }
            if (count($this->filled_vol) > 0) // hundreds means - candles possible wrong loaded
                log_cmsg("~C96#PERF:~C00 need refill ticks for %d minutes in range %s .. %s ", 
                            count($this->filled_vol), format_tms($this->UnfilledAfter()), format_tms($this->UnfilledBefore()));
            if ($abnormal > 0)
                log_cmsg("~C91#WARN:~C00 %d abnormal minutes, where volume of ticks > volume of candle ", $abnormal); // TODO: patch candles table with reset volume? 
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

        protected function AddDummy(int $t) {
            $best = $t;
            $best_dist = SECONDS_PER_DAY;
            foreach ($this->cache_map as $rec) {                
                $tt = floor($rec[0] / 1000);
                $dist = $tt - $best;
                if (abs($dist) < $best_dist) {
                    $best = $tt;
                    $best_dist = abs($dist);
                }
            }
            $price = 0;
            if (isset($this->cache_map[$best])) 
                $price = $this->cache_map[$best]['price'];
            $tno = "dm-$t";
            $rec = [$t * 1000, $price, 3, 0, $tno];
            $this->cache_map[$tno] = $rec;
        }    

        public function SaldoVolume(): float {
            return $this->CalcSummary(TICK_AMOUNT);
        }

        public function SetRow(mixed $key, array $row): int {            
            $is_dup = isset($this->cache_map[$key]);  
            if ($row[TICK_PRICE] <= 0) 
                throw new ErrorException("Invalid tick record, price field must > 0 ".json_encode($row));

            $this->cache_map[$key] = $row;                    
            if ($is_dup)
                $this->duplicates ++;
            else {                
                $t = floor($row[TICK_TMS] / 1000);                            
                $this->set_filled($t);
                if ($this->index >= -1) {  // в общем кэше нет смысла фиксировать минуты
                    $m = $this->candle_key($t);
                    $this->CheckFillMinute($m, $row[TICK_AMOUNT]);                
                }               
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
            // $cm = $this->candle_map[$m] ?? [0, 0, 0, 0, 0];
            // $mv = $cm[CANDLE_VOLUME];
            return $this->filled_vol[$m]  > 0; 
        } 

        public function UnfilledAfter(): int {
            foreach ($this->filled_vol as $key => $v)
                if ($this->TestUnfilled($key)) {
                    $tms = $this->candle_tms($key);
                    if ($tms <= $this->last_fwd) continue; // предотвращение повторных результатов
                    if ($tms >= $this->lbound_ms) return $tms;
                    log_cmsg("~C91#ERROR(UnfilledAfter):~C00 for minute key %d produced timestamp %s ", $key, format_tms($tms));
                    break;
                }
            return parent::UnfilledAfter();
        }

        public function UnfilledBefore(): int {         
            $rv = array_reverse($this->filled_vol, true); // need maximal key
            foreach ($rv as $key => $v) {                
                if ($this->TestUnfilled($key)) {
                    $tms = $this->candle_tms($key + 1); // download need from next minute
                    if ($tms >= $this->last_bwd) continue; // предотвращение повторных результатов
                    return $tms;
                }
            }
            return parent::UnfilledBefore(); 
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

            
            
            public function __construct(DownloadManager $mngr, stdClass $ti) {
                $this->import_method = 'ImportTicks';
                $this->load_method = 'LoadTicks';    
                $this->block_class = 'TicksCache';     
                $this->data_name = 'ticks';       
                parent::__construct($mngr, $ti);                
                $this->cache = new TicksCache($this);
                $this->days_per_block = 1; // в каждом блоке могут быть тысячи тиков, даже за час
                $this->data_flags = DL_FLAG_HISTORY | DL_FLAG_REALTIME;         
                $this->table_proto = 'clickhouse_ticks_table.sql';                                
                $this->time_precision = 3;
                $this->blocks_at_once = 5;
                $this->max_blocks = 10; // lazy recovery for better latency
            }

            public function CorrectTable (){
                global $mysqli_df;
                // $mysqli_df->try_query("ALTER TABLE {$this->table_name} MODIFY ORDER BY trade_no");
                $table_name = $this->table_name;
                parent::CorrectTable();
                $code = $this->table_create_code_ch;
                if (!is_string($code) || !str_in($code, 'CREATE')) {
                    log_cmsg("~C91#WARN(CorrectTable):~C00 not set create code for %s", $table_name);
                    return;
                }
                if (str_in($code, 'ReplacingMergeTree(ts)'))
                    log_cmsg("~C92#TABLE_OK:~C00 Used actual Engine ");
                else  {
                    log_cmsg("~C31#WARN_UPGRADE:~C00 changing engine for table %s", $table_name);
                    $query = "REPLACE TABLE  $table_name ENGINE  ReplacingMergeTree(ts) ORDER BY trade_no PARTITION BY toStartOfMonth(ts)  AS SELECT * FROM $table_name";
                    $mysqli_df->try_query($query);
                }                
            }    
    
            protected function DayBlock(string $day, float $target_vol = 0) {
                $count = count($this->blocks);
                $sod_t = strtotime($day);    
                $eod_t = $sod_t + SECONDS_PER_DAY - 1;
                $block = $this->CreateBlock($sod_t, $eod_t);               
                $block->code = BLOCK_CODE::NOT_LOADED;
                $block->LoadCandles();
                $block->index = $count;
                $block->target_volume = $target_vol;
                return $block;
            }
          
            public function FlushCache() {
                $cache_size = count($this->cache);
                $saved = $this->SaveToDB($this->cache);
                $this->rest_loads += $saved;
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
                global $mysqli;

                if ($this->data_flags & DL_FLAG_HISTORY == 0) 
                    return;


                if ($end < strtotime(HISTORY_MIN_TS)) {
                    log_cmsg("~C91#ERROR:~C00 invalid end timestamp %d: %s", $end, format_TS( $end));
                    $end = time(); 
                }
    
                if ($start < strtotime(HISTORY_MIN_TS) && !$this->initialized) {
                    log_cmsg("~C31#WARN_INIT_BLOCKS:~C00 start timestamp %d: %s, will be increased", $start, gmdate(SQL_TIMESTAMP, $start));
                    $start = strtotime(HISTORY_MIN_TS); 
                }
                $schedule = $mysqli->select_map('date, target_volume', 'download_schedule', 
                                                "WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks') AND (target_volume > 0) ORDER BY date DESC");
                if (0 == count($schedule))
                    $this->ScanIncomplete($start, $end);
                else {
                    foreach ($schedule as $date => $tv) {
                        if (count($this->blocks) >= $this->max_blocks) break;
                        $this->DayBlock($date, $tv);
                    }
                    log_cmsg("~C97#INIT_BLOCKS:~C00 selected from schedule %d / %d blocks ", count($this->blocks), count($schedule));
                }

            }

            protected function ScanIncomplete(int $start, int $end) {
                global $mysqli, $mysqli_df;

                $day_start = time();
                $back = $this->initialized ? 5 : 0;                
                $day_start = floor($day_start / SECONDS_PER_DAY - $back) * SECONDS_PER_DAY; // полный набор блоков от сегодня и в прошлое. Потом отсев заполненных
        
                // SELECT * FROM  (SELECT  FROM bitmex__candles__gmtusd  WHERE ts >= '2024-01-01' GROUP BY DATE(ts) ORDER BY volume) LIMIT 50;

                $candle_tab = str_replace('ticks', 'candles', $this->table_name);
                $control_map = false;
                $today = gmdate('Y-m-d');
                $mgr = $this->get_manager();

                // TODO: убрать следующие строки из релиза. Плохие записи попали в БД, когда перепуталось price & buy
                $mysqli_df->try_query("DELETE FROM {$this->table_name} WHERE price = 0");
                $mysqli_df->try_query("DELETE FROM {$this->table_name} WHERE price = 1");
RESTART:                
                $ts_start = format_ts($start);                
                // контрольная таблица - минутные свечи, что позволяет определить диапазон времени, в котором есть данные
                if ($mysqli_df->table_exists($candle_tab))  {                     
                    $columns = 'DATE(ts) as date, MIN(ts) as min, MAX(ts) as max, SUM(volume) as volume, COUNT(*) as count';
                    $control_map = $mysqli_df->select_map($columns, $candle_tab, "FINAL WHERE (ts >= '$ts_start') AND volume > 0 GROUP BY DATE(ts)", MYSQLI_OBJECT);                
                    // control ClickHouse candles by MySQL daily candles
                    $ref_map = $mysqli->select_map($columns, $candle_tab, "WHERE (ts >= '$ts_start') AND volume > 0 GROUP BY DATE(ts)", MYSQLI_OBJECT);                
                    foreach ($ref_map as $day => $rec) 
                        if (isset($control_map[$day])) {
                            $diff = $control_map[$day]->volume - $rec->volume;
                            if (abs($diff) > $rec->volume * 0.001) {
                                log_cmsg("~C31 #WARN:~C00 candles volume mismatch for ClickHouse %s: %s vs MySQL  %s", $day, format_qty($control_map[$day]->volume), format_qty($rec->volume));
                                $control_map[$day] = $rec;
                            }
                        }
                        else  {  
                            log_cmsg("~C31 #WARN:~C00 in ClickHouse absent day %s, need resync", $day);
                            $control_map[$day] = $rec;
                        }
                }
                else
                    log_cmsg("~C31 #WARN:~C00 table %s not exists, can't using for comparation", $candle_tab);

                if (0 == count($control_map) && str_in($mysqli_df->error, 'Unknown codec family code')) {                    
                    $this->table_need_recovery= true;    
                    $this->CorrectTable();
                    goto RESTART;
                }

                // таблица статистики по тикам: начало и конец истории внутри дня, сумма объема
                $mysqli_df->try_query("OPTIMIZE TABLE {$this->table_name} FINAL");
                $days_map = $mysqli_df->select_map('DATE(ts) as date, MIN(ts) as min, MAX(ts) as max, SUM(amount) as volume', 
                                                            $this->table_name, "FINAL WHERE (ts >= '$ts_start') GROUP BY DATE(ts)", MYSQLI_OBJECT);
                unset($control_map[$today]);                                                              
                unset($days_map[$today]);
                $unchecked = $control_map;
                $rescan = [];
                $this->blocks = [];
                $this->blcocks_map = [];

                                
                $full = $incomplete = 0;
                $sieved = $this->BlocksCount();                

                krsort($control_map);  // последними должны быть в списке - новейшие блоки (2025+), поэтому сканирование списка нужно проводить начиная с последних.
                foreach ($control_map as $day => $cstat) {                    
                    if (!$mgr->active) break; // stop yet now
                    unset($unchecked[$day]);
                    $block = $this->DayBlock($day, $cstat->volume);
                    $sod_t = strtotime($day);                        
                    if (!isset($days_map[$day])) {
                        log_cmsg("~C33 #VOID_BLOCK_DETECTED($this->symbol):~C00 at %s volume = %s", $day, $cstat->volume);                         
                        $rescan []= $block;
                        continue;
                    }                    

                    $candles_vol = $cstat->volume;              
                    $meta = $days_map[$day];
                    $min = $meta->min;
                    $max = $meta->max;                    
                    $void_left  = max(0, $block->min_avail - strtotime($cstat->min)) / 60.0;
                    $void_right = max(0, strtotime($cstat->max) + 59 - $block->max_avail) / 60.0;

                    $vdiff = $candles_vol - $meta->volume;
                    $vdiff_pp = 100 * $vdiff / max($candles_vol, $meta->volume, 0.1);                                        
                                        
                    $vol_info = '';
                    $excess = false;
                    if ($candles_vol > 0) {
                        $block->code = ($void_left > 1 || $void_right > 1 || abs($vdiff_pp) > $this->volume_tolerance ) ? BLOCK_CODE::PARTIAL : BLOCK_CODE::FULL;                         
                        if ($candles_vol == $meta->volume || $candles_vol * 2 == $meta->volume) {  // double volume?
                            $block->code = BLOCK_CODE::FULL;         
                            $candles_vol = $meta->volume;
                            $vdiff = 0;
                            $vdiff_pp = 0;
                        }                                                
                        
                        if (abs($vdiff_pp) < $this->volume_tolerance) 
                            $vol_info = '~C00, same candles'; 
                        else {
                            if ($vdiff > 0) 
                                $vol_info = sprintf('~C00, ~C93insufficiently~C00 relative candles =~C95 %8.1f%%~C00, diff +', $vdiff_pp);                                                            
                            else {
                                $vol_info = sprintf('~C00, ~C91excess~C00 relative candles  ~C95 %8s~C00, diff +', format_qty($candles_vol));
                                $excess = true;                                
                            }
                            $vol_info .= format_qty($vdiff) ;
                        }
                    }   
                    
                    
                    if ($block->code == BLOCK_CODE::FULL)
                        $full ++; // not add to rescan
                    elseif ($block->code == BLOCK_CODE::PARTIAL && $this->initialized) { // добавлять недозаполненные когда второй круг
                        $incomplete ++;                    
                        $block->index = count($rescan);
                        $block->LoadCandles();
                        $block->target_volume = $candles_vol;
                        $block->db_need_clean = $excess;
                        $rescan []= $block;
                        log_cmsg("~C94#BLOCK_CHECK({$this->symbol}):~C00 prev index %4d, day %s [%s..%s, volume %10s%s in %d ticks]; margins [%.3f, %.3f] hours, code %s",
                                        $block->index, $day, $min, $max, 
                                        format_qty($meta->volume), $vol_info, $meta->count,                                             
                                            $void_left, $void_right, $block->code->name);
                    }    
                } // foreach control_map - main scan loop

                log_cmsg("~C93#DBG:~C00 unchecked days %d", count($unchecked));
                krsort($unchecked);
                foreach ($unchecked as $day => $meta) {
                    // if (count($rescan) >= $this->max_blocks) break;
                    log_cmsg("~C33#VOID_BLOCK_ADD:~C00 schedudled %s", $day);
                    $rescan []= $this->DayBlock($day, $meta->volume);                                                  
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
                    if ($mysqli->try_query($query))
                        log_cmsg("~C97#SCHEDULE:~C00 added %d / %d rows to schedule", $mysqli->affected_rows, count($rows));
                }

                $this->blocks_map = [];

                $rescan = array_slice($rescan, -$this->max_blocks); // ограничение на текущее сканирование
                foreach ($rescan as $idx => $block)                 
                    $this->blocks_map[$block->key] = $block;
                
                ksort($this->blocks_map);  // не упорядоченные блоки могут привести к нежелательным пропускам (перекрытиям)
                $this->blocks = array_values($this->blocks_map); 
                foreach ($this->blocks as $idx => $block)
                    $block->index = $idx; 

                $sieved -= count($rescan);
                $first = $this->TimeStampEncode($start, 1);
                $last =  $this->TimeStampEncode( $end, 1);
                if (count($this->blocks_map) > 0) {
                    $first = array_key_first($this->blocks_map);
                    $last = array_key_last($this->blocks_map);
                }

                $tag = $this->initialized ? '#RELOAD_BLOCKS' : '#INIT_BLOCKS';
                log_cmsg("~C97$tag:~C00 for symbol %s will used range %s .. %s, expected download %d blocks, %d sieved, %d incomplete, %d full", $this->symbol,
                        $first, $last, $this->BlocksCount(), $sieved, $incomplete, $full);
                $this->initialized = true;                        
            }    

            abstract public function     ImportTicks(array $data, string $source, bool $direct_sync = true): ?TicksCache;

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
                foreach ($block->filled_vol as $m => $v) {
                    $cr = $block->candle_map[$m] ?? [0, 0, 0, 0, 0];
                    $mv = $cr[CANDLE_VOLUME] ?? 0;
                    if ($v > $mv * 0.05) {
                        $max = max($max, $m);
                        $min = min($min, $m);
                    }                   
                }
                log_cmsg(" ~C94#BLOCK_INFO:~C00 filled_vol min %d / max %d ", $min, $max);
                $uf = ( $block->UnfilledBefore() - $block->lbound_ms ) / 1000;
                if ($max > 120 && $uf < 3600) {
                    $vals = array_values($block->filled_vol);
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
                $check = sqli_df()->select_row('DATE(ts) as date, SUM(amount) as volume, COUNT(*) as count', $this->table_name, 
                                    "FINAL WHERE DATE(ts) = '$day' GROUP BY DATE(ts)", MYSQLI_OBJECT);

                $check_volume = is_object($check) ? $check->volume : $saldo_volume;                
                if ($saldo_volume > $check_volume * 0.75)
                    sqli()->try_query("DELETE FROM `download_schedule` WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks') AND (date = '$day')");                
                
                $count = is_object($check) ? $check->count : count($block);                
                $db_diff = $saldo_volume - $check->volume;                 

                $db_diff_pp = 100 * $db_diff / max($check->volume, $saldo_volume, 0.01);
                if (is_object($check) && $db_diff_pp > 0.5) 
                    log_cmsg(" ~C31#WARN_DB_PROBLEM:~C00 %s, count in DB %d vs count in block %d, volume %s vs %s (diff %.1f%%)",
                             $check->date, $check->count, count($block),
                             format_qty($check->volume), format_qty($saldo_volume), $db_diff_pp);                

                $diff = abs($block->target_volume - $check_volume);                 
                $diff_pp = 100 * $diff / max($block->target_volume, $check_volume, 0.01);
                if ($diff_pp < $this->volume_tolerance) {
                    log_cmsg("$prefix($symbol/$index):~C00 %s, lag left %d, %s, target_volume reached %s in %d ticks filled in session: %s ", 
                                strval($block), $lag_left, $block->info, format_qty($check_volume), $count, $filled);    
                } else {
                    log_cmsg("~C04{$prefix}_WARN($symbol/$index):~C00 %s, lag left %d, %s, target_volume reached %s instead %s (diff %.1f%%), filled in session: %s ", 
                                strval($block), $lag_left, $block->info, format_qty($check_volume), format_qty($block->target_volume), $diff_pp, $filled);    
                    $fk = [];
                    $uk = [];
                    $vk = [];
                    foreach ($block->filled_vol as $mk => $v) {
                        if ($v > 0) 
                            $uk [$mk]= $v;
                        else    
                            $fk [$mk]= $v;
                    }
                    foreach ($block->candle_map as $mk => $cr)
                        $vk [$mk]= $cr[CANDLE_VOLUME];                     
                    log_cmsg(" ~C94#FILLED_DUMP: ~C00 F: %s \n U: %s \n CM: %s", json_encode($fk), json_encode($uk), json_encode($vk));                                
                    if ($diff_pp > 5)
                        error_exit("~C101~C97       STOPe               ~C00");
                }
                $block->Reset(); // clean memory
            }

            protected function OnCacheUpdate(DataBlock $default, DataBlock $cache) {         
                $ublocks = [];           
                if (0 == count($cache)) return;

                foreach ($cache->keys as $key) {
                    $tick = $cache[$key];
                    $tms = $tick[0];                    
                    $this->oldest_ms = min($this->oldest_ms, $tms); // in ms
                    $this->newest_ms = max($this->newest_ms, $tms); // in ms
                    $ts = $this->TimeStampEncode($tms);
                    $day = substr($ts, 0, 10);
                    $block = $this->blocks_map[$day] ?? $default;                                          
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
                    if ($block->IsFullFilled() && BLOCK_CODE::FULL != $default->code) {
                        $block->code = BLOCK_CODE::FULL;
                        $block->info = "fullfilled by cache {$cache->key}";
                    }

                foreach ($ublocks as $k => $block) 
                    if ($block->Covered_by($oldest_ms, $newest_ms)  && BLOCK_CODE::FULL != $default->code) {
                        $block->code = BLOCK_CODE::FULL;
                        $block->info = "overlap by cache {$cache->key}";
                        unset($ublocks[$k]);
                    }


                
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
                    $this->db_oldest = min($this->db_oldest, $oldest);
                    $newest = $cache->oldest_ms();
                    $this->db_newest = min($this->db_newest, $newest);
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
                if (file_exists($fname)) 
                    $map = file_load_json($fname, null ,false);
                $symbol = $this->symbol;
                $map->$symbol = ['ts' => date(SQL_TIMESTAMP), 'rows' => count($cache), 'oldest' => $this->db_oldest, 'newest' => $this->db_newest];
                file_save_json($fname, $map);
                return $res;
            }   
            
    

    }
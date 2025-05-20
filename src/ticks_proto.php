<?php               
    
    require_once 'blocks_loader.php';
    require_once 'candles_cache.php';
    require_once 'ticks_cache.php';

    # NOTE: most timestamps are in ms  

    /** 
     * Базовый класс (прототип) для загрузчика истории тиков/трейдов.
     * Подразумевается основная база хранения ClickHouse, через интерфейс MySQL. 
     * Предназначение предполагает в т.ч. высокочастотную торговлю, поэтому данные WebSocket пишутся в БД без кэширования.
     */

    abstract class TicksDownloader 
        extends BlockDataDownloader {

        protected $scan_year = 0;

        protected bool $reconstruct_candles = true;

        protected $candles_b1s = null;  // agregated candles for 1s from buyer ticks
        protected $candles_s1s = null;

        protected $flush_count = 0;

        protected $rebuild_time_last = 0;

        protected $rebuild_task = null;
            
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
            $this->candles_b1s = $this->CreateMCC('b1s');            
            $this->candles_s1s = $this->CreateMCC('s1s');
        }

        private function CreateMCC(string $name): CandlesCache {            
            $mc = new CandlesCache($this);                        
            $mc->interval = 1;
            $mc->key = $name;  // table suffix
            return $mc;            
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

        public function CreateTables (): bool {
            $mysqli_df = sqli_df();
            $result = parent::CreateTables();
            $prefix = "candles__{$this->ticker}";
            $proto = str_replace('ticks', 'candles', $this->table_proto_ch);
            $result &= $this->CreateTable($mysqli_df, $proto, "{$prefix}" ); // minute candles if not exists
            $result &= $this->CreateTable($mysqli_df, $proto, "{$prefix}__b1s" ); // buyer 1s candles
            $result &= $this->CreateTable($mysqli_df, $proto, "{$prefix}__s1s" ); // seller 1s candles
            return $result;
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
            $this->ProcessAggregate(); // handling mini_cache with latest imported ticks
            $this->SaveMiniCandles($this->candles_b1s);
            $this->SaveMiniCandles($this->candles_s1s);

            $cache_size = count($this->cache);
            $start = $this->cache->oldest_ms();
            $saved = $this->SaveToDB($this->cache);
            $this->last_aggregated = 0;
            $this->rest_loads += $saved;
            
            $t_start = pr_time();            
            $fc = $this->flush_count ++; 
            // количество сбросов за час для одного загрузчика варьируется, по одному на каждый завершенный блок, или больше в случае тяжелых данных
            if (10 == $fc % 100) {
                $part = date_ms('Y-m-01', $start);
                sqli_df()->try_query("OPTIMIZE TABLE {$this->table_name} PARTITION ('$part') FINAL DEDUPLICATE"); // с некоторой вероятностью за месяц эта процедура много раз выполнятся будет
                $t_opt = pr_time() - $t_start;
                log_cmsg("~C97#DB_PERF(Flush-$fc):~C00 saved to DB %d / %d records, opt. time = %.1f s", $saved, $cache_size, $t_opt);
            }
            else
                log_cmsg("~C97#DB_PERF(Flush-$fc):~C00 saved to DB %d / %d records", $saved, $cache_size);
            
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
            if ($this->total_scheduled > 0)  {                
                $min_ts = gmdate('Y-m-d', $this->history_first);
                $mysqli->try_query("DELETE FROM download_schedule WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks') AND (date < '$min_ts')"); // костыль пока не убирать!
                $schedule = $mysqli->select_map('date, target_volume', 'download_schedule', 
                                                  "WHERE $strict AND (date >= '$actual_ts') ORDER BY date LIMIT {$this->max_blocks}");
            }

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

            if (true === $data) {
                $data = $this->ws_raw_data; 
                $this->ws_raw_data = [];
            }

            if (!is_array($data)) {
                log_cmsg("~C91#IMPORT_RAW_FAILED:~C00 invalid data type %s", var_export($data, true));
                return 0;
            }

            if (0 == count($data)) return 0;
            

            $ticks = $this->ImportTicks($data, "WebSocket$context");            
            $imp = 0;
            if (is_object($ticks)) {
                $imp = count($ticks);
                $this->ProcessAggregate($ticks);
            }
            $msg = format_color("from WebSocket imported %d:%s with source size %d ticks, first: %s\n", $imp, gettype($data), count($data), json_encode($data[0]));
            if (0 == $imp && count($data) > 0)
                log_cmsg("~C91#IMPORT_WARN~C00: $msg");
            
            file_put_contents( $this->ws_log_file, $msg);
            $this->ws_loads += $imp;
            $this->ws_time_last = time();
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
            $filled = '~C43~C30['.$block->FormatFilled().']~C00';                                
            $index = $block->index;
            $this->last_error = '';
            $tmp_dir = $this->get_manager()->tmp_dir.'/blocks';
            check_mkdir($tmp_dir);
            $prefix = ' ~C93#BLOCK_COMPLETE';
            if ($block->covers($this->history_first) && $this->loaded_full()) {
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

            $mysqli = sqli();

            $saldo_volume = $block->SaldoVolume();                
            $check = sqli_df()->select_row('DATE(ts) as date, SUM(amount) as volume, MIN(price) as low, MAX(price) as high, COUNT(*) as count', $this->table_name, 
                                "FINAL WHERE DATE(ts) = '$day' AND amount > 0 GROUP BY DATE(ts) LIMIT 1; -- check", MYSQLI_OBJECT);

                                             
            $check_volume = is_object($check) ? $check->volume : $saldo_volume;                
            $check_date = is_object($check) ? $check->date : '<null>';
            $count = is_object($check) ? $check->count : count($block);           

            $s_info = 'postponed for verify/retry, ';

            $attempts = $mysqli->select_value('COUNT(*)', 'download_history', 
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
            $block->time_strict = ''; // autodetect available time range
            if ($saldo_volume >= $block->target_volume)
                $block->time_strict = "DATE(ts) = '$day'";

            if ($this->reconstruct_candles && $block->index >= 0 && $need_recovery)
                $recv = $this->RecoveryCandles($block, $check_volume,true);  // recovery absent minute candles

            // для тиков целевой объем должен быть меньше или равен набранному, плюс минус погрешность-толерантность
            // исключение: удалось произвести апгрейд свечей 
            $inaccuracy = 0.01; // погрешность суммирования float с очень разным порядком 0.01%                
            $good_enough = abs($diff_pp) < $this->volume_tolerance;    
            $good_enough &= $recv > 0 || $diff_pp + $inaccuracy >= 0;

            // избавиться от повтора загрузки, при наличии приличий...    
            if ($good_enough || $attempts >= 2 || $recv > 0)  {
                $replica = $mysqli->replica;
                $res = $mysqli->try_query("DELETE FROM `download_schedule` WHERE (ticker = '{$this->ticker}') AND (kind = 'ticks') AND (date = '$day')");                
                $afr = $mysqli->affected_rows;
                if ($replica) {
                    $afr += $replica->affected_rows;
                    $s_info .= "RDBG: {$replica->last_query}, ";
                }
                $s_info = $res ? sprintf('removed from scheduler(s) %d, ', $afr) : "~C91 failed~C00 remove '$day' from scheduler(s): {$mysqli->error}, ";
            }                
           
            $load_result = 'full-filled';
            $this->mini_cache = null;
            $this->ProcessAggregate();


            if ($good_enough) {
                $whole_load = true;
                $summary = is_object($check) && $check instanceof mysqli_row  ? strval($check) : json_encode($check);
                log_cmsg("$prefix($symbol/$index/$attempts):~C00 %s, attempts %d, %s,\n\t\ttarget_volume reached %s in %d ticks, block summary %s, sess. time spent %.1f min, $s_info filled in session: %s ", 
                            strval($block), $attempts, $block->info, format_qty($check_volume), $count, $summary, $block->sess_load_time, $filled);    
                if (-1 == $block->index)
                    $this->RecoveryCandles($block, $check_volume);  // upgrade latest
                
            } elseif ($block instanceof TicksCache) {
                $whole_load = $diff < 0;
                $problem = $diff > 0 ? 'PARTIAL' : 'EXCESS';
                if (0 == $check_volume)
                    $problem = 'VOID';
                $load_result = sprintf('%s diff %.2f%%', $problem, $diff_pp);

                log_cmsg("~C04{$prefix}_$problem($symbol/$index/$attempts):~C00 %s, attempts %d, %s, target_volume reached %s (saldo %s) instead %s daily (diff %.2f%%) and %s minutes,\n\t\t $s_info block summary %s, filled in session: %s ", 
                            strval($block), $attempts, $block->info, 
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
                    if ($block->minutes_volume < $block->target_volume)
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

                 
            $load_result = substr($load_result, 0, 32);
            $query = "INSERT INTO `download_history` (`ts`, `date`, `kind`, `ticker`, `count`, `volume`, `result`)\n VALUES";
            $query .= sprintf("(NOW(), '$day', 'ticks', '%s', %d, %f, '%s')", 
                            $this->ticker, $count, $check_volume, $load_result);
            sqli()->try_query($query); // по сути это журнал загрузки. Сверка с ним, позволит избежать повторов без ручной очистки                                            
            $block->Reset($whole_load); // clean memory
            if ($whole_load) {
                shell_exec("rm {$this->cache_dir}/*.json"); // remove files from prevous sessions            
                $this->ReleaseMiniCandles($this->candles_b1s, $day);
                $this->ReleaseMiniCandles($this->candles_s1s, $day);
            }
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

        protected function ProcessAggregate(TicksCache $cache = null) {
            $method = 'Combine';
            if (null === $cache) {
                $cache = is_object($this->mini_cache) ? $this->mini_cache : $this->cache;                        
                $method = is_object( $this->mini_cache) ? 'Combine' : 'SetRow';
            }
            $t_start = pr_time();
            $data = array_slice($cache->Export(), $cache->last_aggregated);
            if (0 == count($data)) return; // already processed            
            $bc = $this->ReconstructCandles($data, 1, false);
            $sc = $this->ReconstructCandles($data, 1, true);
            $mcb = $this->candles_b1s;
            $mcs = $this->candles_s1s;
            foreach ($bc as $t => $rec)
                $mcb->$method($t, $rec); // buyer 1s candle add/upgrade
            foreach ($sc as $t => $rec)
                $mcs->$method($t, $rec); // seller 1s candle add/upgrade

            $cache->last_aggregated = count($cache);
            $elps = pr_time() - $t_start;
            if ($elps > 0.1)
                log_cmsg("~C96 #PERF_AGGR:~C00 from %d ticks produced %d buyer and %d seller 1s candles, elapsed: %.3f s",  
                        count($data), count($bc), count($sc), $elps);
        }

        public function ProcessPause(int $us) {                        
            parent::ProcessPause($us);
            if (0 == $this->shadow_jobs) return;            
            $t_start = pr_time();
            $this->ProcessAggregate();            
            $this->ProcessTasks(true);           

            $elps = pr_time() - $t_start;
            if ($elps >= 0.5)
                $this->shadow_jobs = 0;
            else
                $this->shadow_jobs --;
        }

        public function ProcessTasks(bool $fast = false) {
            $rbc = $this->rebuild_task ?? sqli()->select_row('date,target_volume', 'download_schedule',
                                                             "WHERE (ticker = '{$this->ticker}') AND (kind = 'rebuild-c') LIMIT 1", MYSQLI_OBJECT);
            $mgr = $this->get_manager();               
            $timeout = 30;                                              
            while ($rbc && $this->rebuild_time_last < 10 && $mgr->active) {
                set_time_limit(50);
                $res = $this->RebuildCandles($rbc);
                $this->rebuild_task = $res ? null : $rbc; // continue later if not finished                    
                if ($fast || $res) break; 
                $mgr->ProcessWS(false);
                if ($timeout-- <= 0) {
                    log_cmsg("~C91 #ERROR:~C00 deadlock in rebuild task %s for %s", $rbc->date, $this->ticker);
                    break;
                }
            }
        }

        /** Rebuild candles from ticks full day load from DB. 
         * returns true if finished day
          */
        protected function RebuildCandles(\stdClass $job): bool {
            $mysqli_df = sqli_df();
            $hour = $job->hour ?? 0;
            $date = $job->date;
            $target_vol = $job->target_volume; 

            $src = $this->table_name;    
            $saldo_vol = 0;        
            $saldo_elps = $job->elapsed ?? 0;

            $candles_table = str_replace('ticks', 'candles', $this->table_name);
            $time_strict = '';
            $part_id = date('Ym01', strtotime($date));
            for ($h = $hour; $h < 24; $h++) {
                $job->hour = $h + 1; // for next iteration if not finished now

                $t_start = pr_time();                                
                $hour_start = sprintf("$date %02d:00:00", $h);
                $hour_end = format_ts( strtotime($hour_start) + 3600 );
                $time_strict = "(ts >= '$hour_start') AND ts < ('$hour_end')"; // seems its fast with index
                $rows = $mysqli_df->select_map('trade_no, toUnixTimestamp(ts) * 1000 + toMillisecond(ts), buy, price, amount, trade_no', $src, 
                                        "WHERE _partition_id = '$part_id' AND $time_strict", MYSQLI_NUM);
                if (0 == count($rows))                    
                    continue;                                                      
                
                foreach ($rows as $k => $row) {
                    $rows[$k][TICK_TMS] = $row[TICK_TMS] * 1; 
                    $rows[$k][TICK_PRICE] = $row[TICK_PRICE] * 1;
                    $rows[$k][TICK_AMOUNT] = $row[TICK_AMOUNT] * 1;
                    $rows[$k][TICK_BUY] = $row[TICK_BUY] * 1; 
                }
                $t_load = pr_time() - $t_start;
                $mc = new TicksCache($this);
                $mc->key = $date;
                $mc->target_volume = $target_vol; // prevent upgrade daily volume here
                $mc->Import($rows);
                $volume = $mc->SaldoVolume();
                $saldo_vol += $volume;
                $mc->time_strict = $time_strict;
                $mc->index = 1000 + $h; // marking visual
                $this->RecoveryCandles($mc, $volume, true); // rebuild for single hour
                $this->ProcessAggregate($mc); // rebuild 1s candles
                $elps = pr_time() - $t_start;
                $saldo_elps += $elps;
                $job->elapsed = $saldo_elps;                
                $this->rebuild_time_last = $elps;
                
                if ($elps > 0.5) {
                    log_cmsg("~C96#PERF_REBUILD_CANDLES:~C00 hour %02d loaded %7d ticks, %s volume, strict %s, load time %.3f, summary %.3f ", 
                            $h, count($rows), format_qty($volume), $mc->time_strict, $t_load, $elps);                            
                    break;
                }                
            }            
            
            if ($job->hour < 24) return false; 
            set_time_limit(55);
            $this->SaveMiniCandles($this->candles_b1s);
            $this->SaveMiniCandles($this->candles_s1s);
            $this->ReleaseMiniCandles($this->candles_b1s, $date);
            $this->ReleaseMiniCandles($this->candles_s1s, $date);

            $query = "UPDATE {$candles_table}__1D SET `volume` = GREATEST(`volume`, $saldo_vol) WHERE Date(`ts`) = '$date';";
            sqli()->try_query($query);
            $query = "DELETE FROM `download_schedule` WHERE (ticker = '{$this->ticker}') AND (kind = 'rebuild-c') AND (date = '$date');";
            sqli()->try_query($query);
            log_cmsg("~C96#REBUILD_CANDLES:~C00 %s, saldo volume %s, last strict %s, elapsed %.3f s", 
                    $date, format_qty($saldo_vol), $time_strict, $saldo_elps);
            return true;            
        }

        protected function ReconstructCandles(array $source, int $period = 60, mixed $exclude = null): array {
            $c_map = []; // candles result         
            $saldo_tv = 0;   
            foreach ($source as $tick) {
                $tk = floor($tick[TICK_TMS] / $period / 1000) * $period; // round to minute into seconds                
                $tp = $tick[TICK_PRICE];
                $tv = $tick[TICK_AMOUNT];                
                if ($tick[TICK_BUY] === $exclude) continue; // for separate candles

                $saldo_tv += $tv;
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
            $saldo_cv = 0;
            foreach ($c_map as $tk => $rec) 
                $saldo_cv += $rec[CANDLE_VOLUME];

            $side = $exclude ? 'sell' : ($exclude !== null ? 'buy' : 'both');

            if ($saldo_cv < $saldo_tv * 0.999) 
                log_cmsg("~C31#WARN_VOLUME_LOW(ReconstructCandles):~C00 saldo volume %s vs ticks source %s, side %s", 
                            format_qty($saldo_cv), format_qty($saldo_tv), $side);
                

            return $c_map;
        }

        protected function RecoveryCandles(TicksCache $source, float $ticks_volume, bool $reconstruct = false) {
            global $mysqli, $mysqli_df;
            if (0 == count($source)) return;
            $start = $source->oldest_ms();
            $end  = $source->newest_ms();
            $idx = $source->index;            
            $date = $source->key;
            $ticks = $source->Export();
            $candles_table = str_replace('ticks', 'candles', $this->table_name);                                            
            $updated = 0;

            if (-1 == $idx) {
                $m_start = floor(time_ms() / 60000) * 60000;
                $latest = [];
                foreach ($ticks as $tick) 
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
            
            if (null == $source->time_strict && $idx >= 1000) {
                log_cmsg("~C31 #STRANGE:~C00 unset time_strict for hourly period recovery, index %d, stack: %s", $idx, format_backtrace());            
                $source->time_strict = sprintf("DATE(ts) = '%s' AND HOUR(ts) = %d", $date, $idx - 1000);
            }
            $tmr = new ProfileTimer();
            
            $ticks_count = count($ticks);
            $c_map = $this->ReconstructCandles($ticks);
            $start_t = floor($start / 60000) * 60; // round to minute into seconds
            $end_t   = ceil($end / 60000) * 60;
            $start_ts = format_ts( $start_t);            
            $end_ts = format_ts( $end_t);
            $tmr->add('calc');

            $strict = strlen($source->time_strict) > 10 ? $source->time_strict : "ts >= '$start_ts' AND ts < '$end_ts' -- autodetect";
                        
            $exists = $mysqli->select_map('ts,volume', $candles_table, "WHERE $strict");
            $tmr->add('load');
            $rows = [];            
            $info = "EXISTS {$source->key}: ". print_r($exists, true);
            $info .= "UPGRADE: ".print_r($c_map, true);
            $mgr = $this->get_manager();
            $prev_vol = 0;
            foreach ($exists as $vol)
                $prev_vol += $vol;

            $upgrade_pp = ($prev_vol > 0) ? 100 * $ticks_volume / $prev_vol - 100 : 100;

            if ($ticks_volume > 0 && $upgrade_pp <= 0.001) // float inaccuracy 0.001%
                return;             

            file_put_contents("{$mgr->tmp_dir}/candles_recovery_{$this->ticker}.txt", $info);                        
            $insert_vol = 0;
            foreach ($c_map as $tk => $rec) {
                $ts = format_ts($tk);
                [$o, $c, $h, $l, $v] = $rec;
                if (isset($exists[$ts])) $updated ++;              
                $rows []= "('$ts', $o, $c, $h, $l, $v)"; // insert/update lost candle
                $insert_vol += $v;
            }          
            
            if (count($rows) > 0) {
                $query = "INSERT INTO $candles_table (`ts`, `open`, `close`, `high`, `low`,`volume`) VALUES ";
                $query .= implode(",\n", $rows);      
                         
                $res_c = $mysqli_df->try_query($query); // always overwrite existed candles via ReplacingMergeTree
                $query .= "\n ON DUPLICATE KEY UPDATE `open` = VALUES(`open`), `close` = VALUES(`close`),";
                $query .= " `high` = VALUES(`high`), `low` = VALUES(`low`), `volume` = VALUES(`volume`)";
                // $query .= "\n\t`high` = GREATEST(`high`, VALUES(`high`)), `low` = LEAST(`low`, VALUES(`low`)), `volume` = GREATEST(`volume`, VALUES(`volume`))";
                $res_m = $mysqli->try_query($query);         
                $qins = $query;

                $tmr->add('insert');
                $elapsed = $tmr->elapsed();
                $timings = '~C00'.$tmr->dump(0.1, 'format_color').format_color(' total %.1f s', $elapsed);
                $inserted = count($rows) - $updated;
                $show_info = $elapsed >= 0.2 || !$res_m || !$res_c;

                if ($inserted > 0 && $show_info)
                    log_cmsg("~C92#RECOVERY_CANDLES($idx):~C00 %d missed candles inserted, %d updated in %s, volume change %s => %s, for MySQL %s, ClickHouse %s, timings: %s", 
                                $inserted, $updated, $candles_table, format_qty($prev_vol), format_qty($ticks_volume),
                                 b2s($res_m), b2s($res_c), $timings);
                elseif ($show_info) 
                    log_cmsg("~C92#RECOVERY_CANDLES($idx):~C00 %d candles updated in %s, volume change %s => %s (+%.3f%%), for MySQL %s, ClickHouse %s, timings: %s",
                                $updated, $candles_table, format_qty($prev_vol), format_qty($ticks_volume), $upgrade_pp,
                                b2s($res_m), b2s($res_c), $timings);            
                
                $daily_table = "{$candles_table}__1D";

                if ($ticks_volume > $source->target_volume ) {
                    $query = "UPDATE $daily_table SET `volume` = GREATEST(`volume`, $ticks_volume) WHERE Date(`ts`) = '$date';";
                    if ($mysqli->try_query($query)) {
                        $diff_pp = 100;
                        if ($source->target_volume > 0)
                            $diff_pp = 100 * $ticks_volume / $source->target_volume;
                        log_cmsg("~C92#RECOVERY_CANDLES:~C00 %s: daily candle %s volume upgraded from %7s to %7s %.1f%% ", 
                                    $daily_table, $date, format_qty($source->target_volume), format_qty($ticks_volume), $diff_pp);
                    }
                    else
                        log_cmsg("~C31#WARN_FAILED_UPDATE:~C00 %s__1D: daily candle volume still unchanged", $candles_table);
                }                

                
                $query = "INSERT IGNORE INTO `download_schedule` (`date`, `kind`, `ticker`, `target_volume`, `target_count`)\n VALUES";
                $query .= sprintf("('%s', 'sync-c', '%s', %f, 0)", 
                                 $source->key, $this->ticker, $ticks_volume);
                $mysqli->try_query($query); // загрузчик свечей в свою очередь осуществит синхронизацию данных в ClickHouse
                $stats_table = $this->stats_table;
                // INSERT INTO $stats_table (`date`, `day_start`, `day_end`, `volume_minutes` TODO: надо видимо вставлять запись, для предупреждения загрузчика свечей
                $cover_hours = ($end_t - $start_t) / 3600;
                if ($cover_hours > 1) {
                    $query = "UPDATE $stats_table SET repairs = repairs + 1 WHERE `date` = '$date' "; 
                    $mysqli->query($query);
                }

                if ($ticks_count > $source->target_count && $source->target_count > 0) {
                    $query = "UPDATE $daily_table SET `trades` = GREATEST(`trades`, $ticks_count)  WHERE Date(`ts`) = '$date';";    
                    $mysqli->try_query($query);
                }
                $first_t = array_key_first($c_map);
                $last_t = array_key_last($c_map);
                $range = sprintf("`ts` >= '%s' AND `ts` <= '%s'", format_ts($first_t), format_ts($last_t)); 
                $control = $mysqli->select_map('ts,volume', $candles_table, "WHERE $strict") ?? [];
                $ctrl_vol = 0;
                $diffs = [];
                foreach ($control as $ts => $real_vol) {
                    $prev = $exists[$ts] ?? 0;
                    $rec = $c_map[strtotime($ts)] ?? [0, 0, 0, 0, 0];
                    $upd = $rec[CANDLE_VOLUME] ?? 0;                                         
                    $ctrl_vol += $real_vol;
                    if (!same_values($upd, $real_vol) && !same_values($prev, $real_vol)) {
                        $info = format_qty($prev) . ' => '. format_qty($upd). ' => '. format_qty($real_vol);                                                
                        file_add_contents("{$mgr->tmp_dir}/candles_recovery_{$this->ticker}.sm", "$ts: $info\n");
                        $diffs[$ts] = $info;
                    }
                }       
                
                $diff_vol = $insert_vol - $ctrl_vol;
                $diff_pp = 100 * $diff_vol / max($ctrl_vol, $insert_vol, $ticks_volume, 0.01);
                if ($diff_pp > 0.01 || count($diffs) > 0) {  //  жаловаться только если объем вставки меньше результата или есть несоответствия
                    log_cmsg("~C31#WARN_SYNC_MISTMATCH:~C00 after checking volume diff %.2f%%, candles %s vs prev %s vs insert %s, produced range %s vs strict %s, diffs: %s", 
                                            $diff_pp, format_qty($ctrl_vol), format_qty($prev_vol), format_qty($insert_vol),
                                            $range, $strict, json_encode($diffs));
                }
            }
            else    
                log_cmsg("~C33#RECOVERY_CANDLES:~C00 no candles generated for update %s from %d ticks", $candles_table, $ticks_count);
            return $inserted + $updated;
        }

        protected function ReleaseMiniCandles(CandlesCache $mcache, string $date) {
            if (0 == count($mcache)) return;
            $start = strtotime("$date 00:00");
            $end   = $start + SECONDS_PER_DAY - 1;
            $save = []; 
            foreach ($mcache->Export() as $t => $rec) { // linear cutout whole day candles
                if ($t < $start || $t > $end) 
                    $save [$t]= $rec;
            }
            $mcache->Import($save, true);
        }

        protected function SaveMiniCandles(CandlesCache $mcache) {  // only into ClickHouse tables
            $values = [];
            if (0 == count($mcache)) return; // maximum estimated 86400 
            foreach ($mcache->keys as $tk) {                                               
                if ($mcache[$tk][CANDLE_VOLUME] > 0) 
                    $values []= $mcache->FormatRow($tk);                
            }

            $table_name = "candles__{$this->ticker}__{$mcache->key}";
            $query = "INSERT INTO $table_name (`ts`, `open`, `close`, `high`, `low`, `volume`, `flags`)\n";
            $query .= 'VALUES  '.implode(",\n", $values).";";                
            sqli_df()->try_query($query); 
            log_cmsg("~C96 #PERF_DB:~C00 saved %d mini candles into %s, in  %.1f seconds", count($values), $table_name, sqli_df()->last_query_time);
            // IDEA: cleanup except latest record...?
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
            
            $this->scan_year ++;
            $base = 2012 + date('n');            
            $years = date('Y') - 2012 + 1;
            $year = 2012 + ($this->scan_year + $base) % $years;

            $month = date('g'); // scan single partition on DB - better efficiency. Full history covered twice per day
            $start = max($start, strtotime(HISTORY_MIN_TS));                                  
            $start = max($start, $this->history_first);
            $single = strtotime("$year-$month-01");            

            if ($this->scan_year > $years) { // ненужно сканировать больше
                $this->zero_scans ++;                
                return;     
            }

            $ts_start = format_ts($start);            
            log_cmsg("~C96#PERF(ScanIncomplete):~C00 gathering stats for %s, checking period %d-%02d, ts_start = %s", $table_name, $year, $month, $ts_start);
            $bad_candles = [];
            $sched_rows = [];

RESTART:                
                            
            $skips = [0, 0, 0, 0];
            $period = "( YEAR(ts) = $year ) AND ( MONTH(ts) = $month ) AND ( ts >= '$ts_start' )";

            $control_map = [];
            // control ClickHouse candles by MySQL daily candles
            $columns = 'DATE(ts) as date, MIN(ts) as min, MAX(ts) as max, SUM(volume) as volume, COUNT(*) as count';            
            $control_map = $mysqli->select_map($columns, $candle_tab, "WHERE $period AND volume > 0 GROUP BY DATE(ts) -- control_map", MYSQLI_OBJECT) ?? [];                
            // контрольная таблица - минутные свечи, что позволяет определить диапазон времени, в котором есть данные
            if ($mysqli_df->table_exists($candle_tab))  {                                     
                $ref_map = $mysqli_df->select_map($columns, $candle_tab, "FINAL WHERE $period AND volume > 0 GROUP BY DATE(ts) -- ref_map", MYSQLI_OBJECT) ?? [];                
                foreach ($ref_map as $day => $rec)  { 
                    $day_ex = $extremums[$day] ?? null;
                    if (isset($control_map[$day])) {
                        $mysql_vol = $control_map[$day]->volume;
                        $chdb_vol =  $rec->volume; 
                        $diff = $mysql_vol - $chdb_vol;
                        $diff_pp = 100 * $diff / max($chdb_vol, $mysql_vol, 0.01);
                        if (abs($diff_pp) > 0.1) {
                            log_cmsg("~C31 #WARN:~C00 candles volume mismatch for ClickHouse %s: %s vs MySQL %s, diff = %.1f%%", 
                                        $day, format_qty($chdb_vol), format_qty($mysql_vol), $diff_pp);                            
                            $sched_rows []= sprintf("('%s', 'sync-c', '%s', %f, 0)", $day, $this->ticker, $mysql_vol);
                        }
                    }
                    elseif (isset($rec->volume) )  {  
                        log_cmsg("~C31 #WARN:~C00 in MySQL absent day %s, but avail in ClickHouse: %s ", $day, json_encode($rec));
                        $control_map[$day] = $rec;
                        $bad_candles [$day] = $day_ex ? $day_ex->volume : $rec->volume;                        
                    }
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
                $history[$day] = ($history[$day] ?? 0) + 1;
            }
            unset($control_map[$today]);                                                              
            unset($days_map[$today]);
            $unchecked = [];
            $rescan = [];
            $this->blocks = [];
            $this->blocks_map = [];
            $full = $partial = 0;

            $voids = 0;
            if (!$this->initialized) {
                // первичный поиск - оценка неполноты блоков, по всей истории по заполнению разделов. Если в разделе данных нет, при наличии хотя-бы одной дневной свечи, нужно добавить в расписание
                // каждый раздел охватывает месяц, если ничего не поменялось, и имеет название в формате даты первого числа
                $db_name = DB_NAME;
                $parts = $mysqli_df->select_map('partition, rows', 'system.parts', "WHERE `table` = '$table_name' AND `active` = 1 AND `database` = '$db_name' ", MYSQLI_OBJECT);
                foreach ($extremums as $date => $rec) {
                    $yms = substr($date, 0, 7).'-01'; // year-month
                    if (isset($parts[$yms]) || array_value($history, $date, 0) >= 2 || strtotime($yms) < $start) continue;
                    $unchecked [$date] = $rec;                      
                    $voids ++;
                }                
                if ($voids > 0) goto SKIP_SCAN;
            }

            $unchecked = $control_map;                        
            $dark_zone = $this->initialized ? $start : strtotime(HISTORY_MIN_TS); 
            // стратегия поменялась: теперь загрузка с самого начала, до победного конца. Чтобы в БД все ложилось последовательно и доступ был оптимальный. Так собирать сложные свечи будет быстрее
            ksort($control_map);  
            $first_day = array_key_first($control_map);         
            $t_start = pr_time();       

            
                 
            
            foreach ($control_map as $day => $cstat) {        
                $elps = pr_time() - $t_start;            
                if (!$mgr->active || $elps > 30) return;     // stop yet now
                $mgr->ProcessWS(false);  // support WebSocket minimal latency
                unset($unchecked[$day]);
                $block = $this->DayBlock($day, $cstat->volume, false);
                $sod_t = strtotime($day);                        
                if ($sod_t <= $dark_zone) {                    
                    $skips[0] ++;
                    continue;
                }
                

                if (!isset($days_map[$day])) {
                    log_cmsg("~C33 #VOID_BLOCK_DETECTED($this->symbol):~C00 at %s volume = %s", $day, $cstat->volume);                                             
                    $rescan []= $block;
                    $skips[1] ++;
                    continue;
                }                    

                $block->minutes_volume = $candles_vol = $cstat->volume;                       
                $meta = $days_map[$day];
                $last_price = 0;
                $target_trades = 0;
                $day_ex = $extremums[$day] ?? null;
                if (is_object($day_ex)) {                    
                    $target_trades = $day_ex->trades;
                    $extra = $day_ex->volume - $candles_vol;
                    $extra_pp = 100 * $extra / max($candles_vol, $day_ex->volume);
                    if ( abs($extra_pp) >= 0.1) {
                        log_cmsg("~C31 #VOLUME_MISMATCH({$this->ticker}):~C00 for %s candles: saldo %s != daily %s, diff = %s (%.2f%%). Using from higher timeframe",
                                $day, format_qty($candles_vol), format_qty($day_ex->volume), format_qty($extra), $extra_pp); 
                        $candles_vol =  $day_ex->volume; // 1D table can be manually patched, means have max priority                    
                        $bad_candles [$day] = $candles_vol;                                
                    }
                }

                $attempts = array_value($history, $day, 0); // well fast get attempts count                                    
                if ($attempts > 3) { // TODO: check code not duplicated
                    $cleanup = sprintf("DELETE FROM download_history WHERE date = '$day' AND kind = 'ticks' AND ticker = '{$this->ticker}' LIMIT %d ", $attempts - 3);
                    $mysqli->try_query($cleanup);
                }
                $min = $meta->min;
                $max = $meta->max;                    
                $void_left  = max(0, $block->min_avail - strtotime($cstat->min)) / 60.0;
                $void_right = max(0, strtotime($cstat->max) + 59 - $block->max_avail) / 60.0;

                $vdiff = $candles_vol - $meta->volume;
                $inacuracy = $candles_vol * 0.001;  // float inaccuracy
                $vdiff_pp = 100 * $vdiff / max($candles_vol, $meta->volume, 0.1);                                                                            
                $vol_info = '';
                $excess = false;
                $incomplete = false;

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
                        $incomplete = true;
                        if ($vdiff > 0) 
                            $vol_info = sprintf('~C00, ~C93less~C00 relative candles =~C95 %8.1f%%~C00, diff +', $vdiff_pp);                                                            
                        else {
                            $vol_info = sprintf('~C00, ~C04~C93excess~C00 relative candles  ~C95 %8s~C00, %8.2f%% diff +',  format_qty($candles_vol), $vdiff_pp);
                            $excess = true; // если эта проблема сохраняется, значит минутки не пересобрались в прошлый раз. Надо проверять логику...                                
                        }
                        $vol_info .= format_qty($vdiff) ;
                    }                       
                    /* if ($meta->count < $target_trades) {
                        $incomplete = true;
                        $vol_info .= sprintf('~C00, ~C91insufficiently~C00 trades count %d < %d', $meta->count, $target_trades);
                    } // */
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
                                
                $block->code = $incomplete ? BLOCK_CODE::PARTIAL : BLOCK_CODE::FULL;
                
                if ($block->code == BLOCK_CODE::FULL) {
                    $full ++; // not add to rescan
                    $skips[2] ++;
                    continue;
                }
                if (0 == $meta->volume) 
                    $block->code = BLOCK_CODE::NOT_LOADED;

                if ($attempts > 1) {
                    log_cmsg("~C33 #SCAN_BLOCK_SKIPPED({$this->ticker}):~C00 %s, already %d attempts to load", $day, $attempts);
                    $skips[3] ++;
                    continue; 
                }                                    

                $block->index = count($rescan);                    
                $block->target_volume = $candles_vol;
                $block->target_close = $cstat->close;
                $block->target_count = $target_trades;
                $block->db_need_clean = $excess;
                $rescan []= $block;
                $partial ++;
                $tag = $excess ? '~C04~C97#BLOCK_NEED_RELOAD' : '~C94#BLOCK_NEED_FINISH';
                log_cmsg("$tag({$this->symbol}):~C00 index %4d, day %s [%s..%s, volume %10s%s in %d ticks]; %s margins [%.3f, %.3f] hours, attempt %d, code %s",
                                $block->index, $day, $min, $max, 
                                format_qty($meta->volume), $vol_info, $meta->count, $price_info,                                            
                                    $void_left, $void_right, $attempts, $block->code->name);
                   
            } // foreach control_map - main scan loop
SKIP_SCAN:          
            
            if (!$this->initialized) {
                ksort($unchecked);
                foreach ($unchecked as $day => $meta) {
                    if (strtotime("$day 00:00") < $start) 
                        continue;                  
                    // if (count($rescan) >= $this->max_blocks) break;
                    log_cmsg("~C33#VOID_BLOCK_ADD({$this->ticker}):~C00 schedudled %s with target volume %s", $day, $meta->volume);
                    $rescan []= $block = $this->DayBlock($day, $meta->volume, false);                                                  
                    if (isset($meta->close))
                        $block->target_close = $meta->close;
                    if (isset($meta->trades))
                        $block->target_count = $meta->trades;
                }                                               
            }

            $query = "INSERT IGNORE INTO `download_schedule` (date, kind, ticker, target_volume, target_close) VALUES ";
            $rows = $sched_rows;
            // полное отложенное расписание
            foreach ($rescan as $block) {
                if ($block->IsFull()) continue; 
                $rows []= sprintf("('%s', 'ticks', '%s', %f, 0)", 
                                    $block->key, $this->ticker, $block->target_volume);
            }
            foreach ($bad_candles as $day => $tv) {
                $rows []= sprintf("('%s', 'rebuild-c', '%s', %f, 0)", 
                                    $day, $this->ticker, $tv);
            }

            if (count($rows) > 0) {
                $query .= implode(",\n", $rows);
                if ($mysqli->query($query)) // WARN: need ignore replica
                    log_cmsg("~C97#SCHEDULE:~C00 for %s added %d / %d rows to scheduler on %s ", $this->ticker, $mysqli->affected_rows, count($rows), $mysqli->host_info);
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
            if (0 == ($full + $partial) && count($control_map) > 0)
                $tag = "~C91 #MISSED_BLOCKS:";
            log_cmsg("~C04~C97$tag:~C00 for symbol %s will used range %s .. %s, expected download %d blocks, %d partial, %d full from %d, skips: %s", $this->symbol,
                    $first, $last, $this->BlocksCount(), $partial, $full, count($control_map), json_encode($skips));            
        } // function ScanIncomplete
    

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
        if (!$mysqli) 
            error_exit("~C91 #FATAL:~C00 cannot initialze DB interface! ");                    
        if (MYSQL_REPLICA)
            $mysqli->replica = init_replica_db(DB_NAME, [MYSQL_REPLICA]);

        $mysqli_df = ClickHouseConnectMySQL(null, null, null, DB_NAME);  
        if ($mysqli_df)
            log_cmsg("~C93 #START:~C00 MySQL interface connected to~C92 %s@$db_name_active~C00 ", $db_servers[0] ); 
        else
            error_exit("~C91#FATAL:~C00 cannot connect to ClickHouse DB via MySQL interface! ");

        if (CLICKHOUSE_REPLICA) { // if const are defined not as null/false            
            $mysqli_df->replica = ClickHouseConnectMySQL(CLICKHOUSE_REPLICA.':9004', null, null, DB_NAME);
            if (is_object($mysqli_df->replica)) 
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
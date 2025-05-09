<?php
    require_once 'loader_proto.php';
    require_once 'data_block.php';   
    define('END_OF_TODAY', floor_to_day(time()) + SECONDS_PER_DAY - 1); // non static - not really const!    



    abstract class BlockDataDownloader 
             extends DataDownloader {
        
        protected   $blocks = [];
        public      $blocks_map = []; // typically [date] = DataBlock

        public      object $first_block;  // только для определения начала истории
        public      object $last_block;   // включает как минимум текущий день

        protected   $block_class = 'DataBlock';

        protected   int $default_limit = 1000; // how many records load at once via REST API        
        protected   int $days_per_block = 5;
        protected   int $blocks_at_once = 10;
        protected   int $timeout_at_once = 5; 
        public      $current_interval = 10; // используется при обработке импорта и округлении шага загрузки

        public     int $db_oldest = 0; // saved in session, not global
        public     int $db_newest = 0; // saved in session, not global

        protected  int $saved_min = 0; // timestamp of first item in DB
        protected  int $saved_max = 0; // timestamp of last item in DB
        public     int $first_ts = 0;  // first item timestamp was imported in session

        /** @var $newest_ms - last item timestamp was imported in session  */
        public     int $newest_ms = 0;  // 

        /**  @var $oldest_ms - timestamp in ms of oldest tick loaded in session */
        public      int $oldest_ms = -1; 

        

        public     int $history_first = 0;  // first timestamp in ALL history, detected via API

        /* max time for REST Download cycle. У многих бирж накопление rate limit привязывается к отдельному инструменту в течении минуты */
        public     int $cycle_time = 50;   
        public     bool $cycle_break = false;
        public     int $max_blocks = 30;  // how much blocks to load in one session

        public      $head_loaded = false;
        public      $tail_loaded = false;

        protected   $initialized = false;
        protected   $init_count = 0;
        public      $init_time_last = 0;  // time of last blocks scan

        public      int $loaded_blocks = 0;
        protected   $import_method = '';  // converter from API format to common
        protected   $load_method = '';  // REST loader for kind of data
        protected   string $data_name;  // for tables naming and logging

        protected  $stats_table = '';

        protected   $table_create_code = ''; // for MySQL loaded by CorrectTables
        protected   $table_create_code_ch = ''; // for ClickHouse loaded by CorrectTables

        protected   $table_corrected = false;
        protected   $table_engine = '';
        protected   $table_info = null;        // record describes stats for table bigdata (start_part, end_part, min_time, max_time, size)
        protected   $table_need_recovery = false;

        protected   $total_requests = 0;
        

        protected    $cache = null; // cache object, descendant from DataBlock                

        protected    $cache_file_last = '';
        
        protected   $rqs_cache_map = []; // [md5(rqs)] = API response
        protected   $cache_hit = false;

        protected   $last_api_headers = ''; //  last API response headers for ratelimit control
        protected   $last_api_request = ''; // last API request by api_request function
        protected   $last_api_rqt = 0.0; // last API request time in seconds
 
        protected   $last_requests = []; // cache for API requests
                
        protected   $last_error = '';

        protected   $block_load_history = []; // для предотвращения многократных загрузок, одного и того-же, по любым причинам

        protected   $nothing_to_download = false; // выставляется, если после сканирования блоков, все оказались заполненны

        protected  $volume_tolerance = 0.01;  // in % for block completion

        protected  $partitioned = true;

        protected  $zero_scans = 0;


        public function __construct(DownloadManager $mngr, \stdClass $ti) {
            $this->oldest_ms = $this->db_oldest = time_ms();
            parent::__construct($mngr, $ti);            
            if (!$this->data_name)
                throw ErrorException("~C91ERROR:~C00 data_name not set for {$this->symbol} before calling constructor");

            $this->stats_table = $mngr->TableName("stats__{$this->ticker}");
            $this->table_name = $mngr->TableName("{$this->data_name}__{$this->ticker}");
            $this->tables['data'] = $this->table_name;
            $day_start = floor_to_day(time());  
            $class = $this->block_class;                      
            $this->first_block = new $class($this, EXCHANGE_START_SEC,  $day_start - 1); // HistoryFirst must adjust block bounds later                                   
            $this->first_block->index = -2;
            $this->last_block = new $class($this, $day_start, END_OF_TODAY);            
            $this->last_block->index = -1;
            if (0 === stripos($this->symbol, 'BTCUSD') || 0 === stripos($this->symbol, 'ETHUSD'))
                $this->normal_delay = 30;             
        }


        protected function api_request(string $url, array $params, int $cache_for = 180): string { // REST API request with typical GET params.
            global $rest_allowed_ts, $curl_resp_header;
            $res = 'fail?'; 
            $this->cache_file_last = '';
            $fresh_rqs = [];
            $t_now = time();
            foreach ($this->last_requests as $rqt_remove => $rqs)
                if ($t_now < $rqt_remove)
                    $fresh_rqs [$rqt_remove] = $rqs;
                else {
                    $key = md5($rqs);
                    unset($this->rqs_cache_map[$key]); // remove expired cache
                }
        
            $this->last_requests = $fresh_rqs;
            $rqs = $url.'?'.http_build_query($params);         
            $rqs_dbg = urldecode($rqs);
            $key = md5($rqs);
            $last_rqt = array_search($rqs, $this->last_requests);       
            $this->cache_hit = false !== $last_rqt;

            if ($this->cache_hit && $cache_for > 0) {           
                $last_tss = gmdate('Y-m-d H:i:s', $last_rqt);                                                
                if (isset($this->rqs_cache_map[$key])) {
                    $res = $this->rqs_cache_map[$key];
                    $len = strlen($res);
                    $info = $len > 20 ? "~C37 length~C96 $len~C00" : $res;
                    log_cmsg("\t~C31#CACHE_HIT~C00: requesting data %s was already performed before (expired at %s). By key %s returns from cached %s ",
                                json_encode($params), $last_tss, $key,  $info);               
                    return $res;
                }
            }          
            $cache_dir = str_replace($this->rest_api_url, '', $url);
            $cache_dir = str_replace('//', '/', "/cache/$cache_dir"); // if possible - mount zram compressed lz4
            
            check_mkdir($cache_dir);
            $cache_file = "$cache_dir/$key.json";
            $today = date('Y-m-d');
            // запросы от сегодняшнего дня - не кэшировать
            $latest = str_in($rqs_dbg, $today) || $this->IsDynamicRequest($rqs_dbg) || $cache_for <= 0;  
            if (!$latest && file_exists($cache_file)) { // для отладки пока без ограничения времени жизни кэша, это заблокирует обновление для динамических URL!
                $res = file_get_contents($cache_file);
                $this->cache_file_last = $cache_file;
                $data = json_decode($res);                
                if (is_array($data) || is_object($data)) {
                    log_cmsg("\t#PERF_CACHE:~C00 data loaded from %s, size = %d", $cache_file, strlen($res));                
                    goto SKIP_DOWNLOAD;
                }
                else  {
                    log_cmsg("\t~C91 #WRONG_CACHE:~C00 bad/truncated data (decoded as %s) loaded from %s, size = %d, renamed", gettype($data), $cache_file, strlen($res));                
                    rename($cache_file, str_replace('.json', '.bad', $cache_file));
                }
            }

            $limiter = $this->Limiter();
            $avail = $limiter->Avail();
            $delay = 100000;

            if ($avail < 30) 
                $delay = 15 * 1000000 / ($avail + 1); // progressive delay in microseconds                               
            $limiter->Wait($delay, 'usefull_wait');                                 
            if (time() < $rest_allowed_ts) {
                $avail = 0;
                usefull_wait (3000000);
                return 'TEMPORARY_BAN(ratelimit: error) rest_allowed_ts = '.date(SQL_TIMESTAMP, $rest_allowed_ts);
            }

            $rq_start = pr_time();
            $this->last_api_request = $rqs;
            $res = curl_http_request($rqs);
            $this->last_api_headers = $curl_resp_header;
            $this->last_api_rqt = pr_time() - $rq_start;
            
            $this->total_requests ++;
            if (false !== strpos($res, 'ratelimit: error')) {
                $ban = random_int(300, 480);
                log_cmsg("~C91#WARN:~C00 for endpoint %s request ratelimit is reached, due BAN ops disabled for $ban seconds, total_requests = %d, last delay = %.1f sec...", $url, $this->total_requests, $delay / 1e6); 
                $rest_allowed_ts = time() + $ban;
                file_put_contents(REST_ALLOWED_FILE, $rest_allowed_ts); // expand for breaked script
                return $res;
            }
SKIP_DOWNLOAD:            
            $res = str_replace('},{', "},\n {", $res); // pretty formatting json
            if ($latest)  // latest data - no cache
                return $res;

            if (strlen($res) < 256)
                $cache_for = 3600; // small response, cache for 1 hour
            $this->last_requests [$t_now + $cache_for] = $rqs; // save as non-failed            
            $today = date('Y-m-d'); // за текущий день слишком много запросов
            $forward = str_in($rqs_dbg, 'reverse=false') || str_in($rqs_dbg, 'sort=1');
            if (!$latest || str_in($rqs_dbg, ':00:00') && $forward) {
                $res = str_replace('],[', "],\n[", $res); // pretty formatting json
                $res = str_replace('},{', "},\n{", $res); // pretty formatting json
                file_put_contents($cache_file, $res);
                $this->cache_file_last = $cache_file;
                $this->rqs_cache_map[$key] = $res; // пригодиться для не отлаженного перезапроса данных
            }
            return $res;
        }


        // using local DB engine: MySQL
        public function db_select_value(string $column, string $params = ''): mixed { 
            return sqli_df()->select_value($column, $this->table_name, $params);
        }
        public function db_select_row(string $column, string $params = '', int $mode = MYSQLI_OBJECT): mixed { 
            return sqli_df()->select_row($column, $this->table_name, $params, $mode);
        }
        public function db_select_rows(string $column, string $params = '', int $mode = MYSQLI_OBJECT): mixed { 
            return sqli_df()->select_rows($column, $this->table_name, $params, $mode);
        }

        public function db_first(bool $as_str = true, int $tp = 0) {            
            $info = $this->table_info;
            $ts = is_object($info) ? $info->min_time : sqli_df()->select_value('MIN(ts)', $this->table_name, "WHERE YEAR(ts) >= 2001 LIMIT 1  -- db_first");
            if (!is_string($ts)) return null;            
            $t = $tp < 3  ? strtotime($ts) : strtotime_ms($ts);
            if ($t < BEGIN_2001) return null;            
            return $this->db_timestamp($t, $as_str, $tp);
        }
        public function db_last(bool $as_str = true, int $tp = 0) {            
            $info = $this->table_info;
            $ts = is_object($info) ? $info->max_time : sqli_df()->select_value('MAX(ts)', $this->table_name, 'LIMIT 1  -- db_last');
            $t = $tp < 3  ? strtotime($ts) : strtotime_ms($ts);            
            if (0 == $t) return null;            
            return $this->db_timestamp($t, $as_str, $tp);
        }
        
        protected function db_timestamp(mixed $ts, bool $as_str = true, int $tp = 0): mixed {
            if ($ts === null || str_in($ts, '1970'))
                return 0;           

            if ($as_str) 
                return  is_string($ts) ? $ts : $this->TimeStampEncode($ts, $tp);            
            else
                return is_string($ts) ? $this->TimeStampDecode($ts, $tp) : $ts;
        }

        public   function  loaded_full() {                        
            $lb = $this->last_block;
            $elps = time() - $lb->max_avail;
            $data_ok = $elps < $this->normal_delay || $this->ws_sub;           
            $data_ok &= $this->nothing_to_download;
            return 0 == $this->BlocksCount() && $this->loaded_blocks > 0 && $data_ok;
        }                

        protected function CorrectTables() {
            global $mysqli, $mysqli_df, $chdb, $db_error;            
            $valid_engine = 'ReplacingMergeTree';
            
            $func = __FUNCTION__;

            $qlist = [];
            $table_name = $this->table_name;
            $this->table_create_code = null;          
            
            $hour = date('H');

            if (DEBUGGING) { // TODO: remove block from release            
                $query = "ALTER TABLE `download_history` ADD COLUMN IF NOT EXISTS `result` VARCHAR(32) NOT NULL AFTER `volume`";
                $mysqli->try_query($query);                 
                $query = sprintf("DROP TABLE iF EXISTS `%s.%s__%s`", DB_NAME, $this->data_name, $this->ticker);
                $mysqli_df->try_query($query);
                $query = sprintf("DROP TABLE iF EXISTS `__%s`", $this->ticker);
                $mysqli_df->try_query($query);
            }

            log_cmsg("~C97#CORRECT_TABLE:~C00 checking problems for `%s` ", $table_name);
            $table_code = null;

            if ($mysqli->table_exists($table_name)) {
                $this->table_create_code = $table_code = $mysqli->show_create_table($table_name);                
            }
            elseif (str_in($this->table_proto, 'mysql') )
                log_cmsg("~C31#WARN(func):~C00 on %s table %s not created yet from %s", $mysqli->server_info, $table_name, $this->table_proto);

            if ($mysqli_df->table_exists($table_name)) {
                $prop = $mysqli_df->is_clickhouse() ? 'table_create_code_ch' : 'table_create_code';
                $this->$prop = $table_code =  $mysqli_df->show_create_table($table_name);
                log_cmsg("~C92#TABLE_CODE:~C00 loaded for %s on %s, saved as %s.", $table_name, $mysqli_df->server_info, $prop);
            }
            elseif (!$chdb) {
                log_cmsg("~C31#WARN($func):~C00 on %s table %s not created yet, due last conn - can't continue", $mysqli_df->server_info, $table_name);
                return;
            }                             
            if ($table_code) 
                $this->partitioned = str_in($table_code, 'PARTITION BY');    


            if (is_object($chdb))
            try {                
                $chdb->database(DB_NAME);
                $tables = $chdb->showTables();                                                
                $short_name = str_replace(DB_NAME.'.', '', $this->table_name);
                $tables = array_column($tables, 'name');
                $tables = array_flip($tables);                
                if (!isset($tables[$short_name])) {
                    log_cmsg("~C91#INFO:~C00 table %s not exists in ClickHouse, all tables: %s ", $short_name, json_encode($tables));
                    return;
                }

                $res = $chdb->select("SHOW CREATE TABLE $table_name");
                $row = [];
                if (is_object($res))
                    $row = $res->fetchOne();                
                $table_code = is_array($row) ? strval($row['statement']) ?? '' : '';
                $this->table_create_code_ch = $table_code;                
                if (is_string($table_code) && 22 == $hour) 
                    $res = $chdb->write("OPTIMIZE TABLE $table_name FINAL");                    
                
                
            } catch (Throwable $e) {
                log_cmsg("~C91#EXCEPTION(CorrectTabless):~C00 %s, from: %s", $e->getMessage(), $e->getTraceAsString());
                return;                
            }                                              

            if (!str_in($table_code, 'CREATE')) {                     
                log_cmsg("~C91 #ERROR($func):~C00 failed retrieve table creating code for %s, error: %s", $table_name, $db_error);
                return;
            }           
            preg_match('/ENGINE = (\S+)/', $table_code, $match);
            $this->table_engine = $match[1] ?? '??';
            log_cmsg("~C93#TABLE_ENGINE:~C00 table `%s` engine is [%s]", $table_name, $this->table_engine);

            // здесь процесс пойдет, если mysqli_df - подключен к ClickHouse вместо chdb    
            if ($mysqli_df->is_clickhouse()) {
                $res = true;
                $this->QueryTableInfo($mysqli_df);

                if (20 == $hour)
                    $mysqli_df->try_query("CHECK TABLE $table_name");                                

                
                if (23 == $hour) {                    
                    $param = '';
                    $meta = this->table_info;
                    if (is_object($meta) && isset($meta->end_part)) 
                        $param .= " PARTITION '{$meta->end_part}'"; // in practice it's took too much time for 1B+ rows without PARTITION clause
                    $param .= $table_code && str_in($table_code, $valid_engine) ? 'FINAL' : '';                     
                    $res = $mysqli_df->try_query("OPTIMIZE TABLE $table_name $param DEDUPLICATE"); 
                    $this->table_need_recovery |= !$res && str_in($mysqli_df->error, 'Unknown codec family code');
                }
                
                if ($this->table_need_recovery)  {             
                    // TODO: here need using current prototype code for recreation or REPLACE TABLE
                    $dmg_name = "{$table_name}_dmg";
                    if ($mysqli_df->try_query("RENAME $table_name TO $dmg_name") &&
                        $mysqli_df->try_query("CREATE TABLE $table_name LIKE $dmg_name") &&
                        $res = $mysqli_df->try_query("INSERT INTO $table_name SELECT * FROM $dmg_name")) {                        
                        $mysqli_df->try_query("DROP TABLE $dmg_name");
                        log_cmsg("~C92#TABLE_RECOVERY:~C00 table `%s` recovered. Dropped damgaged old. Result: %s", $this->table_name, json_encode($res->info()));
                    } else {
                        log_cmsg("~C91#ERROR($func):~C00 need manual recovery table $table_name ");
                        $mysqli_df->try_query("RENAME $dmg_name TO $table_name");
                    }
                }                                
                
            }

            
DUMP_SQL:        
                       
            // log_cmsg("~C04~C97#REQUEST:~C00 %s", shell_exec("~/chdb $fname"));
        }


        public function CleanupBlock(DataBlock $block) { // possible override in child class
            global $chdb, $mysqli_df;

            if (0 == ($this->data_flags & DL_FLAG_REPLACE)) 
                return;

            if (1 == $this->days_per_block)
                $bounds = "Date(ts) = '{$block->key}'";
            else {
                $from_ts = $this->TimeStampEncode($block->lbound, 1);
                $to_ts =   $this->TimeStampEncode($this->time_precision == 1 ? $block->rbound : $block->rbound_ms );                                    
                $bounds = "(ts >= '$from_ts') AND (ts <= '$to_ts')";
            }

            $res = false;
            $query = "DELETE FROM {$this->table_name} WHERE $bounds";                                        
            $count = $mysqli_df->select_value("COUNT(*)", $this->table_name, "WHERE $bounds");
            if ($mysqli_df->table_exists($this->table_name))                     
                $res = $mysqli_df->try_query($query); // remove excess data                   
            
            $data_info = $res ? format_color("Removed %d rows from %s %s bounds [%s]", $count, $mysqli_df->server_info, $this->table_name, $bounds) : 
                                format_color("~C91Failed~C00  remove existing data: %s", $mysqli_df->error);                                    
            if ($chdb)
                try {
                    $stmt = $chdb->write($query);
                    if (!is_object($stmt) ||$stmt->isError()) 
                        $data_info .= format_color("~C91Failed~C00 query %s on ClickHouse", $query);
                    elseif (is_object($stmt))
                        $data_info .= ", from ClickHouse some rows removed";
                } 
                catch (Exception $e) {
                    log_cmsg("~C91#ERROR:~C00 query failed on ClickHouse: %s", $e->getMessage());
                }
            log_cmsg("~C04~C93 #BLOCK_CLEAN: ~C00  $data_info");    
            $block->Reset(); // min/max avail must be reseted   
            $block->db_need_clean = false;
        }

        public function CreateBlock(int $start, int $end): DataBlock {
            $block_class = $this->block_class;
            $block = new $block_class($this, $start, $end); // in real implementation is cache of ticks or candles
            $block->index = count($this->blocks);
            $this->blocks []= $block;
            $this->blocks_map[$block->key] = $block;
            return $block;
        }

        public function BlocksCount(): int {
            return count($this->blocks);
        }

   
        abstract public function FlushCache();

        /** HistoryFirst must return timestamp (seconds) of exchange first candle */
        abstract public function HistoryFirst(): bool|int;

        /**
         * Summary of InitBlocks - заполнение карты блоков перед выполнением цикла загрузки данных
         * @param int $start
         * @param int $end
         * @return void
         */
        protected function InitBlocks(int $start, int $end) {
            $this->blocks = [];
            $this->LinearFillBlocks($start, $end);            
            $this->initialized = true;
            $this->init_count ++;
            // WARN: избыточные блоки не фильтруются здесь!
        }

        protected function IsDynamicRequest(string $rqs): bool {
            $z_start = str_in($rqs, 'start=0');
            return str_in($rqs, 'reverse=true') && $z_start || str_in($rqs, 'sort=-1') && $z_start;                   
        }


        protected function LinearFillBlocks(int $start, int $end) {         
            $mult = SECONDS_PER_DAY;
            $last_day = ceil($end / $mult); // перекрывать надо весь день, даже если в нем единственная запись         
            $cursor = ($last_day + 1) * $mult;    // little overlap
            $range =  $this->days_per_block * $mult; // seconds in range    
            $start =  max($start, EXCHANGE_START_SEC);
            log_cmsg("~C93#SCAN_BLOCKS_LINEAR:~C00 from %s to %s ", color_ts($start), color_ts($end));   

            for ($i = 0; $i < $this->max_blocks; $i ++) {           
                $cursor -= $range;
                if ($cursor < $start) break;
                verify_timestamp($cursor);    
                $this->CreateBlock( $cursor, $end - 1);                
                $end = $cursor;                
            }
        }       

        public function LastBlock(): ?DataBlock {
            return $this->last_block;
        }

        public   function   LoadBlock(int $index): int { // only last data checks            
            global $verbose, $rest_allowed_t, $chdb;
            if (!$this->initialized && $index >= 0)
                throw new ErrorException("~C91#ERROR:~C00 LoadBlock(%d) called before InitBlocks()", $index);               

            $mysqli_df = sqli_df();    
            $today = date('Y-m-d'); // за текущий день слишком много запросов
            $this->last_error = '';
            $block = $this->last_block;            
            $data_name = $this->data_name;              
            $ticker = $this->ticker;

            $mgr = $this->get_manager();            
            $limit = $this->default_limit;
            
            if ($index >= 0)  // historical
                $block = $this->blocks[$index];
            else 
                goto SKIP_CHECKS;

            if (0 == count($block)) {
                $info = strval($block);                
                $this->block_load_history[$info] = ($this->block_load_history[$info] ?? 0) + 1;    
            }    
            
            verify_timestamp($block->lbound);    
               
            if ($block->attempts_bwd > 4) {
                $block->code = BLOCK_CODE::INVALID; // void block or error download
                $this->last_error = "WARN: block #$index is toxic, can't download";
                return 0;
            }    
                           
            if (BLOCK_CODE::INVALID == $block->code || BLOCK_CODE::FULL == $block->code) {                
                $this->last_error = format_color("~C91#ERROR:~C00 attempt to load block %d, with code %s ", $index, var_export($block->code, true));
                return 0; // already loaded or failed
            }
            
            $period = $block->rbound - $block->lbound;            
            $max_capacity = 86400 * $this->days_per_block; 
            if ($period >= $max_capacity) {                
                log_cmsg("~C31#WARN_FAT:~C00 block #%d is too large %s [%s..%s], %.1f hours, will be limited to one day. Generator probable buggy!", 
                            $this->ticker, format_TS( $block->lbound),  format_TS( $block->rbound), $index, $period / 3600);
                $block->set_rbound($block->lbound + $max_capacity - 1); // limit to
            }
            if ($today == $block->key && $block->index >= 0) {
                log_cmsg("~C91#ERROR:~C00 attempt to load last block %s, but index %d", $block->key, $block->index);
                $block->code = BLOCK_CODE::INVALID;
                return 0;   
            }            

            if ($block->rbound - $block->lbound > $this->days_per_block * SECONDS_PER_DAY) {
                $block->code = BLOCK_CODE::INVALID;
                log_cmsg("~C91 #ERROR:~C00 block range to invalid (too big), cleanup disabled: %s", strval($block));
                return 0;
            }
SKIP_CHECKS:            

            $ps = $block->db_need_clean ? 'RELOAD' : 'DOWNLOAD';
            if ($index >= 0)
                log_cmsg("~C93#BLOCK_{$ps}_CYCLE($ticker):~C00  %s ", strval($block));            

            $this->OnBeginDownload($block);

            if ($block->db_need_clean) {                    
                // remove for reliable overwrite
                $this->CleanupBlock($block);
            }


            $added = 0;                
            $t_now = time();
            $fresh_rqs = [];
            foreach ($this->last_requests as $rqt => $rqs)
                if ($t_now - $rqt <= 180)
                    $fresh_rqs [$rqt] = $rqs;

        
            $this->last_requests = $fresh_rqs;
            $limiter = $this->manager->rate_limiter;

            $block->OnUpdate();            
            $before_ms = $block->UnfilledBefore(); // in ms with little AHEAD forward
            $loop_start = time();            
            
            $lbound_ms = $block->lbound_ms; // in ms                                    
            $lag_right = $block->rbound - $block->max_avail;

            if ($index >= 0 && $before_ms <= $lbound_ms && $lag_right <= 300 )  {
                $this->last_error = sprintf('WARN: block #%d start %s is left block->lbound %s, right lag %d seconds, but not marked as full', $index,
                                                    $block->format_ts('min_avail'), $block->format_ts('lbound'), $lag_right);
                $block->code = BLOCK_CODE::FULL;                                            
                return 0;
            } 
           
            $rtm_min = time() - 60;
            $dstart = floor( time() / SECONDS_PER_DAY ) * SECONDS_PER_DAY; 
            $hstart = floor(time() / 3600) * 3600; 

               
            $cache = $this->cache;
            $cache->key = 'main';
            $cache->OnUpdate();
            
            $round_max = 60000; 
            $last_density = 0;
            try {
            // блок загружается справа - налево, заполняясь с хвоста
                while ($before_ms > $lbound_ms || $lag_right > 300) {
                    //  =============================================================================================                           
                    $now = time();
                    if ($block->loops >= 1500) {
                        $this->last_error = "loop count reach {$block->loops}, probably infinite loop";
                        break;
                    }                   

                    $minute = date('i') * 1;
                    if (!$mgr->active || $minute >= 58 ) {
                        $this->last_error = "script going to termination";
                        break;
                    }
                    
                    $reverse = true;
                    $heavy_data = $last_density >= $this->default_limit;

                    // если в предыдущую минуту было загружено данных с избытком, нужно небольшими шагами двигаться по истории внутри той-же минуты 
                    $after_ms  = $heavy_data ? $after_ms + 10000 : $block->UnfilledAfter();  
                    $before_ms = $heavy_data ? $before_ms - 10000 : $block->UnfilledBefore(); 

                    if ($before_ms < EXCHANGE_START) { // ещё миллион таких костылей добавлю, и заработает...
                        log_cmsg("~C91 #FINAL_BLOCK_DUMP:~C00 %s", json_encode($block, JSON_PRETTY_PRINT));
                        error_exit(" before_ms retrieved < EXCHANGE_START ".format_tms($before_ms));
                    }

                    // коррекция курсора, по данным в кэше
                    // && ($cache->Covers($block->min_avail) || $cache->Covers($block->max_avail))
                    if (count($cache) > 0 ) {   
                        // если очень много данных в каждой минутке, округление шага надо снижать. Вопрос в критерии                     
                        $round_step = $heavy_data ? 10000 : $round_max;                       
                        $oldest_ms = $cache->oldest_ms();
                        $newest_ms = $cache->newest_ms();                        
                        // вариант контролирующей оптимизации: отталкиваться от времени крайней записи в кэше (слева и справа соответственно). 
                        if ($heavy_data)  {                           
                            $pp = 100 * $last_density / $this->default_limit;
                            log_cmsg("\t~C33~C04#HEAVY_DATA:~C00 last density overlap step capacity for %.1f%%, using shift inside minute. Input cursors: %s .. %s ",
                                             $pp - 100, format_tms($before_ms), format_tms($after_ms)); 
                            $after_ms  = min($after_ms , $newest_ms);                                                                        
                            $before_ms = max($oldest_ms, $before_ms);                         
                        } else {
                            // перезапросы одних и тех-же данных из-за плохой вертикальной валидации (минутки и тики Bitfinex), замедляют процесс слишком сильно. Сдвиг окна исчисляется минутами, хотя загружаются всякий раз - часы
                            if ($cache->duplicates >= count($cache)) {
                                $after_ms  = max($after_ms, $newest_ms);
                                $before_ms = min($before_ms, $oldest_ms);
                            }
                            $after_ms  = floor($after_ms / $round_step ) * $round_step;
                            $before_ms = ceil($before_ms / $round_step ) * $round_step;                                    
                        }

                        if ($before_ms < EXCHANGE_START) {
                            log_cmsg(" ~C91#ERROR(LoadBlock):~C00  before_ms (%d) = oldest_ms (%d) | block->UnfilledBefore(%d) < EXCHANGE_START for block %s, from cache %s", 
                                        $before_ms,  $oldest_ms, $block->UnfilledBefore(),
                                        strval($block), strval($cache));                                                  
                            break;
                        }
                        $this->oldest_ms = min($this->oldest_ms, $oldest_ms);    
                    }                
                                        
                    $block_id = "$index:{$block->key}";          
                    $txt = 'LOAD';                              
                    
                    if ($block->index < 0) { // last block, need tail load                                                        
                        if ($now < $block->next_retry) break;
                        $reverse = false; // default forward scan

                        $block->next_retry = $now + 60; // not to frequiently...                        
                        if ($block->lbound < $dstart && 1 == $this->days_per_block) 
                            log_cmsg("~C91 #ERROR:~C00 last(tail) block have range [%s..%s]", 
                                        format_ts($block->lbound), format_ts($block->rbound));
                        // from hour start if initial
                        $tail = $block->max_avail > $block->lbound ? $block->max_avail : time() - 180;                                        
                        $tail = max($tail, $hstart);       
                        $after_ms = min ($after_ms, $hstart * 1000);
                        if ($block->max_avail <= $hstart && !$block->LoadedForward($after_ms)) {  // means no records in this hour, or initial download                                                               
                            $from_ts = date_ms('Y-m-d H:00:00', $after_ms);                             
                            $after_ms = $this->TimeStampDecode($from_ts, 3); 
                            log_cmsg("~C04~C93#TAIL_DOWNLOAD_FWD({$block->loops}):~C00 right block side after %s, next attempt planned at %s", color_ts($from_ts), color_ts($block->next_retry));
                        }
                        elseif ($block->loops <= 0 && !$block->LoadedBackward($before_ms, 30000)) {  
                            $reverse = true;
                            $before_ms = time_ms();                            
                            log_cmsg("~C04~C93#TAIL_DOWNLOAD_BWD({$block->loops}):~C00 right block side before now");
                        }          
                        else
                            log_cmsg("~C04~C97#TAIL_UPDATE:~C00 loading forward from %s", color_tms($after_ms));              
                    }
                    elseif ( $block->Covers_ms($after_ms) && $block->attempts_fwd < 3) { // $block->rbound - $block->max_avail > 300 &&                        
                        // выполнение этого кода очень желательно, чтобы история ложилась в БД последовательно хотя-бы внутри дня. Это оптимизирует доступ для волатильной истории
                        if ($block->LoadedForward($after_ms)) {
                            $block->attempts_fwd ++;
                            $txt = 'RELOAD';
                        }                        
                        else
                            $block->attempts_fwd = 0;
                        log_cmsg(" ~C36 >>> #BLOCK_TAIL_$txt:~C00 block %s filled %d before %s, requesting right completion, target volume %s",  
                                        $block_id, $block->filled, format_tms($after_ms), format_qty($block->target_volume));                        
                        $reverse = false;                        
                    }                                         
                    elseif ( $block->Covers_ms($before_ms) && $block->attempts_bwd < 3 && count($block) > 0) {  // $block->max_avail - $block->lbound > 300 &&
                        // загрузка справа налево, при нормальных данных этого не должно случаться
                        if ($block->LoadedBackward($before_ms)) {
                            $block->attempts_bwd ++;                        
                            $txt = 'RELOAD';
                        }
                        else
                            $block->attempts_bwd = 0;
                        $info = $block->min_avail > $block->lbound ? sprintf('filled %d up to %s ', $block->fills, color_ts($block->min_avail)) : 'empty';                    
                        log_cmsg(" ~C36 <<< #BLOCK_HEAD_$txt:~C00 block %s $info, requesting left completion before %s, target volume %s", 
                                    $block_id, color_ts($before_ms / 1000), format_qty($block->target_volume));
                    }                            
                    elseif (count($cache) > 0) {  // все варианты попробованы, остается пропуск
                        $data = [];
                        $this->cache_hit = true;                                        
                        $avail = count($block);
                        $stored = $cache->Store($block);                        
                        if ($stored > $avail) 
                            $this->OnCacheUpdate($block, $cache);                                                                             

                        $block->attempts_bwd ++;
                        $block->attempts_fwd ++;
                        log_cmsg("\t~C33#BLOCK_SKIP:~C00 source timepoints %s .. %s outbound block %s, covered %d / %d records; no download will help",
                                    strval($block), format_tms($before_ms), format_tms($after_ms), $stored, $avail);                                                    
                                    
                        goto PROCESS_DATA;
                    }

                    $rqs_avail = $limiter->Avail();         
                    if ($rqs_avail < 3) {
                        log_cmsg("~C91#WARN:~C00 rqs_avail = %d, breaking...", $rqs_avail);
                        break;
                    }
                    
                    set_time_limit(45);                                            
                    
                    if ($reverse && $after_ms > time_ms())
                        log_cmsg("~C31 #WARN:~C00 trying look to future after %s", format_tms($after_ms));

                    $t_from = $reverse ? $before_ms : $after_ms;    
                    verify_timestamp_ms($t_from, 't_from before load block '.strval($block));                    

                    if ($reverse)
                        $block->LoadedBackward($t_from, 0, true);
                    else
                        $block->LoadedForward($t_from, 0, true);                    

                    if (!$reverse && $t_from < $block->lbound_ms)
                        throw new Exception("OUTBOUND: selected time before block starts for forward load " . strval($block).': '.format_tms($t_from));
    

                    $method = $this->load_method;  // LoadCandles or LoadTicks  
                    $data = [];
                    if ($block->attempts_bwd + $block->attempts_fwd >= 10)
                        goto PROCESS_DATA; // not try more                

                    if (strlen($method) > 1 && method_exists($this, $method)) {                    
                        assert ($t_from > EXCHANGE_START, new Exception("FATAL: timestamp $t_from < EXCHANGE_START"));
                        $ts_from = $this->TimeStampEncode($t_from, 3);                     
                        $data = $this->$method ($block,  $ts_from, $reverse, $this->default_limit); // method params: $block, int $timestamp, bool $backward_scan, int $limit
                    }
                    else 
                        throw new ErrorException("FATAL: method $method not found in ".get_class($this));

                    if (time() < $rest_allowed_t) {
                        log_cmsg("\t~C31#WARN:~C00 breaking load block, due API temporary disabled");
                        break;     
                    }
                                       
                    
    PROCESS_DATA:                

                    $small_data = !is_array($data) || count($data) < $this->default_limit;
                    /*
                    if ($reverse)
                        $block->attempts_bwd += ( $this->cache_hit || $small_data ) ? 1 : 0;
                    else
                        $block->attempts_fwd += ( $this->cache_hit || $small_data ) ? 1 : 0;
                    //*/
                    $mini_cache = null;
                    $prev_count = count($block);
                    $prev_dups = $block->duplicates;
                    $imp = 0;                        
                    if (is_array($data) && count($data) > 0) {
                        $method = $this->import_method;                        
                        $prev_avail_min = $block->min_avail;
                        $prev_avail_max = $block->max_avail;                        
                        if (method_exists($this, $method)) 
                            $mini_cache = $this->$method ( $data, 'Rest-API', $index < 0);               
                        else
                            throw new ErrorException("FATAL: method $method not found in ".get_class($this));
                    }

                    $count_diff = 0;
                    $dups_diff = 0;
                    if (is_object($mini_cache)) { 
                        $imp = count($mini_cache);    
                        $added += $imp;                      
                        $last_density = $mini_cache->DataDensity();                      
                        $mini_cache->Store($this->cache);                         
                        $this->OnCacheUpdate($block, $cache); // раздача данных всем блокам, включая текущий                         
                        $count_diff = count($block) - $prev_count;
                        $dups_diff = $block->duplicates - $prev_dups;
                        if (BLOCK_CODE::FULL == $block->code)                             
                            break; // больше нечего тут делать, при обработке кэша все решилось                        

                        $cache_size = count($cache); // integraded with last                    
                        if (count($data) > $cache_size + 10 && $index >= 0) {
                            // it may be ignorable only rows
                            $first = $last = null;
                            if ($imp > 0) {
                                $first = array_key_first($data);
                                $last = array_key_last($data);
                                $first = $data[$first]; 
                                $last = $data[$last];
                            }                            
                            log_cmsg("~C91#WARN_LOSS:~C00 imported/saved only %d $data_name from %d, in cache now %d. First loaded: %s, imported: %s",
                                        $imp, count($data), $cache_size,
                                        json_encode($data[0] ?? $data),
                                        json_encode($first), json_encode($last) );                                
                        }
                        $oldest_ms = $mini_cache->oldest_ms();                         
                        $newest_ms = $mini_cache->newest_ms();
                        $key_first = $cache->firstKey();
                        $key_last = $cache->lastKey();                        

                        if ($block->recovery && 0 == count($block)) {  // по логике запроса таких блоков, хотя-бы одна запись должна быть загружена, иначе в его временном диапазоне вакуум                       
                            $first_keys = array_slice($mini_cache->keys, 0, 5);
                            log_cmsg("\t~C41~C97 #BLOCK_INVALID:~C40 ~C00 recovery request failed, no data covers %s, loaded data range: %s .. %s, first 5 keys imported: %s",
                                            strval($block), format_tms($oldest_ms), format_tms($newest_ms), json_encode($first_keys)); 
                            $dump = var_export($block, true);                                            
                            log_cmsg("\t~C31#BLOCK_DUMP:~C00 %s", substr($dump, 0, 200));                                            
                            // die("DEBUG BREAK\n");
                            $block->code = BLOCK_CODE::INVALID;
                            break;    
                        } elseif (0 == $count_diff && 0 == $dups_diff && $imp > 0 && $block->code !== BLOCK_CODE::FULL) {
                            log_cmsg("\t~C31#NO_UPDATES:~C00 %d downloaded records have timestamps out of block %s volume %s, records accumulated %d, key range [%s..%s]",
                                        $imp, strval($block), format_qty($block->SaldoVolume()), $prev_count,
                                        var_export($key_first, true), var_export($key_last, true));
                            if ($reverse)
                                $block->attempts_bwd ++;
                            else
                                $block->attempts_fwd ++;
                        }                    
                        elseif($count_diff > 0) {
                            // подразумевается, что в кэше не застревают данные последнего блока, вообще не появляются 
                            if ($block->min_avail < $prev_avail_min)                                             
                                log_cmsg("~C97\t#BLOCK_FILLS_LEFT:~C00 was %s now %s, +filled %d ", color_ts($block->min_avail), color_ts($prev_avail_min), $count_diff);                                                
                            if ($block->max_avail > $prev_avail_max)                                             
                                log_cmsg("~C97\t#BLOCK_FILLS_RIGHT:~C00 was %s now %s, +filled %d ", color_ts($block->max_avail), color_ts($prev_avail_max), $count_diff);                                                
                        }                        
                        
                        if ($before_ms < EXCHANGE_START || $oldest_ms < EXCHANGE_START)                              
                            log_cmsg("~C91 #ERROR:~C00 outbound oldest value select from  %s / %s / %s ",
                                    format_tms($this->oldest_ms), format_tms($before_ms), format_tms($oldest_ms));
                        
                        $before_ms = min($before_ms, $oldest_ms);                                   
                        $before_ms = max(EXCHANGE_START, $before_ms);                    
                        $this->oldest_ms = min($this->oldest_ms, $oldest_ms);                        


                        $cache_oldest = $cache->oldest_ms();
                        $cache_newest = $cache->newest_ms();                        
                        if ($block->Covered_by_ms($cache_oldest, $cache_newest) && BLOCK_CODE::FULL != $block->code && count($block) > 0)  {
                            $block->code = BLOCK_CODE::FULL; 
                            $block->info = "cache {$cache->key} covered block by time";
                        }

                        if ($block->loops >= 1500) {
                            $block->code = count($block) > 0 ? BLOCK_CODE::FULL : BLOCK_CODE::VOID; 
                            $block->info = "deadlock detected, loop count {$block->loops}";
                        }

                                                
                        $left_volume = $block->LeftToDownload();                         
                        if (count($block) > 0 && $block->IsEmpty()) 
                            $block->code = BLOCK_CODE::PARTIAL; //     

                        if (0 == $left_volume && $block->code && $imp < $this->default_limit && !$block->IsFull()) {
                            $block->code = BLOCK_CODE::FULL;
                            $block->info = 'target volume reached';
                        }
                                            
                        $this->newest_ms = max($this->newest_ms, $newest_ms); // last block update => upgrade latest available tick

                        $cache_info = 'invalid/void';
                        if ($cache_oldest <= $cache_newest)
                            $cache_info = format_color("%s..%s = %.1f min", color_ts($cache_oldest / 1000), color_ts($cache_newest / 1000), 
                                                                          ($cache_newest - $cache_oldest) / 60000);
                        

                        $pp = 100;
                        if ($block->target_volume > 0)
                            $pp = 100 - 100 * $left_volume / $block->target_volume;                        

                        log_cmsg("~C96\t\t#BLOCK_SUMMARY({$block->loops})~C00: %5d $data_name downloaded (%7d saldo, %7d in cache) with density %4.1f / minute, accumulated range [$cache_info], block stored range [%s..%s], left vol %-7s, progress %.1f%%", 
                                    count($data), $added, $cache_size, $last_density,
                                    color_ts( $block->min_avail),                                
                                    color_ts( $block->max_avail), 
                                    format_qty($left_volume), $pp);
                    }   // if mini_cache loaded 
                    elseif (is_array($data) ) {
                        if ($block->attempts_bwd > 2 && $block->attempts_fwd > 2) {
                            $block->code = 0 == count($block) ? BLOCK_CODE::VOID : BLOCK_CODE::FULL;
                            $block->info = 'data not received/imported several times';
                            log_cmsg("\t~C31#VOID_CACHE:~C00 block marked as completed, last_loaded %d, imported %d $data_name", count($data), count($this->cache));
                        }
                        else
                            log_cmsg("\t~C94#VOID_CACHE:~C00 ~C00 block %s, no data imported from %d records, attempts %d, last error: %s",
                                        $block_id, count($data), $block->attempts_bwd + $block->attempts_fwd, $this->last_error);                    
                    }

                    $block->loops ++;             
                    $cache->loops ++;                                            
                    
                    $mgr->ProcessWS(false); // обновления мимо кэша!
                    
                    $last_ts = '';
                    if ($block->max_avail >= $rtm_min || $index < 0)  {                 
                        $last_loaded = 0;                       
                        if (is_array($data) && $index < 0) {
                            $last_loaded = count($data);                        
                            $last_ts = format_ts($block->max_avail);
                        }
                        else    
                            $last_ts = format_ts( $block->max_avail);

                        $brk = (!is_array($data) || $last_loaded < $limit) ? ', breaking' : false; 
                        log_cmsg("\t~C94#LAST_BLOCK_SYNC: ~C00 latest avail %s (+%d s), loaded %d $data_name, rest blocks %d$brk ", 
                                color_ts($last_ts), time() - $block->max_avail, $last_loaded, $this->BlocksCount());
                        if ($brk) break;                           
                    } 

                    if (BLOCK_CODE::FULL == $block->code || BLOCK_CODE::VOID == $block->code) {                                                                
                        break;                                       
                    } 
                } // while
            } catch (Throwable $E) {
                log_cmsg("~C91#EXCEPTION(LoadBlock):~C00 %s at %s:%d, trace: %s", $E->getMessage(), $E->getFile(), $E->getLine(), $E->getTraceAsString());
                $block->code = BLOCK_CODE::INVALID;                
            }
            return intval($added);
        }

        public function LoadData(DataBlock $block, string $url, array $params, string $tss, bool $backward_scan): ?array {
            $dname = strtoupper($this->data_name);
            $m_cycles = $this->get_manager()->cycles;                                
            $index = $block->index;

            $rqs = "$url?".http_build_query($params);
            $rqs_dbg = urldecode($rqs);                       

            $tag = $block->index < 0 ? '~C40' : '~C04';
            if ($backward_scan)
                log_cmsg(" $tag~C33#{$dname}_DOWNLOAD($m_cycles/$index)~C00: from ~C04%s before = %s, attempts <<< %d", 
                                $rqs_dbg, color_ts($tss), $block->attempts_bwd);                       
            else
                log_cmsg(" $tag~C33#{$dname}_DOWNLOAD($m_cycles/$index)~C00: from ~C04%s after = %s, attempts >>> %d", 
                                $rqs_dbg, color_ts($tss), $block->attempts_fwd);
                                
            // TODO: ttl config table                                
            $cache_ttl = 180;
            if (-3 == $index)
                $cache_ttl = 10;
            elseif (-1 == $index)
                $cache_ttl = 0;
                                
            // request API with new URI                          
            $t_start = pr_time(); 
            $res = $this->api_request($url, $params, $cache_ttl);  
            $elps = pr_time() - $t_start;
            if ($elps > 10)
                log_cmsg("~C31 #PERF_WARN:~C00 API request time %.1f sec, retrieved %d length string", $elps, strlen($res));
            
            $block->last_api_request = $this->last_api_request;
            $data = json_decode($res, false);            
            if (false !== strpos($res, 'error') || !is_array($data))  {
                log_cmsg("~C91#WARN: API request failed with response: %s %s", gettype($data), substr($res, 0, 200)); // typical is ratelimit errro
                return null;
            }      

            if ($block->index >= 0 && strlen($this->cache_file_last) > 0)
                $block->cache_files []= $this->cache_file_last; // this file can be deleted after whole block is loaded

            if (is_array($data) && $backward_scan)
                $data = array_reverse($data); // API returns data in reverse order, normalizing  
            elseif (!is_array($data))     
                log_cmsg("~C91#REQUEST_FAILED:~C00 %s %s", gettype($data), substr($res, 0, 100));
            

            $this->rest_time_last = time();                        
            return $data;
        }


        public function QueryTableInfo(mixed $conn): ?stdClass {            
            if (!$this->partitioned) return null;

            if (is_object($conn) && $conn instanceof mysqli_ex && $conn->is_clickhouse()) {
                $db_name = $conn->active_db();
                $this->table_info = $conn->select_row('MIN(partition) as start_part, MAX(partition) as end_part, MIN(min_time) AS min_time, MAX(max_time) AS max_time, SUM(rows) as size', 'system.parts',
                                                "WHERE (table = '$this->table_name') AND (active = 1) AND (rows > 0) AND (database = '$db_name')", MYSQLI_OBJECT);   
            }
            elseif  (is_object($conn) && $conn instanceof mysqli_ex && !$conn->is_clickhouse()) {
                $db_name = $conn->active_db();
                $rows = $conn->select_rows('PARTITION_NAME as `name`, TABLE_ROWS as `rows`', 'INFORMATION_SCHEMA.PARTITIONS', "WHERE (TABLE_SCHEMA = '$db_name') AND (TABLE_NAME = '$this->table_name') AND (TABLE_ROWS > 0)", MYSQLI_ASSOC);
                if (is_array($rows) && count($rows) > 0) {
                    $this->table_info = $info = new stdClass();
                    $info->start_part = $rows[0]['name'];
                    $info->end_part = $rows[count($rows) - 1]['name'];
                    $info->min_time = $conn->select_value("MIN(ts)", $this->table_name, " PARTITION ({$info->start_part}) LIMIT 1  -- QTI");
                    $info->max_time = $conn->select_value("MAX(ts)", $this->table_name, " PARTITION ({$info->end_part}) LIMIT 1  -- QTI");                    
                    $info->size = 0;
                    foreach ($rows as $row) 
                        $info->size += $row['rows'];                    
                }
                                                
            }
            // TODO: need also implementation for chdb 
            return $this->table_info;                                               
        } 
        public function QueryTimeRange(): array { // request lowes and highest timestamps            
            $mysqli_df = sqli_df();
            $range = [false, false];                 
            $this->QueryTableInfo($mysqli_df);

            $init = $this->saved_max <= $this->saved_min;                                  
            $min = $this->db_first();
            $max = $this->db_last();                           
            
            $min_t = strtotime($min);
            $max_t = strtotime($max);
            $coef = 3 == $this->time_precision ? 1000 : 1;
            if ($min_t > EXCHANGE_START_SEC && $max_t >= $min_t) {
                if ($this->get_manager()->cycles < 10 && $init)
                    log_cmsg("~C94#INFO_QTR:~C00 data range in DB: from [%s] to [%s] for %s", $min,  $max, $this->ticker);
                $this->saved_min = $min_t;
                $this->saved_max = $max_t;
                $range = [$min, $max];
            } else {
                $this->saved_min = 0;
                $this->saved_max = time() - 60;
                             
                log_cmsg(" #INFO: DB returns null, used range from 0 to now");
            }
            $this->saved_min *= $coef;
            $this->saved_max *= $coef;
            return $range;
        }


        // функции конверторов с проверкой, возможно медленные
        public function  TimeStampDecode(string $ts, int $tp = 0) {  
            if (!$tp) $tp = $this->time_precision;
            if ($tp == 1)                 
                return verify_timestamp(strtotime($ts), 'TimeStampEncode');
            if ($tp == 3)           
                return verify_timestamp_ms(strtotime_ms($ts), 'TimeStampEncode');
            return false;
        }
        public function TimeStampEncode(int $t, int $tp = 0): string {
            if (!$tp) $tp = $this->time_precision;

            if ($tp == 3)           
                return date_ms(SQL_TIMESTAMP3, verify_timestamp_ms($t, 'TimeStampEncode'));
            return date(SQL_TIMESTAMP, verify_timestamp($t, 'TimeStampEncode')); // default
        }        

        public function PrepareDownload() {
            
            $limit_past = strtotime(HISTORY_MIN_TS);
            if (!$this->table_corrected)
                $this->CorrectTables();

            
            
            $start = $this->HistoryFirst(); // method should cache return value in this->history_first                    
            
            $start = floor($start); // if float => seconds
            if (!is_int($start))
                 $start = EXCHANGE_START_SEC;  // using abstract history start

            if (!$this->initialized) {
                $this->QueryTimeRange();
                $start = max($start, $limit_past);
            }
            $this->history_first = $start;          
            $end = $this->db_first(false, 1); // in seconds!
            if (!is_int($end)) {
                log_cmsg("~C107~C00 #HISTORY_VOID: ~C00~C40 required full history download for %s", $this->ticker);
                $end = time() - 60;
            }
            if ($end < EXCHANGE_START_SEC) {
                log_cmsg("~C91#WARN:~C00 db_first(%s) end time %s in DB < EXCHANGE_START_SEC", $this->ticker, format_ts($end));
                $end = EXCHANGE_START_SEC;
            }
            
            if ($end <= $start)  {                
                $end = time(); // in seconds                 
            }

            if ($start >= $end) {
                log_cmsg("~C91 #ERROR(PrepareDownload): for {$this->symbol} assumed invalid scan range: start %s > end %s:",            
                                    format_ts($start), format_ts($end));
                $end = floor_to_day(time()) + SECONDS_PER_DAY - 60;                                     
            }

            $month_ago = format_ts(time() - 30 * SECONDS_PER_DAY);
            // журнал скачивания предупреждает частые повторные попытки, если данные "не бьются" и считаются неполными. Остается некоторый шанс, что биржи по запросам на тикеты восстановят данные... 
            // так что очистка в следующей строке, позволит через месяц заново запросить данные
            sqli()->try_query("DELETE FROM `download_history` WHERE (`ticker` = '{$this->ticker}') AND (`ts` < '$month_ago')");

            $blocks_count = $this->BlocksCount();
            if (0 == $blocks_count && $this->data_flags & DL_FLAG_HISTORY) 
                $this->InitBlocks($start, $end); // here can also defined last_block
            elseif ($blocks_count > 10)
                log_cmsg("~C33#PREPARE_DL:~C00 already have %d blocks, scaning disabled", $blocks_count);
            
            $blocks_count = $this->BlocksCount();
            if ($blocks_count > 0) { 
                $start_tss = gmdate(SQL_TIMESTAMP, $start);
                $end_tss = gmdate(SQL_TIMESTAMP, $end);
                log_cmsg("~C97#MAPPING:~C00 performed for %s, produced %d blocks, from %s to %s", $this->ticker, $blocks_count, $start_tss, $end_tss);
            }            
            else {
                $this->head_loaded = true;               
                $this->nothing_to_download = $this->zero_scans > 0;
                if ($this->loaded_full() && !$this->initialized)
                    log_cmsg("~C97#HISTORY_FULL:~C00 nothing planned for download/recovery");
            }

            $last = $this->db_last(false, 3); // in seconds
            if (0 == $this->newest_ms && $last !== null) 
                $this->newest_ms = $last;
            $this->initialized = true;
            $this->init_time_last = time();
        }

        public function RestDownload(): int {
            global $verbose, $rest_allowed_t;
            if ($this->rest_errors > 100)  return -1;                     
                          
            $this->cycle_break = false;
            $load_history = ($this->data_flags & DL_FLAG_HISTORY) > 0;
            $n_blocks = count($this->blocks);
            $mgr = $this->get_manager();
            $minute = date('i');            
            if (0 == $n_blocks) {                
                if (0 == $this->loaded_blocks && 0 == $this->zero_scans && $mgr->cycles < 10 && $load_history)
                    log_cmsg("~C34#REST_DOWNLOAD:~C00 too small blocks to download for %s, scaning...", $this->ticker);                    
                $this->PrepareDownload();
                $n_blocks = count($this->blocks);                             
            }
            
            $limiter = $mgr->rate_limiter;
            // TODO: non busy update, add web-socket status checking
            $allow_upd = $limiter->Avail() > 25;
            $allow_upd |= $this->Elapsed() > 600;       


            $uptime = $mgr->Uptime();
            $ws_elps = time() - $this->ws_time_last;

            if ($uptime > 240 && $ws_elps < 60)
                $allow_upd = false; // no need to update last block if WebSocket is alive
            if (time() - $this->last_block->last_load < 60 || $this->ws_sub && 0 != $this->loaded_blocks)
                $allow_upd = false;

            $elps = $this->Elapsed();    
            $total_loads = 0;
            if ( $elps > $this->normal_delay && $allow_upd) {    
                $elps = round($elps);                 
                if ($elps > 7 * SECONDS_PER_DAY) 
                    $ets = 'week+ (expired?)';
                elseif ($elps < SECONDS_PER_DAY)
                    $ets = sprintf("%.3f hours", $elps / 3600);
                else
                    $ets = sprintf("%.4f days", $elps / SECONDS_PER_DAY);

                $wts = sprintf('%.1f~C00 min', $ws_elps / 60);
                if ($ws_elps > SECONDS_PER_DAY)
                    $wts = sprintf('never updated');                    
                if ($ws_elps > 600 && $mgr->cycles < 10)    
                    log_cmsg("~C94#TAIL_BLOCK_UPDATE:~C00 for %s, elapsed time = %s, uptime = %1.f min, ws_elps = %s ", 
                                    $this->symbol, $ets, $uptime / 60, $wts);                
                $total_loads += $this->LoadBlock(-1);
                if (0 == $n_blocks)
                    $this->loaded_blocks = max ($this->loaded_blocks, 1);                                
            }   


            if ($mgr->cycles < 2 || !$load_history) return $total_loads;
                    
            if ($n_blocks > 0) {
                usort($this->blocks, 'compare_blocks'); // descending by timestamp (lbound)
                $tmp = $mgr->tmp_dir;
                $class = get_class($this);
                $dump = json_encode($this->blocks);
                $dump = str_ireplace('},{', "},\n {", $dump);
                file_put_contents("$tmp/$class-{$this->symbol}_blocks.json", $dump);
            } 
            else                 
                return 0;
            


            $index = $n_blocks - 1;
            $removed = 0;
            
            $t_start = time();
            $code_map = [0, 0, 0, 0];
            $not_loaded = [0];
            $failed = 0;
            $minute = date('i');
            $big_loads = 0;                  
            $total_loops = 0;
            $this->last_error = '';            

            if (count($this->cache) > 0) {                
                $this->FlushCache();
            }

            $prev_day = $this->cache->firstKey();

            // download history by mapping
            while ($index >= 0 && $big_loads < $this->blocks_at_once && $mgr->active && !$this->cycle_break) {
                usefull_wait(100000);
                $block = $this->blocks[$index];
                $key = intval($block->code->value);                                
                if ($key < 0 || $block->index < 0) {
                    $index --;
                    continue;
                }
                $block->index = $index;                
                if ( $prev_day > 0 && abs($prev_day - $block->lbound) > SECONDS_PER_DAY  && count($this->cache) > 0) {
                    log_cmsg("~C94 #JUMP:~C00 cache flushing due multi-day gap from %s to %s ",
                                color_ts($prev_day), color_ts($block->lbound));
                    $this->FlushCache();
                }
                $prev_day = $block->lbound;
                if ($block == $this->last_block)
                    throw new Exception("Last block available with index $index");       
                
                if (time() < $rest_allowed_t) {
                    log_cmsg("~C91#WARN:~C00 breaking loop, due REST API temporary disabled");
                    break;
                }

                if (BLOCK_CODE::NOT_LOADED == $block->code || BLOCK_CODE::PARTIAL == $block->code) {
                    $res = $this->LoadBlock($index);
                    $key = $block->code->value;                             
                    $total_loops += $block->loops;
                    $total_loads += $res;
                    
                    if ($res >= 500)                         
                        $big_loads ++; // historical loads                                        
                    if (0 == $res && BLOCK_CODE::FULL != $block->code) {
                        if (strlen($this->last_error) > 1) {
                            $ke = $this->last_error;
                            $not_loaded [$ke]= ($not_loaded [$ke] ?? 0) + 1; // not loaded blocks
                        }   
                        $failed ++;
                    }
                }  
                else                       
                    $code_map[$key] ++;                    
                
                $index --;        
                $elps = time() - $t_start;
                if ($elps >= $this->cycle_time * 0.85 || str_in($this->last_error, 'timeouted')) {
                    $this->cycle_break = true;
                    break;
                }
                if ($minute >= 58) break; // last minutes = minimal work
            } // while - loading blocks one by one

            $blocks = array_reverse($this->blocks);

            $cache_size = count($this->cache);
            if ($cache_size > 0) {                
                $this->FlushCache();
            }

            foreach ($blocks as $block) 
                if ($block->Finished()) {
                    $this->OnBlockComplete($block);    
                    unset($this->blocks[$block->index]); 
                    unset($this->blocks_map[$block->key]);
                    $removed ++;                
                }
            
            if ($removed > 0)
                log_cmsg("~C97#COMPLETE:~C00 While download %d blocks removed for %s, codes: %s", $removed, $this->ticker, json_encode($code_map));

            $errs = implode("\n\t", array_keys($not_loaded));    
            if ($failed > 0 && str_in($errs, 'ERROR'))
                log_cmsg("~C31#WARN_DL_FAILED:~C00 While download %d blocks in %d loops failed for %s, errors: %s", 
                            $failed, $total_loops, $this->ticker, $errs);

            return $big_loads;
        } // RestDownload


        protected function OnBeginDownload(DataBlock $block) {

        }
        abstract public function OnBlockComplete(DataBlock $block);
        abstract protected function OnCacheUpdate(DataBlock $default, DataBlock $cache);
        
    }
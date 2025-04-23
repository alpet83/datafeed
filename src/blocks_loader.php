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

        public     int $cycle_time = 30;   // max time for REST Download cycle
        public     int $max_blocks = 30;  // how much blocks to load in one session

        public      $head_loaded = false;
        public      $tail_loaded = false;

        protected   $initialized = false;
        
        public      int $loaded_blocks = 0;
        protected   $import_method = '';  // converter from API format to common
        protected   $load_method = '';  // REST loader for kind of data
        protected   $data_name = '';  // for logging

        protected   $table_create_code = ''; // for MySQL loaded by CorrectTable 
        protected   $table_create_code_ch = ''; // for ClickHouse loaded by CorrectTable
        protected   $table_need_recovery = false;

        protected   $total_requests = 0;
        

        protected    $cache = null; // cache object, descendant from DataBlock                
        
        protected   $rqs_cache_map = []; // [md5(rqs)] = API response
        protected   $cache_hit = false;
        protected   $last_api_request = ''; // last API request by api_request function
 
        protected   $last_requests = []; // cache for API requests
                
        protected   $last_error = '';

        protected   $block_load_history = []; // для предотвращения многократных загрузок, одного и того-же, по любым причинам

        protected   $nothing_to_download = false; // выставляется, если после сканирования блоков, все оказались заполненны

        protected  $partitioned = true;


        public function __construct(DownloadManager $mngr, \stdClass $ti) {
            $this->oldest_ms = time_ms();
            parent::__construct($mngr, $ti);
            $this->table_name = $mngr->TableName("{$this->data_name}__{$this->ticker}");
            $this->tables['data'] = $this->table_name;
            $day_start = floor_to_day(time());  
            $class = $this->block_class;                      
            $this->first_block = new $class($this, EXCHANGE_START_SEC,  $day_start - 1); // HistoryFirst must adjust block bounds later                                   
            $this->first_block->index -2;
            $this->last_block = new $class($this, $day_start, END_OF_TODAY);            
            if (0 === stripos($this->symbol, 'BTCUSD') || 0 === stripos($this->symbol, 'ETHUSD'))
                $this->normal_delay = 30;             
        }


        protected function api_request(string $url, array $params, int $cache_for = 180): string { // REST API request with typical GET params.
            global $rest_allowed_ts;
            $res = 'fail?'; 
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
                    log_cmsg("~C31 #CACHE_HIT~C00: requesting data was already performed before (expired at %s). By key %s returns from cached %s ",
                                $last_tss, $key,  $info);               
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
                log_cmsg("#PERF_CACHE:~C00 data loaded from %s, size = %d", $cache_file, strlen($res));
                goto SKIP_DOWNLOAD;
            }

            $limiter = $this->manager->rate_limiter;
            $avail = $limiter->Avail();
            $delay = 15 * 1000000 / ($avail + 1); // progressive delay in microseconds

            $limiter->Wait($delay, 'usefull_wait');                     
            if (time() < $rest_allowed_ts) {
                $avail = 0;
                usefull_wait (3000000);
                return 'TEMPORARY_BAN(ratelimit: error) rest_allowed_ts = '.date(SQL_TIMESTAMP, $rest_allowed_ts);
            }

            $this->last_api_request = $rqs;
            $res = curl_http_request($rqs);
            
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
                file_put_contents($cache_file, $res);
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
            $ts = sqli_df()->select_value('MIN(ts)', $this->table_name);
            if ($ts === null)  
                return 0;

            if ($as_str || !is_string($ts))
                return $ts;
            else
                return $this->TimeStampDecode($ts, $tp);
        }
        public function db_last(bool $as_str = true, int $tp = 0) {            
            $ts = sqli_df()->select_value('MAX(ts)', $this->table_name);
            if ($ts === null)
                return 0;            

            if ($as_str || !is_string($ts))
                return $ts;
            else
                return $this->TimeStampDecode($ts, $tp);
        }
        

        public   function  loaded_full() {                        
            $lb = $this->last_block;
            $elps = time() - $lb->max_avail;
            $data_ok = $elps < $this->normal_delay || $this->ws_sub;           
            $data_ok &= $this->nothing_to_download;
            return 0 == $this->BlocksCount() && $this->loaded_blocks > 0 && $data_ok;
        }                

        protected function CorrectTable() {
            global $mysqli, $mysqli_df, $chdb;            
            $valid_engine = 'ReplacingMergeTree';
            $bad_engine = 'MergeTree';

            $qlist = [];
            $table_name = $this->table_name;
            $temporary = "{$table_name}__src";            
            
            log_cmsg("~C97#CORRECT_TABLE:~C00 checking problems for %s ", $table_name);

            $this->table_create_code = $mysqli->show_create_table($table_name);                         
            if ($this->table_create_code) 
                $this->partitioned = str_in($this->table_create_code, 'PARTITION BY');
            else
                log_cmsg("~C31#WARN(MySQL):~C00 table %s not created yet", $table_name);

            $qlist []= "SELECT COUNT(*) AS count FROM $table_name";
            $qlist []= "RENAME TABLE $table_name TO $temporary";
            $qlist []= "table_proto, replicated_deduplication_window=1";
            $qlist []= "INSERT INTO $table_name SELECT * FROM $temporary";            
            $qlist []= "SELECT COUNT(*) AS count FROM $table_name FINAL";
            $qlist []= "DROP TABLE $temporary";
            $qlist []= "OPTIMIZE TABLE $table_name FINAL";

            
            if (is_object($chdb))
            try {
                log_cmsg("~C93#INFO:~C00 detected native ClickHouse connection...");
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
                if (!is_string($table_code) || !str_in ($table_code, 'ENGINE = MergeTree')) {
                    // log_cmsg("~C97#INFO:~C00 table proto: %s", $table_proto);
                    $res = $chdb->select("SELECT COUNT(*) AS count FROM $table_name FINAL");
                    log_cmsg("~C93 #SIZE:~C00 %d", $res->fetchOne()['count']);                    
                    $res = $chdb->write("OPTIMIZE TABLE $table_name FINAL");
                    if (is_object($res))
                        log_cmsg("~C93 #OPTIMIZE:~C00 %s", json_encode($res->info()));
                    return;
                }
                goto DUMP_SQL;                
            } catch (Throwable $e) {
                log_cmsg("~C91#EXCEPTION(CorrectTable):~C00 %s, from: %s", $e->getMessage(), $e->getTraceAsString());
                return;                
            }                                              

            $table_code = $this->table_create_code;        
            // здесь процесс пойдет, если mysqli_df - подключен к ClickHouse вместо chdb    
            if (str_in($table_code, 'CREATE')) {
                $res = true;
                $mysqli_df->try_query("CHECK TABLE $table_name");                
                $param = $table_code && str_in($table_code, $valid_engine) ? 'FINAL' : '';                                                
                $res = $mysqli_df->try_query("OPTIMIZE TABLE $table_name $param");                

                $this->table_need_recovery |= !$res && str_in($mysqli_df->error, 'Unknown codec family code');
                
                if ($this->table_need_recovery)  {                    
                    $dmg_name = "{$table_name}_dmg";
                    if ($mysqli_df->try_query("RENAME $table_name TO $dmg_name") &&
                        $mysqli_df->try_query("CREATE TABLE $table_name LIKE $dmg_name") &&
                        $res = $mysqli_df->try_query("INSERT INTO $table_name SELECT * FROM $dmg_name")) {                        
                        $mysqli_df->try_query("DROP TABLE $dmg_name");
                        log_cmsg("~C92#TABLE_RECOVERY:~C00 table `%s` recovered. Dropped damgaged old. Result: %s", $this->table_name, json_encode($res->info()));
                    } else {
                        log_cmsg("~C91#ERROR:~C00 need manual recovery table $table_name ");
                        $mysqli_df->try_query("RENAME $dmg_name TO $table_name");
                    }
                }                                
                
            }
            else {
                log_cmsg("~C91 #ERROR:~C00 failed retrieve table prototype for %s", $table_name);
                return;
            }

            if ($table_code)
                log_cmsg("~C93#TABLE_PROTO_DUMP:~C00 %s", $table_code);                           
            if (!str_in ($table_code, "ENGINE = $bad_engine")) return;                        
DUMP_SQL:        
            
            $table_code = str_replace("ENGINE = $bad_engine", "ENGINE = $valid_engine", $table_code);
            $fname = $this->manager->tmp_dir."/correct_{$this->symbol}.sql"; 
            file_put_contents($fname, "--------------- \n");
            $results = [];
            try {
                foreach ($qlist as $q) {
                    $query = str_replace('table_proto', $table_code, $q);
                    log_cmsg("~C33 #QUERY:~C00 %s", $query);
                    file_add_contents($fname, "$query;\n");                    
                    if ($chdb) {
                        if (0 === strpos($query, 'SELECT'))
                            $results []= $chdb->select($query);
                        else
                            $results []= $chdb->write($query);
                    }
                    else
                        $mysqli_df->try_query($query);
                }    
            } catch (Throwable $e) {
                log_cmsg("~C91#EXCEPTION(CorrectTable):~C00 %s, from: %s", $e->getMessage(), $e->getTraceAsString());                
            }                        
            foreach ($results as $id => $res) {
                if (!is_object($res)) continue;
                log_cmsg("~C93 #CH_QUERY_RESULT($id):~C00 %s", json_encode($res->info()));
            } //*/
            // log_cmsg("~C04~C97#REQUEST:~C00 %s", shell_exec("~/chdb $fname"));
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
            global $verbose, $rest_allowed_t;
            if (!$this->initialized && $index >= 0)
                throw new ErrorException("~C91#ERROR:~C00 LoadBlock(%d) called before InitBlocks()", $index);               

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
SKIP_CHECKS:            
            $added = 0;                

            $this->loops = 0;              
            $t_now = time();
            $fresh_rqs = [];
            foreach ($this->last_requests as $rqt => $rqs)
                if ($t_now - $rqt <= 180)
                    $fresh_rqs [$rqt] = $rqs;

        
            $this->last_requests = $fresh_rqs;
            $limiter = $this->manager->rate_limiter;

            $block->OnUpdate();            
            $before_ms = $block->UnfilledBefore(); // in ms

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

            if ($index >= 0)
                log_cmsg("~C93#BLOCK_DOWNLOAD_CYCLE($ticker):~C00  %s", strval($block));
            $cache = $this->cache;
            $cache->OnUpdate();

            try {
            // блок загружается справа - налево, заполняясь с хвоста
                while ($before_ms > $lbound_ms || $lag_right > 300) {
                    //  =============================================================================================       
                    $now = time();
                    $elps = $now - $loop_start;
                    $cycle_elps = $now - $mgr->cycle_start;
                    if ($elps > $this->cycle_time * 0.75
                        || $this->loops > 100 
                        || $cycle_elps >= $this->cycle_time) {
                        $this->last_error = sprintf('timeouted elps = %d, cycle_elps %d, loops = %d, ', $elps, $cycle_elps, $this->loops);
                        break; // не занимать все время общего цикла
                    }
                    
                    $reverse = true;
                    // коррекция курсора, по данным в кэше
                    if (count($cache) > 0) {
                        $oldest_ms = $cache->oldest_ms();
                        $newest_ms = $cache->newest_ms();                        
                        $before_ms = min($before_ms, $oldest_ms, $block->UnfilledBefore()); // in ms                                        
                        if ($before_ms < EXCHANGE_START) {
                            log_cmsg("~C91#ERROR(LoadBlock):~C00  before_ms (%d) = oldest_ms (%d) | block->UnfilledBefore(%d) < EXCHANGE_START for block %s, from cache %s", 
                                            $before_ms,  $oldest_ms, $block->UnfilledBefore(),
                                            strval($block), strval($cache));                                                  
                            break;
                        }
                        $this->oldest_ms = min($this->oldest_ms, $oldest_ms);    
                    }                    
                    

                    $after_ms = 0;    
                    $block_id = "$index:{$block->key}";

                    
                    $after_ms = $block->UnfilledAfter(); 
                    
                    if ($index < 0) { // last block, need tail load                                                        
                        if ($now < $block->next_retry) break;
                        $block->next_retry = $now + 60; // not to frequiently...                        
                        if ($block->lbound < $dstart && 1 == $this->days_per_block) 
                            log_cmsg("~C91 #ERROR:~C00 last(tail) block have range [%s..%s]", 
                                        format_ts($block->lbound), format_ts($block->rbound));
                        // from hour start if initial
                        $tail = $block->max_avail > $block->lbound ? $block->max_avail : time() - 180;                                        
                        $tail = max($tail, $hstart);      
                        if ($block->max_avail <= $hstart && $block->newest_ms() != $block->last_fwd) {  // means no records in this hour, or initial download                             
                            $reverse = false;         
                            $from_ts = date_ms('Y-m-d H:00:00', $after_ms);                             
                            $after_ms = $this->TimeStampDecode($from_ts, 3); 
                            log_cmsg("~C04~C93#TAIL_DOWNLOAD({$this->loops}):~C00 right block side after %s, next attempt planned at %s", color_ts($from_ts), color_ts($block->next_retry));
                        }
                        elseif ($this->loops <= 0 && $before_ms != $block->last_bwd) {  
                            $before_ms = time_ms();                            
                            log_cmsg("~C04~C93#TAIL_DOWNLOAD({$this->loops}):~C00 right block side before now");
                        }                        
                    }                     
                    elseif ($before_ms != $block->last_bwd && $before_ms > $lbound_ms) {  // $block->max_avail - $block->lbound > 300 &&
                        // самый базовый сценарий загрузки исторических блоков
                        $info = $block->min_avail > $block->lbound ? sprintf('filled %d up to %s ', $block->fills, color_ts($block->min_avail)) : 'empty';                    
                        log_cmsg("~C34#BLOCK_HEAD_LOAD:~C00 block %s $info, requesting left completion before %s, target volume %s", 
                                    $block_id, color_ts($before_ms / 1000), format_qty($block->target_volume));
                    }
                    elseif ( $after_ms != $block->last_fwd && $block->max_avail < $block->rbound) { // $block->rbound - $block->max_avail > 300 &&
                        log_cmsg("~C36#BLOCK_TAIL_LOAD:~C00 block %s filled %d before %s, requesting right completion, target volume %s",  
                                    $block_id, $block->filled, format_tms($after_ms), format_qty($block->target_volume));                        
                        $reverse = false;                        
                    }                
                    else {  // все варианты попробованы, остается пропуск
                        $data = [];
                        if ( 0 == ($block->attempts_bwd + $block->attempts_fwd)) 
                            log_cmsg("~C33#BLOCK_SKIP:~C00 no additional records received, tested before %s after %s ", format_ts($block->last_bwd), format_ts($block->last_fwd));
                        $this->cache_hit = true;
                        $block->code = $block->fills > 0 ? BLOCK_CODE::FULL : BLOCK_CODE::VOID;
                        goto PROCESS_DATA;
                    }

                    $rqs_avail = $limiter->Avail();         
                    if ($rqs_avail < 3) {
                        log_cmsg("~C91#WARN:~C00 rqs_avail = %d, breaking...", $rqs_avail);
                        break;
                    }
                    set_time_limit(45);                                            

                    $t_from = $reverse ? $before_ms : $after_ms;    
                    verify_timestamp_ms($t_from, 't_from before load block '.strval($block));                    

                    if ($reverse)
                        $block->last_bwd = $t_from;
                    else
                        $block->last_fwd = $t_from;                    

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
                        log_cmsg("~C31 #WARN:~C00 breaking load block, due API temporary disabled");
                        break;     
                    }
                    
                    $lag_left = $block->min - $block->lbound;
                    $lag_right = $block->rbound - $block->max_avail;
    PROCESS_DATA:                

                    $small_data = !is_array($data) || count($data) < $this->default_limit;
                    if ($reverse)
                        $block->attempts_bwd += ( $this->cache_hit || $small_data ) ? 1 : 0;
                    else
                    $block->attempts_fwd += ( $this->cache_hit || $small_data ) ? 1 : 0;

                    $mini_cache = null;
                    $prev_count = count($block);
                    $prev_dups = $block->duplicates;
                    $imp = 0;                        

                    if (is_array($data) && count($data) > 0) {
                        $method = $this->import_method;                        
                        $prev_avail_min = $block->min_avail;
                        $prev_avail_max = $block->max_avail;                        
                        if (strlen($method) > 1 && method_exists($this, $method)) 
                            $mini_cache = $this->$method ( $data, 'Rest-API', $index < 0);               
                        else
                            throw new ErrorException("FATAL: method $method not found in ".get_class($this));
                    }

                    $count_diff = 0;
                    $dups_diff = 0;
                    if (is_object($mini_cache)) { 
                        $imp = count($mini_cache);    
                        $added += $imp;                                            
                        $mini_cache->Store($this->cache);                         
                        $this->OnCacheUpdate($block, $cache); // раздача данных всем блокам, включая текущий                         
                        $count_diff = count($block) - $prev_count;
                        $dups_diff = $block->duplicates - $prev_dups;
                        if (BLOCK_CODE::FULL == $block->code) {
                            $this->OnBlockComplete($block);
                            break; // больше нечего тут делать, при обработке кэша все решилось
                        }

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
                            log_cmsg("~C41~C97 #BLOCK_INVALID:~C40 ~C00 recovery request failed, no data covers %s, loaded data range: %s .. %s, first 5 keys imported: %s",
                                            strval($block), format_tms($oldest_ms), format_tms($newest_ms), json_encode($first_keys)); 
                            $dump = var_export($block, true);                                            
                            log_cmsg("~C31#BLOCK_DUMP:~C00 %s", substr($dump, 0, 200));                                            
                            // die("DEBUG BREAK\n");
                            $block->code = BLOCK_CODE::INVALID;
                            break;    
                        } elseif (0 == $count_diff && 0 == $dups_diff && $imp > 0 && $block->code !== BLOCK_CODE::FULL) {
                            log_cmsg("~C31 #NO_UPDATES:~C00 %d downloaded records have timestamps out of block %s volume %s, records accumulated %d, key range [%s..%s]",
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
                                log_cmsg("~C97 #BLOCK_FILLS_LEFT:~C00 was %s now %s, +filled %d ", color_ts($block->min_avail), color_ts($prev_avail_min), $count_diff);                                                
                            if ($block->max_avail > $prev_avail_max)                                             
                                log_cmsg("~C97 #BLOCK_FILLS_RIGHT:~C00 was %s now %s, +filled %d ", color_ts($block->max_avail), color_ts($prev_avail_max), $count_diff);                                                
                        }                        
                        
                        if ($before_ms < EXCHANGE_START || $oldest_ms < EXCHANGE_START)                              
                            log_cmsg("~C91 #ERROR:~C00 outbound oldest value select from  %s / %s / %s ",
                                    format_tms($this->oldest_ms), format_tms($before_ms), format_tms($oldest_ms));
                        
                        $before_ms = min($before_ms, $oldest_ms);                                   
                        $before_ms = max(EXCHANGE_START, $before_ms);
                    
                        $this->oldest_ms = min($this->oldest_ms, $oldest_ms);                        
                        $head_cover = $this->oldest_ms <= $lbound_ms;
                        $tail_cover = $newest_ms >= ($block->rbound_ms - 1000);

                        $lag_left = $block->VoidLeft();
                        $lag_right = $block->VoidRight();

                        if ($head_cover && $tail_cover ||                            
                            $block->attempts_bwd >= 5 || $block->attempts_fwd >= 5) {
                            $block->code = BLOCK_CODE::FULL;                                            
                            $block->info = 'cache covers block';
                        }
                        elseif ($block->fills > 0 && BLOCK_CODE::NOT_LOADED == $block->code) 
                            $block->code = BLOCK_CODE::PARTIAL;       

                        if ($reverse && $block->IsFullFilled()  || !$reverse && ($lag_right < 3600 && $lag_left < 3600 &&  $block->attempts_bwd++ > 2) )  {
                            // log_cmsg("~C97#AUTO_COMPLETION:~C00 small lags %d <-> %d ", $lag_left, $lag_right);
                            $block->code = BLOCK_CODE::FULL;
                            $block->info = 'both lags to small';
                        }   
                                            
                        $this->newest_ms = max($this->newest_ms, $newest_ms); // last block update => upgrade latest available tick
                        log_cmsg("~C96\t\t#BLOCK_SUMMARY({$this->loops})~C00: %d $data_name downloaded (%d saldo, %d in cache), range [%s..%s], block range [%s..%s]", 
                                    count($data), $added, $cache_size, 
                                    color_ts($oldest_ms / 1000),                                    
                                    color_ts($newest_ms / 1000),
                                    color_ts( $block->min_avail),                                
                                    color_ts( $block->max_avail));
                    }   // if mini_cache loaded 
                    elseif (is_array($data) ) {
                        if ($block->attempts_bwd + $block->attempts_fwd >= 10) {
                            if (0 == $block->fills && $block->min_avail == $block->rbound && $block->max_avail == $block->lbound) 
                                $block->code = BLOCK_CODE::VOID;
                            else    
                                $block->code = BLOCK_CODE::FULL;
                            $block->info = 'data not received/imported several times';
                            log_cmsg("~C31#VOID_RESPONSE:~C00  block marked as completed, now in cache %d $data_name ", json_encode($data), count($this->cache));
                        }
                        else
                            log_cmsg("~C94#VOID_RESPONSE:~C00 ~C00 block %d, no data received, attempts %d", $index, $block->attempts_bwd + $block->attempts_fwd);                    
                    }

                    $this->loops ++;                                                         
                    if (BLOCK_CODE::FULL == $block->code || BLOCK_CODE::VOID == $block->code) {                                                                
                        break;                                       
                    } 
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
                        log_cmsg("~C94#LAST_BLOCK_SYNC: ~C00 latest avail %s (+%d s), loaded %d $data_name, rest blocks %d$brk ", 
                                color_ts($last_ts), time() - $block->max_avail, $last_loaded, $this->BlocksCount());
                        if ($brk) break;                           
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
                log_cmsg("$tag~C33 #{$dname}_DOWNLOAD($m_cycles/$index)~C00: from ~C04%s before = %s, attempts <<< %d", 
                                $rqs_dbg, color_ts($tss), $block->attempts_bwd);                       
            else
                log_cmsg("$tag~C33 #{$dname}_DOWNLOAD($m_cycles/$index)~C00: from ~C04%s after = %s, attempts <<< %d", 
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
            $data = json_decode($res);            
            if (false !== strpos($res, 'error') || !is_array($data))  {
                log_cmsg("~C91#WARN: API request failed with response: %s", $res); // typical is ratelimit errro
                return null;
            }      
            if (is_array($data) && $backward_scan)
                $data = array_reverse($data); // API returns data in reverse order, normalizing  
            elseif (!is_array($data))     
                log_cmsg("~C91#REQUEST_FAILED:~C00 %s %s", gettype($data), substr($res, 0, 100));
            

            $this->rest_time_last = time();                        
            return $data;
        }

        public function QueryTimeRange() { // request lowes and highest timestamps            
            $range = array(false, false);
            $range[0] = $this->db_select_value('MIN(ts)',  '');
            $range[1] = $this->db_select_value('MAX(ts)',  '');
            if ($range[0]) {
                if ($this->get_manager()->cycles < 10)
                    log_cmsg("~C94#INFO_QTR:~C00 data range in DB: from [%s] to [%s] for %s", $range[0],  $range[1], $this->ticker);
                $this->saved_min = $this->TimeStampDecode($range[0], 1);
                $this->saved_max = $this->TimeStampDecode($range[1], 1);
            } else {
                $this->saved_min = 0;
                $this->saved_max = time() - 60;
                if (3 == $this->time_precision) 
                    $this->saved_max *= 1000;                

                log_cmsg(" #INFO: DB returns null, used range from 0 to now");
            }
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
            if (!$this->initialized)
                $this->CorrectTable();

            $this->QueryTimeRange();

            $start = $this->HistoryFirst();                    
            $start = floor($start); // if float => seconds
            if (!is_int($start))
                 $start = EXCHANGE_START_SEC;  // using abstract history start

            if (!$this->initialized)
                $start = max($start, $limit_past);
            $this->history_first = $start;          


            $end = $this->db_first(false, 1); // in seconds!
            if (!is_int($end) || $end < EXCHANGE_START_SEC) {
                log_cmsg("~C97#HISTORY_VOID:~C00 required full history download for %s", $this->ticker);
                $end = time() - 60;
            }

            if ($end <= $start)  {                
                log_cmsg("~C97#HISTORY_FULL:~C00 database oldest record time %s", color_ts($end));
                $this->head_loaded = true;                
                if ($this->initialized) return;
                $end = $start + 1 * SECONDS_PER_DAY - 1; // in seconds                 
            }

            if ($start >= $end) {
                log_cmsg("~C91 #ERROR(PrepareDownload): for {$this->symbol} assumed invalid scan range: start %s > end %s:",            
                                    format_ts($start), format_ts($end));
                $end = floor_to_day(time()) + SECONDS_PER_DAY - 60;                                     
            }

            $blocks_count = count($this->blocks);
            if (0 == $blocks_count && $this->data_flags & DL_FLAG_HISTORY) 
                $this->InitBlocks($start, $end); // here can also defined last_block
            elseif ($blocks_count > 10)
                log_cmsg("~C33#PREPARE_DL:~C00 already have %d blocks, scaning disabled", $blocks_count);
            
            $n_blocks = count($this->blocks);
            if ($n_blocks > 0) { 
                $start_tss = gmdate(SQL_TIMESTAMP, $start);
                $end_tss = gmdate(SQL_TIMESTAMP, $end);
                log_cmsg("~C97#MAPPING:~C00 performed for %s, produced %d blocks, from %s to %s", $this->ticker, $n_blocks, $start_tss, $end_tss);
            }            
            else
               $this->head_loaded = true;

            $this->nothing_to_download = 0 == $this->BlocksCount();

            $last = $this->db_last(false, 3); // in seconds
            if (0 == $this->newest_ms && !is_null($last)) 
                $this->newest_ms = $last;
            $this->initialized = true;
        }

        public function RestDownload(): int {
            global $verbose, $rest_allowed_t;
            if ($this->rest_errors > 100)  return -1;          
                          
            $n_blocks = count($this->blocks);
            $mgr = $this->get_manager();
            $minute = date('i');            
            if (0 == $n_blocks && ($mgr->cycles < 10 || 0 == $minute % 5)) {                
                if (0 == $this->loaded_blocks && $mgr->cycles < 30)
                    log_cmsg("~C34#REST_DOWNLOAD:~C00 too small blocks to download for %s, head loaded = %s; scaning...", $this->ticker, $this->head_loaded ? 'yep' : '~C31nope');                    
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

            if ($mgr->cycles < 2) return $total_loads;

                    
            if ($n_blocks > 0) {
                usort($this->blocks, 'compare_blocks'); // ascending by timestamp (lbound)
                $tmp = $mgr->tmp_dir;
                $class = get_class($this);
                $dump = json_encode($this->blocks);
                $dump = str_ireplace('},{', "},\n {", $dump);
                file_put_contents("$tmp/$class-{$this->symbol}_blocks.json", $dump);
            }


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
            

            // download history by mapping
            while ($index >= 0 && $big_loads < $this->blocks_at_once) {
                usefull_wait(100000);
                $block = $this->blocks[$index];
                $key = intval($block->code->value);                                
                if ($key < 0 || $block->index < 0) {
                    $index --;
                    continue;
                }

                $block->index = $index;                

                if ($block == $this->last_block)
                    throw new Exception("Last block available with index $index");       
                
                if (time() < $rest_allowed_t) {
                    log_cmsg("~C91#WARN:~C00 breaking loop, due REST API temporary disabled");
                    break;
                }

                if (BLOCK_CODE::NOT_LOADED == $block->code || BLOCK_CODE::PARTIAL == $block->code) {
                    $res = $this->LoadBlock($index);
                    $key = $block->code->value;                             
                    $total_loops += $this->loops;
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
                if ($elps >= $this->cycle_time * 0.75 || str_in($this->last_error, 'timeouted')) break;
                if ($minute >= 58) break; // last minutes = minimal work
            } // while - loading blocks one by one

            $blocks = array_reverse($this->blocks);

            foreach ($blocks as $block) 
                if ($block->Finished()) {
                    $this->OnBlockComplete($block);    
                    unset($this->blocks[$block->index]); 
                    unset($this->blocks_map[$block->key]);
                    $removed ++;                
                }

            $cache_size = count($this->cache);
            if ($cache_size > 0) {                
                $this->FlushCache();
            }
            
            if ($removed > 0)
                log_cmsg("~C97#COMPLETE:~C00 While download %d blocks removed for %s, codes: %s", $removed, $this->ticker, json_encode($code_map));

            $errs = implode("\n\t", array_keys($not_loaded));    
            if ($failed > 0 && str_in($errs, 'ERROR'))
                log_cmsg("~C31#WARN_DL_FAILED:~C00 While download %d blocks in %d loops failed for %s, errors: %s", 
                            $failed, $total_loops, $this->ticker, $errs);

            return $big_loads;
        } // RestDownload

        abstract public function OnBlockComplete(DataBlock $block);
        abstract protected function OnCacheUpdate(DataBlock $default, DataBlock $cache);
        
    }
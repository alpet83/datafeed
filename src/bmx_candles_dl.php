#!/usr/bin/php
<?php
    $last_exception = null;
    ob_implicit_flush();
    set_include_path(".:./lib:/usr/sbin/lib");
    require_once "common.php";
    require_once 'esctext.php';
    require_once "db_tools.php";
    require_once "lib/db_config.php";
    include_once "clickhouse.php";
    require_once "rate_limiter.php";
    require_once "candle_proto.php";
    require_once "bmx_websocket.php";
    require_once "proto_manager.php";
    require_once "bmx_dm.php";
    require_once 'vendor/autoload.php';

    
    $tmp_dir = '/tmp/bmx';
    define('REST_ALLOWED_FILE', $tmp_dir.'/rest_allowed.ts');    

    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time();

    function chdb_conn():? ClickHouseDB\Client {
        global $chdb;
        return $chdb;
    }

    if (file_exists(REST_ALLOWED_FILE)) {
        $rest_allowed_t = file_get_contents(REST_ALLOWED_FILE);
        log_cmsg("#DBG: RestAPI allowed after %s", gmdate(SQL_TIMESTAMP, $rest_allowed_t));    
        while (time() < $rest_allowed_t) {
            $elps = $rest_allowed_t - time();
            log_cmsg("#WARN: start will be delayed for $elps seconds, due RestAPI BAN applied");
            flush();
            set_time_limit(60);      
            sleep(30);
        }
    }    

    function detect_fields(stdClass $rec, string $keys = 'open,close,high,low'): array {
        $keys = explode(',', $keys);
        $best = $keys[0];
        $extr = $rec->$best;              

        $res = [];
        $min = $max = $extr;
        $min_k = $max_k = $best;

        foreach ($keys as $key) {
            $ref = $rec->$key;
            if ($ref > $extr) {
                $extr = $ref;
                $min_k = $key;               
            }
            if ($ref > $extr) {
                $extr = $ref;
                $max_k = $key;               
            }    
        }

        foreach ($keys as $key) {
            $ref = $rec->$key;
            if ($ref > $min && $ref < $max) {
                $key = 'I:'.$key;
                $res[$key] = 1;
            }                
        }
        
        $res['H:'.$max_k] = 1;
        $res['L:'.$min_k] = 1;
        return $res;        
    }

    file_put_contents($tmp_dir.'/candle_dl.ts', date(SQL_TIMESTAMP)); 
    error_reporting(E_ERROR | E_WARNING | E_PARSE);
    mysqli_report(MYSQLI_REPORT_ERROR);  

    $manager = false;
           

    class BitMEXCandleDownloader extends CandleDownloader {     
        

        

        public   function   __construct(DownloadManager $mngr, stdClass $ti) {
            $this->days_per_block = 1;
            $this->default_limit = 50 * 60; // include excess for flood            
            parent::__construct($mngr, $ti);                                    
            $this->CreateTables();            
            $this->RegisterSymbol('candles', $this->pair_id);                       
            if (0 === stripos($ti->symbol, 'BTCUSD') || 0 === stripos($ti->symbol, 'ETHUSD'))
                $this->normal_delay = 30; 

            // if ($this->db_first(false) < strtotime('2020-01-01 0:00:00')) $this->tail_loaded = true;

        } // constructor


        /**
         * HistoryFirst - request couple of candles with highest timeframe, for detecting first date of history
         * @return bool|int - timestamp or false
         */
        public function HistoryFirst(): bool|int {
            $def_first = false;
            $res = parent::HistoryFirst();
            if ($res > 0) 
                return $res;
            

            $min_t = strtotime('2013-01-01 00:00');           

            $this->last_error = '';
            $url = $this->rest_api_url.'trade/bucketed';                        
            $interval = '1m';                       
            $params = ['symbol' => $this->symbol, 'partial' => 'true', 'binSize' => $interval, 'reverse' => 'false', 'start' => 0, 'count' => 10];                                   
        
            log_cmsg("~C97#HISTORY_FIRST_BMX:~C00 requesting initial candles with params %s", json_encode($params));
            $json = $this->api_request($url, $params);             
            if (false === strpos($json, '[') || false !== strpos($json, 'error')) {
                $this->last_error = sprintf("#ERROR: rest API for %s, %s returned %s",  $url, $params, $json);
                return $def_first; // WARN: problema here
            }   

            if ('[]' == $json && $this->saved_min > $min_t) { // expected head loaded                
                return $this->saved_min; 
            }   

            $data  = json_decode($json);
            if (0 == count($data)) {
                $this->last_error = "#ERROR: failed decode $json";
                return $def_first;
            }   
            $mts = strtotime_ms($data[0]->timestamp);  
            foreach ($data as $rec) 
                $mts = min($mts,    strtotime_ms($rec->timestamp));            
            if ($mts <= strtotime_ms('2010-01-01 00:00')) return $def_first;      
            $start = floor($mts / 1000);            
            $this->last_error = sprintf('#OK: processed %d records, result = %.3f', count($data), $start);      
            log_cmsg("~C103~C94 #INIT:~C00 for %s detected first candle time is %s", $this->symbol, gmdate(SQL_TIMESTAMP, $start));  
            return $start;
        }

                

        /** function LoadCandles - request REST API for loading data before or after specified timestamp  */
        public function LoadCandles(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 1000): ?array {
            $url = $this->rest_api_url.'trade/bucketed';
            $params = ['symbol' => $this->symbol, 'binSize' => '1m', 'count' => $limit];          
            $params ['columns'] = 'open,close,high,low,volume';
            $params ['reverse'] = $backward_scan ? 'true' : 'false';
            $t_from = strtotime($ts_from);
            
            if ($t_from % 60 > 0)
                $t_from = $backward_scan ? ceil($t_from / 60) * 60 : floor($t_from / 60) * 60; // round to minutes, depends back/forward
                
            // фактически все свечи в системе - не свечи, временные метки у них смещены вправо. Т.е. последняя минутка вчерашего дня, имеет метку полуночи сегоднянего. Требуется AHEAD     
            $ts_from = format_ts($t_from + $this->current_interval);                             

            $age = time() - $t_from;
            if ($block->index < 0 && !$backward_scan && $age < 600)
                $params['partial'] = 'true'; // partial data

            $tkey = $backward_scan ? 'endTime' : 'startTime';
            $params [$tkey] = $ts_from;
            return $this->LoadData($block, $url, $params, $ts_from, $backward_scan);            
        }

        

        public function LoadDailyCandles(int $per_once = 1000, bool $from_DB = true): ?array {            
            $mysqli = sqli();            
            $stored = parent::LoadDailyCandles($per_once, $from_DB);            
            // if (is_array($res) && count($res) > 0) return $res;            
            
            $url = $this->rest_api_url.'trade/bucketed';
            $per_once = min($per_once, 1500); 

            $params = ['symbol' => $this->symbol, 'binSize' => '1d', 'columns' => 'open,close,high,low,volume', 
                       'partial' => 'true', 'reverse' => 'false', 'count' => $per_once];                      
            $cursor = 0; // is_array($res) ? count($res) - 1 : 0;            

            $json = 'fail';                        
            $table_name = "{$this->table_name}__1D";
            if (is_array($stored))
                $cursor = count($stored) - 7; // not request all, update single week
            else
                $stored = [];

            $updates = [];    
            $cursor = max(0, $cursor);

            $orig_table = $this->table_name;
            try {
                $this->table_name = $table_name;                
                log_cmsg("~C93 #LOAD_DAILY:~C00 requesting daily candles for %s, available in DB %d", $this->symbol, count($stored));
                $candles = new CandlesCache($this);
                
                $mysqli->try_query("CREATE TABLE IF NOT EXISTS $table_name LIKE $orig_table");
                if (0 == $cursor)                     
                    $mysqli->try_query("TRUNCATE TABLE $table_name");                                  
                
                $candles->interval = $this->current_interval = SECONDS_PER_DAY;
                $start = time();
                while (time() - $start < 30) {
                    $params['start'] = $cursor;
                    $rqs = $url.'?'.http_build_query($params);
                    log_cmsg("~C93   #API_REQUEST:~C00 %s", $rqs);
                    $json = $this->api_request($url, $params, -1);
                    $data = json_decode($json);
                    if (!is_array($data) || count($data) == 0) break;
                    $part = $this->ImportCandles($data, 'REST-API-1D', true);
                    $imp = 0;
                    if (is_object($part)) {
                        $imp =  count($part);
                        $cursor += $imp;
                        $part->Store($candles);                          
                    }
                    if ($imp < $per_once) break;                    
                }            
                
                if (0 == count($candles)) 
                    log_cmsg("~C91#ERROR_SERIOUS:~C00 failed load/import daily candles via %s, last err %s, JSON = %s", 
                                $this->last_api_request, $this->last_error, substr($json, 0, 200));                                
                else  {
                    $tk = $candles->lastKey();                    
                    $last = $candles->last();
                    $updates = $candles->Export();                  

                    log_cmsg("~C92#SUCCESS:~C00 loaded %d daily candles, interval %d, trying save to %s, lasst %s : %s", 
                                count($candles), $this->current_interval, $this->table_name, color_ts($tk), json_encode($last, JSON_NUMERIC_CHECK) );                                                                    
                }
            } 
            finally {
                $this->table_name = $orig_table;  
                $this->current_interval = 60;           
            }           
            return $this->daily_map = array_replace($stored, $updates);
        }
       
        public   function    ImportCandles(array $data, string $source, bool $direct_sync = true): ?CandlesCache {
            global $verbose;
            $this->last_error = '';
            $mysqli_df = sqli_df();
            $mgr = $this->manager;
            $uptime = $mgr->Uptime();
            $this->last_cycle = $mgr->cycles;

            if (!$this->initialized && $uptime < 300 && $this->current_interval < SECONDS_PER_DAY) 
                $this->CorrectTable();
                    
            if (0 == count($data)) {
                $this->last_error = 'void source data';
                return null;
            }

            $dbg_from  = time() - SECONDS_PER_DAY * 5;  // start of day
            $now = time() * 1000;
            $cnt = 0;
            $rcnt = count($data);
            $updated = 0;
            $invalid = 0;
            $strange = 0;            
            $flood = 0;  
            $dups = 0;                      

            $stats = [];
            $row = [];            
            $errs = [];            
            $row = [];

            $last_block = $this->last_block;

            $result = new CandlesCache($this);           
            $result->interval = $this->current_interval;
            $result->mark_flags = $direct_sync ? CANDLE_FLAG_RTMS : 0;
            $tk = time();
            $t_table = "bitmex__ticks__{$this->ticker}";
            $have_ticks = false;
            $chdb = chdb_conn();
            if ($chdb && $stmt = $chdb->select("SHOW TABLES LIKE '$t_table' ")) 
                $have_ticks = $stmt->count() > 0;

            $interval = $this->current_interval;
            $intraday = $interval < SECONDS_PER_DAY;
            $day_last = SECONDS_PER_DAY - $interval;

            if ($interval < 60)
                throw new Exception("Abnormal interval $interval for API candles ");

            foreach ($data as $rec) {                
                if (!is_object($rec) || !isset($rec->timestamp)) {
                    $invalid ++;                    
                    $this->last_error .= format_color ("~C31#INVALID:~C00 %s\n", var_export($rec, true));
                    continue;
                }
                if (!isset($rec->close)) {
                    $flood ++;
                    $this->last_error = "~C94#FLOOD:~C37 ".json_encode($rec)."~C00\n"; 
                    continue;
                }
                                   
                
                // согласно документации, это метка записи свечи, т.е. close_time, поэтому надо вычитать interval как минимум
                $tms = strtotime_ms($rec->timestamp);                
                if ($tms < EXCHANGE_START) {
                    $this->last_error .= format_color ("~C91#ERROR:~C00 candle timestamp outbound: %s ", json_encode($rec));
                    continue;
                }
                
                $tk = floor($tms / 1000) - $interval;  // future candle timestamp with shift               
                // BitMEX timestamp are for bucketed trade is close (saving time)!!!! Very ugly :(
                $tk = floor($tk / $interval ) * $interval; // round to minutes/days, for using as key                

                if ($tk >= $now + 100) {
                    $this->last_error .= format_color("~C91 #REJECT:~C00 attempt import candle from future %s\n", gmdate(SQL_TIMESTAMP, $tms / 1000));
                    log_cmsg($this->last_error);
                    continue;
                }          

                // при прямой синхронизации подразумеваются полезными только свежие данные хвоста блока, чтобы поменьше обращаться к БД - пропуск старых
                if ($direct_sync && $intraday && $tk < $last_block->max_avail) {
                    $dups ++;
                    continue;
                }

                $row = [$rec->open, $rec->close, $rec->high, $rec->low, $rec->volume];

                if (isset($result[$tk])) {
                    $v = $rec->volume;
                    if ($v > $result[$tk][4])
                        $updated ++;
                }
                else
                    $cnt ++; // detect adding new                
                
                $open = null;
                // due documentation open is value of previous candle...
                if ($rec->open < $rec->low || $rec->open > $rec->high) {
                    // retrieving first trade via REST request or from local trades                    
                    if ($have_ticks && $chdb) {  // checking in ClickHouse bigdata                      
                        $tss = format_ts($tk);
                        $tsc = format_ts($tk + $interval);
                        $stmt = $chdb->select("SELECT price FROM $t_table WHERE (ts >= '$tss') AND (ts < '$tsc')");
                        if (is_object($stmt) && !$stmt->isError())
                           $row[0] = floatval($stmt->fetchOne()); 
                    }
                }
                
                // after all open will be invalid, if not reloaded from ticks
                $row[0] = min($row[0], $rec->high);
                $row[0] = max($row[0], $rec->low);               
                
                $row = $this->CheckCandle($tk, $row, $errs);  // save to cache
                if (count($errs) > 0) {
                    log_cmsg(" ~C91#WARN:~C00 for source %s candle record %s have problem: %s ", $source, json_encode($rec), implode(',  ', $errs));
                    $strange ++;
                }   

                if ($day_last == $tk % SECONDS_PER_DAY && $tk > $dbg_from) 
                    log_cmsg("~C94 #BMX_DBG:~C00 %s:%d candle %s source timestamp %s, open = %f, close = %f, ticks open %s", 
                                $this->ticker, $interval, color_ts($tk), $rec->timestamp, $rec->open, $rec->close, json_encode($open));                                 


                $result->SetRow($tk, $row); // save

                $flds = detect_fields($rec, 'open,close,high,low');                
                foreach ($flds as $key => $inc)                 
                    $stats[$key] = $inc + ($stats[$key] ?? 0);
                // print_r($row);
            }
           
            $result->OnUpdate();
            $cnt = count($result);
            $target = $direct_sync ? $this->table_name : 'cache';
            if ($rcnt > 0 && 0 == $cnt && 0 == $updated) {
                $this->last_error .= format_color ("no fresh data was imported from $rcnt records. Invalid %d, flood %d, dups %d. Test: %s" ,
                                                    $invalid, $flood, $dups, var_export($data[0], true));
                return null;
            }

            $ftk = $result->firstKey();
            $ltk = $result->lastKey();

            if ($direct_sync && null !== $ltk) {
                if ($ftk >= $this->last_block->lbound) 
                    $result->Store($this->last_block);

                $cnt = $this->SaveToDB($result, false);                                
                $elps = $now / 1000 - ($ltk + 59); // assume close time
                if ($elps > 60 && str_in($source, 'partial'))
                    log_cmsg("~C31#WS_ELPS_WARN:~C00 last candle for %s started at %s delayed for %d seconds, source %s", $this->symbol, color_ts($ltk), $elps, $source);
            }

            if ($verbose >= 3 && !$direct_sync) {
                $last = $result->last();
                log_cmsg("... into %s $target inserted %d, updated %d,  from %d rows; flood %d, strange %d, total in cache_map %.2fK candles, detect = %s, result last [%s]=%s, last errors: %s",
                            $this->ticker, $cnt, $updated, $rcnt, $flood, $strange, 
                            count($this->cache) / 1000.0, json_encode($stats),
                            format_ts($tk), json_encode($last), $this->last_error);
            }
                           
            return $result;            
        } // ImportCandles

    } // class BitMEXCandleDownloader


    class  CandleDownloadManager
            extends BitMEXDownloadManager {

        private $last_group_dl = 0;

        public function __construct($symbol) {
            $this->ws_data_kind = 'tradeBin1m';
            $this->db_name = DB_NAME;
            $this->loader_class = 'BitMEXCandleDownloader';            
            parent::__construct($symbol, 'candles');
            $this->default_limit = 5000;
        } // constructor


        protected function GroupDownload(array $keys) {
            $map = [];
            $loader = null;
            foreach ($this->loaders as $pair_id => $dl) {                
                $loader = $this->GetLoader($pair_id, 'pair_id');
                $map [$loader->symbol] = $loader;
            }           
            
            $elps = time() - $this->last_group_dl;
            if ($elps >= 10) {
                $this->LatestCandles($map, $this->default_limit);
                $this->last_group_dl = time();
            }
            parent::GroupDownload($keys);
        }

        public function LatestCandles(array $map, int $limit = 1000): ?array {
            $url = "{$this->rest_api_root}/trade/bucketed";
            $params = ['binSize' => '1m', 'count' => $limit];          
            $params ['columns'] = 'open,close,high,low,volume';
            $params ['reverse'] = 'true';
            $params['partial'] = 'true'; // partial data            
            $params ['start'] = 0; // date(SQL_TIMESTAMP, time() + 60);
            $first = array_key_first($map);
            $loader = $map[$first] ?? null;
            if (!is_object($loader) || !($loader instanceof BitMEXCandleDownloader)) return [];            

            $cache = new CandlesCache($loader);
            $cache->index = -3; // group marker
            $ts = format_ts(time());
            $res = $loader->LoadData($cache, $url, $params, $ts, true);            
            if (!is_array($res)) return [];
            
            $raw_data = [];
            foreach ($map as $symbol => $loader)
                $raw_data[$symbol] = [];

            foreach ($res as $rec) {
                if (!is_object($rec)) 
                    throw new Exception("~C91#ERROR:~C00 invalid record type ".var_export($rec, true));
                $symbol = $rec->symbol;                
                if (isset($rec->open) && isset($map[$symbol])) 
                    $raw_data [$symbol][] = $rec;
            }

            $imp_loaders = 0;
            foreach ($raw_data as $symbol => $data) {
                $loader = $map[$symbol];
                if ($loader instanceof BitMEXCandleDownloader) {                    
                    $cache = $loader->ImportCandles($data, 'REST-API-Fast', true);
                    $imp_loaders += is_object($cache) ? 1 : 0;
                }
            }
            log_cmsg("~C96#PERF_SYNC_REST:~C00 latest candles downloaded %d records, matched symbols %d / %d, imported for %d ", 
                        count($res), count($raw_data), count($map), $imp_loaders);            
            return $raw_data;
        }

        protected function SelfCheck(): bool {
            global $mysqli_df;
            $mysqli = sqli();
            $mysqli_df = $mysqli;
            $replica = is_object($mysqli) ? $mysqli->replica : null;
            $elps = time() - $this->last_db_reconnect;

            if (!is_object($mysqli) || !$mysqli->ping()) {
                log_cmsg("~C91 #FAILED:~C00 connection to DB is lost, trying reconnect...");
                
                $mysqli_df = $mysqli = init_remote_db(DB_NAME);            
                $this->last_db_reconnect = time();
                if (!$mysqli_df) {                    
                    sleep(30);
                    return false;
                }
                
            }         

            if (!is_object($replica) || !$replica->ping()) {                
                if ($elps > 60) {  // not to frequent                    
                    log_cmsg("~C31 #WARN:~C00 replication DB connection is lost, trying reconnect...");
                    $replica = init_replica_db(DB_NAME);
                    $this->last_db_reconnect = time();
                }
                else    
                    $replica = null;
            }
            $mysqli->replica = is_object($replica) ? $replica : null;
            return true;
        }        

        protected function Loader(int $index): BitMEXCandleDownloader {
            return $this->loaders[$index];
        }   

        
        public function VerifyRow(mixed $row): bool {
            return is_object($row) && isset($row->timestamp) && isset($row->open) && isset($row->close) && isset($row->symbol);
        }

    } // ImportDataWS

    $ts_start = pr_time();
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

   
    $hour = date('H');
    $hstart = floor(time() / 3600) * 3600;     
    $pid_file = sprintf($tmp_dir.'/candles_dl@%s.pid', $symbol);
    log_cmsg("~C97#INIT:~C00 trying lock PID file %s...", $pid_file);
    try {
        $pid_fd = setup_pid_file($pid_file, 300);       
    } 
    catch (Exception $E) {
        log_cmsg("~C91#ERROR:~C00 failed lock PID file %s, %s", $pid_file, $E->getMessage());
        error_exit("~C91#FATAL:~C00 failed lock PID file $pid_file");
    }

    if (date('H') != $hour) 
        error_exit("~C91#FATAL:~C00 pid lock wait timeouted, hour $hour ended");  
    
    $log_base = 'logs/bmx_candles_dl';
    $log_name = sprintf('%s@%s-%d.log', $log_base, $symbol, $hour); // 24 logs rotation    
    chdir(__DIR__);
    $log_file = fopen(__DIR__."/$log_name", 'w');

    while (!flock($log_file, LOCK_EX)) {
        log_cmsg("~C91#ERROR:~C00 $log_name is already locked"); // here can be hangout 
        sleep(10);
    }

    log_cmsg("~C97 #START:~C00 trying connect to DB...");
    $mysqli_df = $mysqli = init_remote_db(DB_NAME);
    if (!$mysqli) 
         error_exit("~C91 #FATAL:~C00 cannot initialze DB interface! ");               

    $chdb = null;
    $pid = shell_exec('pidof clickhouse-server');
    $pid = trim($pid);
    if (strlen($pid) > 1 && is_integer($pid) || !str_in(CLICKHOUSE_HOST, '127.0.0.1')) {
        $chdb = ClickHouseConnect(CLICKHOUSE_USER, CLICKHOUSE_PASS, DB_NAME);         
    }

    $mysqli->try_query("SET time_zone = '+0:00'");
    $mysqli->set_opt(MYSQLI_OPT_READ_TIMEOUT, 60);
    $mysqli->replica = init_replica_db(DB_NAME);
    log_cmsg("~C93 #START:~C00 connected to~C92 localhost@$db_name_active~C00 MySQL, ClickHouse [$pid] = ".is_object($chdb)); 
    $elps = -1;          
    $manager = new CandleDownloadManager($symbol);
    log_cmsg("~C97#APP_START");
    main($manager);    
    echo "FINISHED...\n";
    fclose($log_file);
    flock($pid_fd, LOCK_UN);
    fclose($pid_fd);    
    if (filesize($log_name) > 20000)
        system("bzip2 -f --best $log_base*"); // pack all, including errors
    $log_file = false;  
    unlink($pid_file);
?>
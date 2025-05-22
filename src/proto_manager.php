<?php        
    require_once 'lib/rate_limiter.php';

    const SECONDS_PER_DAY = 24 * 3600;
    const WORKING_PERIOD = 3570; // 60 * 59 + 30 seconds

    function time_us(): int {
        return floor(microtime(true) * 1000000); // us
    }

    function usefull_wait(int $us) {
        global $manager;
        $start = time_us();
        $ns_wait = min($us, 25000) * 1000; // 25000us = 25ms

        if (is_object($manager)) {
            $elps = 0;
            $limit = 1 + $us / 100000; // estimated 0.1 sec for cycle
            for ($i = 0; $i < $limit; $i++) {
                $manager->ProcessWS(false);
                $elps = time_us() - $start; // ms to us                
                if ($elps >= $us) break;
                $info = [];                
                pcntl_sigtimedwait([SIGQUIT, SIGUSR1, SIGKILL, SIGTERM], $info, 0, $ns_wait);  // real delay                              
            }      
            if ($elps < $us)
                usleep($us - $elps);
        }  
        else 
            usleep($us);
        $real = time_us() - $start;
        $elps = $real / 1000000;

        if ($real > $us + 100000 && $elps >= 3)            
            log_cmsg("~C31 #PERF_WARN:~C00 elapsed time for wait = %.3f sec, but requested %.3f", $elps, $us / 1000000);
        
    }


    function init_replica_db(string $db_name, array $db_hosts = [MYSQL_REPLICA])  {
        global $db_error, $db_profile, $db_alt_server;
        $db_alt_server = '<no hosts defined>';
        $db_profile = [];
        $mysqli = init_remote_db($db_name, null, null, null, $db_hosts);  
        if (is_object($mysqli) && $mysqli instanceof mysqli_ex) {            
            log_cmsg("~C97#DB_INFO:~C00 connected to replica DB via %s, server info %s ", $db_alt_server, $mysqli->server_info); 
            $mysqli->try_query("SET time_zone = '+0:00'");        
        }
        elseif (count($db_profile) > 0)
            log_cmsg("~C31 #WARN:~C00 can't connect to replica DB, last server %s, error: %s", $db_alt_server, $db_error);
        else
            log_cmsg("~C31 #ERROR:~C00 can't connect to replica DB, due profile not configured");
        return $mysqli;
    }


    function sig_handler(int $signo) {
        global $manager;
        if (SIGUSR1 == $signo)  {
            log_cmsg("#SIGUSR1: current location: %s ", format_backtrace());
            return;
        }
        log_cmsg("~C101~C97 #SIGNAL_STOP: ~C00 signal %d received, exiting...", $signo);
        if (is_object($manager))             
            $manager->Stop();        
        else
            exit(0);        
        if (SIGTERM == $signo) 
            throw new Exception("SIGTERM signal received, exiting...");
    } // sig_handler

    function error_handler(int $errno,  string $errstr, string $errfile,         int $errline, array $errcontext): bool {
        log_cmsg("~C91#UNHANDLED_ERROR:~C00  %d: %s in %s on line %d", $errno, $errstr, $errfile, $errline);
        log_cmsg("~C93#TRACE:~C00 %s ", format_backtrace(0,'basic', 15, 0, true));
        return false;
    }

    class BadLoaderException extends Exception {
        public function __construct(DownloadManager $msg, string $symbol) {
            parent::__construct("No loader selected for $symbol");
            throw $this;
        }
    }

    abstract class  DownloadManager  {

        public      $cycles = 0;
        public      $cycle_start = 0;             // timestamp in seconds

        /** engine limit for REST downloads */
        public      int $default_limit = 10000;   
        /** Base URL for REST API */
        public      string $rest_api_root = ''; 
        protected   $loaders = [];         
        public      $ready_subscribe = false;   
        protected   $subs_map   = [];
        protected   $ws = false;

        protected   $ws_active = false;
        public      $ws_imports  = 0;

        protected   $ws_empty_reads = 0;
        protected   $ws_events_cnt = 0;           // simple counter
        protected   $ws_reconnects = 0;
        protected   $ws_records_cnt = 0; 
        protected   $ws_recv_bytes = 0;           // bytes counter 
        protected   $ws_recv_last = 0;           // last time of data received
        protected   $ws_recv_packets = 0;         // packets counter
        protected   $ws_loads_total = 0;          // accumulated loads/records counter
        protected   $ws_data_kind = 'dummy';      // name of data for subscription 

        protected   $ws_connect_t = 0;
        private     $ws_report_t = 0;        
        protected   $ws_stats = [];      // map of events or data counters
        protected   $ws_sub_started = [];  // map [pair_id] = timestamp, for timeout control

        protected   $loader_class = 'BadLoaderException';
               

        protected   array $ticker_list   = [];
        protected   $tables = ['activity' => 'src_activity'];

        public      $db_tables_mysql = []; // all tables 
        public      $db_tables_clickhouse = []; // all tables


        public      $last_db_reconnect = 0;       

        public      $alive_t = 0;
        protected   $create_t = 0;

        protected   $minute = 0;
        


        protected   $db_name = 'datafeed';

        public      $active = true;

        public      $exchange = '';
        public      object $rate_limiter;
        public      $start_cache = [];
        
        public      $tmp_dir = '/tmp';
        
        public function __construct(string $symbol, string $data) {            
            $this->create_t = time();                 
            $db_name = sqli()->query("SELECT DATABASE()")->fetch_column(0);                    
            if ('datafeed' == $db_name)
                throw new Exception ("FATAL: attempt using active DB `datafeed` for data");

            foreach (['ticker_map', 'data_config'] as $suffix)
                if (!isset($this->tables[$suffix])) 
                    $this->tables[$suffix] = $this->TableName($suffix);                
            $this->tables['activity'] = 'datafeed.src_activity';                
            $this->ticker_list = $this->LoadSymbols($symbol, $data);    
            $this->db_tables_mysql = sqli()->show_tables();

            if (!is_array($this->ticker_list) || 0 == count($this->ticker_list))
                throw new ErrorException("FATAL: can't load ticker list on init for $symbol");            
        } // constructor
        
        abstract protected function CreateWebsocket();        
        

        public function SaveActivity() {
            $elps = time() - $this->alive_t;
            if ($elps < 10) return;
            $this->alive_t = time();

            $name = get_class($this);
            $pid = getmypid();
            $ws_loads = $rest_loads = 0;
            foreach ($this->loaders as $downloader) {
                $rest_loads += $downloader->rest_loads;
                $ws_loads += $downloader->ws_loads;
            }           

            $uptime = time() - $this->create_t;
            $host = trim(gethostname());
            $host = sqli()->real_escape_string($host);
            $table_name = $this->tables ['activity'];
            $query = "INSERT INTO $table_name (`name`, `exchange`, `host`, PID) VALUES ('$name', '{$this->exchange}', '$host', $pid)\n ";
            $query .= "ON DUPLICATE KEY UPDATE `ts_alive` = NOW(), `uptime` = $uptime, `PID` = $pid, `ws_loads` = $ws_loads, `rest_loads` = $rest_loads";
            try {
                if (sqli()->try_query($query))
                    log_cmsg("~C97#ACTIVITY:~C00 updated %d rows", sqli()->affected_rows);           
            } catch (Exception $E) {
                log_cmsg("~C91#EXCEPTION:~C00 %s: used query %s ", $E->getMessage(), $query);
                error_exit("Trouble");
            }          
        }

        public function GetLoader(mixed $value, string $field = 'symbol'): mixed {
        
            foreach ($this->loaders as $downloader) 
                if (isset($downloader->$field) && $value === $downloader->$field)
                    return $downloader;
                
            return false; 
        }
    
        public function GetTables(): array {
            return $this->tables;
        }

        public function GetRTMLoaders(): array {
            $result = [];
            foreach ($this->loaders as $pair_id => $downloader) 
                if ($downloader->IsRealtime())
                    $result[$pair_id] = $downloader;
            return $result;
        }

        protected function GroupDownload (array $keys) {
            foreach ($keys as $pair_id)                                      
                if (!$this->ProcessDownload($pair_id)) break;
            

        }

        public function Initialize() {                      
            $loader_class = $this->loader_class;
            $class = get_class($this);
            log_cmsg("~C97#INITIALIZE($class):~C00 tickers for processing %d", count($this->ticker_list));
            foreach ($this->ticker_list as $pair_id => $ti) {                
                $ti->pair_id = $pair_id;  // need for loader
                log_cmsg("~C93#DBG:~C00 Creating instance of %s for  %s:%s, params %s",
                          $loader_class, $ti->symbol, $ti->ticker, strval($ti));
                $downloader = new $loader_class($this, $ti);  // global function, must be defined                                     
                // $downloader->PrepareDownload();
                $this->loaders  [$ti->pair_id]= $downloader;                
                // registering symbol and ticker, updating configuration
            }
        
            foreach ($this->loaders as $downloader) {                
                $downloader->CreateTables();
                $downloader->QueryTimeRange();         
                // $downloader->PrepareDownload();
            }
            log_cmsg("~C103~C30 #INITIALIZE: ================================= completed for %d~C103~C30 loaders ======================================~C40", count($this->loaders));
            $this->rate_limiter->Wait();
            return count($this->loaders);
        }
        abstract protected function ImportDataWS(mixed $data, string $context): int;


        /** function LoadSymbols load table of all symbols and config value for specified kind of data  */
        protected function LoadSymbols(string $symbol, string $data): array {
            global $mysqli;                        
            $tm_table = $this->tables['ticker_map'] ?? '';                         
            $table_cfg = $this->tables['data_config'] ?? '%FAILED%';
            if (0 == strlen($tm_table)) 
                throw new Exception(format_color("#FATAL: invalid tables config: %s", json_encode($this->tables)));

            $column = "load_$data";           
            $strict = '';
            if ('all' !== $symbol) 
                $strict = "AND TM.symbol = '$symbol'";
            $mixer = "INNER JOIN $tm_table AS TM ON `id_ticker` = `id` WHERE `$column` > 0 $strict ORDER BY symbol";                  
            $res = $mysqli->select_map("pair_id, ticker, symbol,`$column` as enabled, id_ticker", $table_cfg, $mixer, MYSQLI_OBJECT);            
            if (is_array($res) && count($res) > 0)                           
                log_cmsg("~C93 #DBG:~C00 enabled for download symbols: [%s]", json_encode( $res));              
            else {
                log_cmsg("~C91#ERROR~C00: retrieve symbols from `$table_cfg` failed with result [~C91{$mysqli->error}~C00], or table empty, query: {$mysqli->last_query} ");
                throw new Exception("No symbols selected for download from $table_cfg!");
            }   
            return $res;                
        } // function LoadSymbols

        protected function ReconnectWS($reason) {
            $loaders = $this->GetRTMLoaders();
            if (0 == count($loaders)) return;
            $elps = time() - $this->ws_connect_t;
            if ($elps < 100)  return;           
            $this->ws_connect_t = time();
            log_cmsg("~C91 #WS_WARN:~C00 WebSocket reconnect due %s, realtime loaders %d", $reason, count($loaders));
            $ws = $this->ws;
            $this->ws_reconnects ++;
            $this->ws_active = false;                        
            $this->ws = null;
            $this->CreateWebsocket();            
            $this->subs_map = [];
            $this->platform_status = -1; // means unknown
            $this->ready_subscribe = time() + 10;
            foreach ($this->loaders as $loader)
                if (is_object($loader)) {                
                    $loader->ws_channel = 0;
                    $loader->ws_sub = 0;
                }
        }
    
                /**         
        * bool @return : if true - cycle can be continued
         */

         protected function ProcessDownload (int $pair_id): bool {
            global $ts_start, $rest_allowed_t;
            $uptime = pr_time() - $ts_start;
            $minute = date('i') * 1 + date('s') / 60;
            
            $downloader = $this->Loader($pair_id);
            $cycle_elps = time() - $this->cycle_start;
            // первые циклы - полные, последующие с обрывами            
            if ($cycle_elps > $downloader->cycle_time && $this->cycles > 1) return true;
            
            $sym = $downloader->symbol;
            $last_tss = 'never';                
            if ($minute >= 59.5 || $uptime >= WORKING_PERIOD || !$this->active) return false;
            $this->ProcessWS(false); // realtime import is fast
            $data_elps = time() - $downloader->db_last(false);
            if ($uptime < 40 && $data_elps <= 60) return true; // REST download start paused

            $elps = (time_ms() - $downloader->newest_ms) / 1000; 
            if ($downloader->newest_ms > 0)   // only tail data avail
                $last_tss = format_tms( $downloader->newest_ms);
            
            // if ($downloader->LastBlock()->code > 0)$pre_loaded ++;                     

            if ($downloader->loaded_full()) {             
                $rest_elps = $downloader->Elapsed();
                $ws_elps  =  $downloader->WSElapsed();
                if ($pair_id == $this->cycles % 1000)
                    log_cmsg("~C92#SEEMS_GOOD:~C00 history for %s is loaded full; elps = %d / %d / %d, loaded blocks = %d ", 
                                $downloader->ticker, $elps, $rest_elps, $ws_elps, $downloader->loaded_blocks);         
                //  $full_loaded ++; 
                if ($elps < $downloader->normal_delay || $this->cycles == $downloader->last_cycle) return true;  // non volatile data
                if ($rest_elps < 500) return true;
            }          

            $overrun = 0 == $minute && $this->cycles > 1000;  // working after hour break
            if (time() < $rest_allowed_t || $minute >= 58 || $overrun) {
                // log_cmsg("~C03~C33 #REST_SKIP({$this->cycles}/$pair_id):~C00 allowed ts = %s", format_TS( $rest_allowed_t));
                return true; // using last minutes for unban         
            }

            $downloader->RestDownload();   // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< main work                  
            return true;
        }

        public  function Run() {
            global $rest_allowed_t;             
            $this->cycles ++; 
            $this->cycle_start = time();
            $uptime = time() - $this->create_t;
            if (!$this->SelfCheck()) return;

            $rtm = $this->GetRTMLoaders();
            if (!is_object($this->ws) && $uptime >= 10 && count($rtm) > 0) {
                log_cmsg("~C97 #WS_INFO:~C00 Switching to WebSocket update, cycles = %d uptime = %d", $this->cycles, $uptime);
                $this->CreateWebsocket();  // switch RTM download
            }            
            
            $ws_lag = 0;            
            $ws_elps = time() - $this->ws_recv_last;
            if (is_object($this->ws)) {
                $ws_lag = time() - $this->ws->last_ping;            
                
            }
            $sort_v = [];
            $keys = [];
            $minute = date('i') * 1 + date('s') / 60;
            $full_loaded = 0;
            $total_blocks = 0;
            if ($minute <= 58.5 && time() > $rest_allowed_t) 
                foreach ($this->loaders as $pair_id => $loader) {
                    $loader->ProcessTasks();                                 
                    if (!($loader instanceof BlockDataDownloader)) continue;        
                    $total_blocks += $loader->BlocksCount();
                    $data_lag = time() - max($loader->last_block->max_avail, $loader->ws_newest);
                    if ($loader->loaded_full() && $data_lag < 300) {
                        $full_loaded ++; 
                        continue;
                    }
                    $rest_active = max($loader->rest_time_last, $loader->init_time_last);
                    $elps = time() - $rest_active;
                    if ($elps >= 30) {
                        $sort_v [] = $rest_active; 
                        $keys []= $pair_id;
                    }
                }
            // загрузчки сортируются в порядке времени последней загрузки REST, чтобы самым отстающим больше шансов было
            array_multisort($keys, SORT_ASC, $sort_v);     
            $minute_i = floor($minute);            
            if ($minute_i != $this->minute) {
                $this->minute = $minute_i;
                log_cmsg("~C92#ALIVE~C00(%d): cycles %5d, scheduled for history download %3d blocks for %3d tickers, full loaded %3d / %3d loaders; WebSocket ping elps = %d, elps = %d seconds, reconnects = %d", 
                            getmypid(), $this->cycles, $total_blocks, count($keys), $full_loaded, count($this->loaders), $ws_lag, $ws_elps, $this->ws_reconnects);         
                $ts_dump = [];
                foreach ($sort_v as $i => $t) {
                    $pair_id = $keys[$i];
                    $ts = $t > 0 ? color_ts($t) : 'never';
                    $loader = $this->GetLoader($pair_id, 'pair_id');
                    $ticker = $loader ? $loader->ticker : "#$pair_id?";
                    $ts_dump []= sprintf("%s@%s", $ticker, trim($ts));
                }
                log_cmsg('~C93#DBG:~C00 rest_time_last sequence %s', implode(', ', $ts_dump));
                $this->SaveActivity();            
                $this->ProcessWS(true);
            }

            if ($uptime < WORKING_PERIOD && time() > $rest_allowed_t)    
                $this->GroupDownload($keys);

            $failed_sub = 0;
            foreach ($this->ws_sub_started as $pair_id => $t) {
                $elps = time() - $t;
                if ($elps > 60) {
                    $failed_sub ++;
                    $loader = $this->GetLoader($pair_id, 'pair_id');
                    if (!is_object($loader)) continue;
                    log_cmsg("~C91 #WS_SUB_FAILED:~C00 for %s, elapsed time %d sec", $loader->symbol, $elps);
                    $loader->ws_sub = false;
                    unset($this->ws_sub_started[$pair_id]);
                }
            }
            if ($failed_sub > 0 && $this->ws_active)
                $this->SubscribeWS(); // try to re-subscribe
            usefull_wait(10000);
        }

        public function Stop (){
            $this->active = false;
        }
        protected function LoadWS() { // read data from WebSocket
            if (!is_object($this->ws)) return false;
            $now = time();
            $elps = $now - $this->ws_report_t;            
            if ($elps >= 300) {
                $this->ws_report_t = $now;
                log_cmsg("~C93#WS_REPORT:~C00 events = %d, records = %d, data = %.1f KiB in %d packets, imports = %d ", 
                         $this->ws_events_cnt, $this->ws_records_cnt, $this->ws_recv_bytes / 1024, $this->ws_recv_packets, $this->ws_imports);
            }

            if (!$this->ws->isConnected()) {         
                $this->ReconnectWS('disconnect status');
                return false;
            }      

            $unread = 1; // $this->ws->unreaded();  
          
            $readed = 0;     
            $t_start = pr_time();
            while ($unread > 0) { 
                $readed += $this->LoadPacketWS(); 
                $elps = pr_time() - $t_start;
                if (!is_object($this->ws) || $elps >= 1) break; // connection lost / timeout(hang)
                $unread = $this->ws->unreaded();                                
            }  // while unread

            foreach ($this->loaders as $loader) 
                if (is_object($loader)) {
                    $loader->ImportWS(true, 'WebSocket/LoadWS');                    
                }                       

            return $readed;
        }


        protected function LoadPacketWS(): int {  
            $trecv = 0;                   
            if (!is_object($this->ws)) return 0;

            $uptime = time() - $this->create_t;

            foreach ($this->loaders as $loader) 
                if (is_object($loader))
                    $trecv = max ($trecv, $loader->ws_time_last);
                
            $elps = time() - $trecv;
            $ws = $this->ws;        
            $ping_elps = time() - $ws->last_ping;            


            if ($trecv > 0 && $elps > 300) {         
                if ($elps > SECONDS_PER_DAY)
                    $elps = '~C31never~C00';

                if ($ping_elps > 120)
                    $this->ReconnectWS("last recieve age $elps ".color_ts($trecv));
                else {   
                    log_cmsg("~C31#WARN_WS_DATA_LAG:~C00 last data age %d, ping elps %d sec. Need re-subscribe", $elps, $ping_elps);
                    foreach ($this->GetRTMLoaders() as $loader) 
                        $loader->ws_sub = false;                                        

                }                
            }
            
            try {            
                $avail = $ws->unreaded();
                if (0 == $avail) return 0;

                $t_start = pr_time();
                $recv = $ws->receive();
                $opcode = $ws->getLastOpcode();
                $elps = pr_time() - $t_start;
                if ($elps > 3) 
                    log_cmsg("~C31 #PERF_WARN(LoadPacketWS):~C00 elapsed time for receive = %.3f sec", $elps);                

                if (null == $recv || strlen($recv) < 1) return 0;                

                $this->ws_empty_reads = 0;
                $this->ws_recv_packets ++;
                $this->ws_recv_last = pr_time();
                $this->ws_recv_bytes += strlen($recv);
                if (0 == $this->ws_recv_packets % 100)
                    log_cmsg("~C96#WS_PERF:~C00 loaded packets %d, data summary %.3f KiB", 
                                $this->ws_recv_packets, $this->ws_recv_bytes / 1024);

                if (0 == strlen($recv)) return 0;
                $recv = trim($recv);
                if ('{' == $recv[0]) { // object/record data 
                    $data = json_decode($recv, false);                
                    if (!is_object($data)) {
                        log_cmsg("~C91 #WARN~C00: recv decoded as %s ", gettype($data));
                        return 0;
                    }  
                    $this->ws_records_cnt ++;
                    $this->ProcessRecord($data);                     
                    return 1;
                } 
                elseif ('[' == $recv[0]) {  // detected array
                    $data = json_decode($recv);        
                    try          
                    {
                        $this->ws_imports += $this->ImportDataWS($data, '');
                    } catch (Exception $E) {
                        log_cmsg("~C91 #WS_EXCEPTION:~C00 %s in %s ", $E->getMessage(), $E->getTraceAsString());
                    }
                    if (0 == $this->ws_recv_packets % 100) 
                        log_cmsg("~C96#PERF_WS:~C00 packets:%d, data:%.3fMiB ", $this->ws_recv_packets, $this->ws_recv_bytes / 1048576);
                    return 1;
                }   
                elseif ($recv == 'pong' ) {                     
                    $this->ws_stats ['pong'] = ($this->ws_stats ['pong'] ?? 1) + 1; 
                    if ($uptime < 180)
                        log_cmsg("~C94 #WS_PONG:~C00 connection still alive");
                }
                elseif ('ping' == $opcode) {
                    $ws->last_ping = time(); 
                    // log_cmsg("~C94 #WS_PING_RX:~C00 payload %s", $recv);
                    $rec = json_decode("{\"payload\":\"$recv\"}", false);
                    $this->on_ws_event('ping', $rec);
                }
                elseif ('pong' == $opcode) {
                    $ws->last_ping = time();
                    $rec = json_decode("{\"payload\":\"$recv\"}", false);
                    $this->on_ws_event('pong', $rec);
                }
                elseif ('close' == $opcode) {
                    log_cmsg("~C31 #WS_CLOSE_WARN:~C00 reason %s", $recv);                    
                    $this->ws_active = false;
                    $this->ws = null;
                }
                else    
                    log_cmsg("~C91 #WS_WARN(LoadPacketWS):~C00 unknown kind of data %s:%s", $opcode, $recv);                   

                
            } catch (Exception $E) {       
                $msg = $E->getMessage();
                $this->ws_stats ['exceptions'] = ($this->ws_stats ['exceptions'] ?? 1) + 1; 
                file_add_contents("{$this->tmp_dir}/ws_exception.log", $msg);
                if (!str_in($msg, 'Empty read') && !str_in($msg, 'Broken frame') && !str_in($msg, 'Bad opcode'))
                    log_cmsg("~C91 #EXCEPTION(LoadPacketWS):~C00 %s", $msg);              
                else
                    $this->ws_empty_reads ++;
                    
                if ($this->ws_empty_reads > 4 && $ping_elps > 120) {          
                    $this->ReconnectWS("receive fails too much, for $avail bytes: $msg");
                } // reconnect typical 
            } // exception handle
            return 0;
        } // LoadPacketWS


        public function ProcessWS(bool $wait = true): int {                  
            $loads = 0;
            if (!is_object($this->ws)) return -1;
            $ws = $this->ws;
            if ($ws->isConnected()) {
                $now = time();
                $uptime = $now - $this->create_t;                
                $last_ping = $ws->last_ping;
                $elps = $now - $last_ping;
                if ($elps > 30) 
                    try {
                        $ws->last_ping = time() - 20; // prevent ping DDoS                        
                        if ($uptime < 180 && $elps < 3600)
                            log_cmsg("~C94 #WS_PING_LATE:~C00 previus ping elapsed time %d sec, unreaded %d bytes", $elps, $ws->unreaded());
                        $ws->ping();        
                                         
                        if ($elps >= 60) $wait = true;
                    } catch (Throwable $E) {
                        log_cmsg("~C31 #EXCEPTION:~C00 while trying ping WebSocket: %s", $E->getMessage());                        
                        $this->ws_stats ['exceptions'] = ($this->ws_stats ['exceptions'] ?? 1) + 1; 
                        if ($elps >= 60 || $this->ws_stats ['exceptions'] > 5)  {
                            $this->ReconnectWS('ping failed / connection lost');
                            $this->ws_stats ['exceptions'] = 0;
                        }
                    }                 
            }                 
            else
                return -2;


            $unreaded  = $this->ws->unreaded();
            if (!$wait && 0 == $unreaded) 
                return 0;

            if ($unreaded > 2000)
                log_cmsg("~C96#PERF(LoadWS):~C00 avail for read %d bytes", $unreaded);

            $t_start = pr_time();    
            if ($this->LoadWS()) 
                $loads ++;                        
            $elps = pr_time() - $t_start;

            if ($elps > 5) {
                $wait = $wait ? 'wait' : 'no wait';
                log_cmsg("~C31 #PERF_WARN_WS:~C00 elapsed time for LoadWS = %.3f sec with $wait ", $elps);
            }

            if ($this->ready_subscribe && time() > $this->ready_subscribe && $this->ws_active) {
                $this->ready_subscribe = time() + 5; // next attempt in 5 minutes
                $this->SubscribeWS();
            }
            return $loads;
        }

        protected function ProcessRecord(stdClass $data) {  // простая имплементация, расчитана на тип API: Bitfinex
            if (isset($data->event)) {              
                $this->on_ws_event($data->event, $data);                      
                $this->ws_events_cnt ++;
            }  
        }
        abstract protected function SubscribeWS();

        public function TableName(string $suffix): string {
            return (sqli()->active_db() == $this->db_name) ? $suffix : "{$this->db_name}.$suffix";
        }

        abstract protected function on_ws_event(string $event, mixed $data);

        public function Uptime() {
            return time() - $this->create_t;
        }

        abstract public function VerifyRow(mixed $row): bool;


    }; // class DownloadManager 

    function main(DownloadManager $manager) {
        global $ts_start, $hstart, $last_exception;
        $last_exception = null; // used by format_traceback
        set_error_handler('error_handler', 	E_RECOVERABLE_ERROR | E_USER_ERROR | E_CORE_ERROR );

        if (pcntl_signal (SIGQUIT, 'sig_handler') && pcntl_signal (SIGTERM, 'sig_handler') &&
            pcntl_signal (SIGUSR1, 'sig_handler'))
                log_cmsg("~C97#SUCCESS:~C00 signal handler registered, async handling %s", 
                                                    pcntl_async_signals(null) ? 'yes' : 'no');
        pcntl_async_signals(true);

        ini_set('precision', 13);
        ini_set('serialize_precision', -1);
        try {                             
            $elps = 0;
            $lcount = $manager->Initialize();            
            if (0 == $lcount)
                throw new ErrorException( 'FATAL: No loaders initialized');

            log_cmsg("~C97#LOOP($hstart):~C00 staring download loop with %d loaders...", $lcount);
            while ($manager->active && $elps < WORKING_PERIOD) {      
                set_time_limit(30);
                $elps = pr_time() - $ts_start;            
                $minute = date('i') + date('s') / 60.0;  // float value            
                if ( $minute > 59.5  && $elps > 120) {
                    log_cmsg("~C91 #END:~C00 hour limit excess, breaking...");
                    break;
                }
                $manager->Run();
                usefull_wait(100000);
            } // main loop
            log_cmsg(" #OK: Script complete with time %.1f seconds", $elps);    
        } 
        catch (Throwable $E) {
            $msg = var_export($E->getMessage(), true);            
            $last_exception = $E;
            log_cmsg("~C91#FATAL_EXCEPTION:~C00 catched $msg at %s:%d, STACK:\n %s", $E->getFile(), $E->getLine(), format_backtrace(-1,'basic', 15, 0, true));
            log_cmsg("~C93#TRACE:~C00 %s", $E->getTraceAsString());
        }
        
    }

<?php
    require_once 'proto_manager.php';
    require_once 'bmx_websocket.php';        
    require_once 'lib/bitmex_common.php';    

    $curl_default_opts->SetCompressed('gzip');
    
    abstract class BitMEXDownloadManager 
        extends DownloadManager {       
        
        public function __construct(string $symbol, string $data) {            
            global $tmp_dir;
            $this->tmp_dir = $tmp_dir;
            $this->db_name = DB_NAME;
            $this->exchange = 'bitmex';      
            $this->rest_api_root = 'https://www.bitmex.com/api/v1/';            
            // all config tables in MySQL!
            $this->rate_limiter = new RateLimiter(30);
            $this->rate_limiter->SpreadFill();            
            $this->default_limit = 3000;
            parent::__construct($symbol, $data);                        
        }

        public function web_socket(): ?BitMEXClient {
            return $this->ws ? $this->ws : null;
        }

        protected function on_ws_event(string $event, mixed $data) {            
            if ('subscribe' == $event && isset($data->subscribe) && $data->success) {
                if (is_string($data->subscribe)) {
                    $this->on_subscribe($data->subscribe);
                    return;
                }
                if (!isset($data->request->args)) return;
                $args = $data->request->args;  
                log_cmsg("~C93#DBG_SUBS:~C00 %s", print_r($data, true));
                if (is_string($args))  {
                    if (str_in($args, '['))
                        $args = json_decode($args);
                    else
                        $this->on_subscribe($args);
                }               
            } elseif ('info' == $event) {
                $info = $data->info ?? '';
                if (isset($data->platform))
                    log_cmsg("~C97 #WS_INFO~C00: %s %s", $info, json_encode($data->platform));
                elseif (isset($data->version))
                    log_cmsg("~C97 #WS_INFO~C00: %s %s", $info, $data->version);

                if (str_in($info, 'Welcome')) {
                    $this->ws_active = true;
                    $this->SubscribeWS();
                }
                // if (1 == $data->platform->status)     $this->SubscribeWS();
            } //*/
            elseif ('pong' == $event) {
                // all good!
            }
            else
              log_cmsg("~C31#WS_EVENT_UNKNOWN:~C00 %s with data: %s", $event, var_export($data, true));
        } // function on_ws_event

        protected function on_subscribe(string $arg) {
            $symbol = str_replace($this->ws_data_kind.':', '', $arg);            
            $downloader = $this->GetLoader($symbol);
            if ($downloader) {
                $downloader->ws_sub = true;
                unset($this->ws_sub_started[$downloader->pair_id]);
                log_cmsg("~C97 #WS_NICE:~C00 subscribed to %s ", $arg);            
            }
            else
                log_cmsg("~C31 #WARN_SUBSCRIBE:~C00 no downloader for %s", $arg);
        } // on_subscribe        
        
        
        protected function  CreateWebsocket()  {          
            $rtm = $this->GetRTMLoaders();            
            if (0 == count($rtm)) return;

            $keys = array_keys($rtm);
            $args = [];                        
            foreach ($keys as $n_loader) {        
                $downloader = $this->Loader ($n_loader);    
                if (!str_in($downloader->symbol, 'XBT') || !$downloader->IsRealtime()) continue; // only BTC pairs 
                if (count($args) >= 10) continue;
                $args []= "{$this->ws_data_kind}:{$downloader->symbol}";
                $this->ws_sub_started[$downloader->pair_id] = time();
                log_cmsg("~C97 #WS_SUB~C00: symbol = %s", $downloader->symbol);                                
                
            }

            $this->ws_connect_t = time();
            $params = [];            
            if (count($args) > 0)
                $params = ['subscribe' => implode(',', $args)];
            $this->ws = new BitMEXClient($params);
            if (!$this->ws->isConnected())             
                log_cmsg("~C91 #WS_ERROR:~C00 failed to connect to WebSocket server, params = %s", json_encode($params));                
                                       
        }


        
        protected function ImportDataWS(mixed $data, string $context): int {
            global $verbose; 
            if (!is_array($data)) {
                 log_cmsg("~C91 #ERROR:~C00 invalid data type %s", gettype($data));
                 return 0;
            }
            

            $data_map = [];      
            foreach ($data as $rec) {                
                $sym = $rec->symbol;
                if (!isset($data_map[$sym]))
                     $data_map[$sym] = [];                

                if ($this->VerifyRow($rec)) 
                    $data_map[$sym][]= $rec;
            }
            
            $total = 0;
            $skipped = [];
            foreach ($data_map as $symbol => $records) {
                $downloader = $this->GetLoader($symbol);
                if (!is_object($downloader)) {
                    $skipped []= $symbol;
                    continue;                
                }
                if (isset($downloader->current_interval) && $downloader->current_interval > 60) {
                    log_cmsg("~C34 #WS_SKIP:~C00 %s, current interval %d", $symbol, $downloader->current_interval);
                    continue; // skip
                }
                $cntr = count($records);
                $downloader->ws_log_file = "{$this->tmp_dir}/ws_last_data-{$this->ws_data_kind}.log";
                $imported = $downloader->ImportWS($records, $context);
                if ($imported > 0) {                    
                    $downloader->ws_loads += $imported;
                    $downloader->ws_time_last = time();
                    $total += $imported;
                }                
                if ($verbose > 3 || $this->alive_t < 100 || 0 == $imported && $cntr > 10)
                    log_cmsg("~C97   #WS_IMPORT~C00: %s source %d, imported %d, inserted %d rows. First: %s ", 
                                    $symbol, $cntr, $imported, $imported, json_encode($records[0]));
            }               
            if (count($skipped) > 0) 
                log_cmsg("~C31 #WARN_WS_IMPORT:~C00 no downloader for %s", implode(', ', $skipped));                   

            return $total;
        } // function ImportDataWS
        

        public function ProcessRecord(stdClass $rec) {
            try {
                $tmp_dir = $this->tmp_dir;
                $sfx = 'unk';                
                if (isset($rec->success) && $rec->success && isset($rec->request)) {
                    $this->on_ws_event($rec->request->op, $rec);        
                    $sfx = 'evt';
                }    
                elseif (isset($rec->table) && $rec->table == $this->ws_data_kind &&  
                        isset($rec->data) && is_array($rec->data)) {                    
                    $imp = $this->ImportDataWS($rec->data, "->$rec->action"); 
                    $sfx = $imp > 0 ? 'data' : 'void';                    
                }
                elseif (isset($rec->info)) {
                    $this->on_ws_event('info', $rec);                    
                    $sfx = 'msg';
                }
                elseif (isset($rec->foreignNotional)) {
                    // nothing to do 
                    $sfx = 'flood';
                }
                elseif (isset($rec->error) && isset($rec->status)) {                    
                    if (str_in($rec->error, 'subscribed to this topic')) {                        
                        preg_match ('/topic:\S*\:(\S*)./', $rec->error, $matches);
                        if (isset($matches[1])) 
                            $this->on_subscribe(trim($matches[1], '. '));                                                
                    }                    
                    else {
                        log_cmsg("~C91#WS_ERROR:~C00 status %d, messsage: %s",  $rec->status, $rec->error);
                        if (isset($rec->request));
                           log_cmsg("~C31#FAILED_REQUEST:~C00 %s", print_r($rec->request, true));
                    }
                    $sfx = 'err';
                }
                else
                    log_cmsg("~C94#UNKNOWN_RECORD:~C00 %s", print_r($rec, true));

                if (isset($rec->action))
                    $sfx = "{$rec->action}-$sfx";
                $this->ws_stats[$sfx] = ($this->ws_stats[$sfx] ?? 0) + 1;     
                if (0 == $this->ws_stats[$sfx] % 50)
                    log_cmsg("~C93#WS_STATS:~C00 %s", json_encode($this->ws_stats));          
                file_save_json("$tmp_dir/ws_last_data-{$this->ws_data_kind}-$sfx.json", $rec);                
            } catch (Throwable $E) {                
                log_cmsg("~C91#EXCEPTION(ProcessRecord):~C00 %s in %s:%d from: %s", 
                        $E->getMessage(), $E->getFile(), $E->getLine(), $E->getTraceAsString());
                log_cmsg("~C93#PARAMS_DATA:~C00 %s", print_r($rec, true));
            } // try

        }                
       

        protected function SubscribeWS() {
            $args = [];
            // next symbols subscribe
            $already = 0;            
            $max = 0;

            foreach ($this->loaders as $pair_id => $downloader) {                
                if ($downloader->IsRealtime()) $max ++;
                if (!is_object($downloader) || !$downloader->IsRealtime() || $downloader->ws_sub) {
                    $already += $downloader->ws_sub ? 1 : 0;
                    continue;                
                }
                if (count($args) >= 20) continue;
                if (isset($this->ws_sub_started[$pair_id])) continue;
                $args []= "{$this->ws_data_kind}:{$downloader->symbol}";                                
                $this->ws_sub_started[$pair_id] = time();
            }               
            if (0 == count($args))
                return;             
                        
            log_cmsg("~C97 #WS_SUBSCRIBE~C00: already subscribed %d / %d, add = %s", $already, $max, json_encode($args));
            $this->ws->subscribe( $args);         
            $this->ProcessWS(true);
        }  // function SubscribeWS

    }

    

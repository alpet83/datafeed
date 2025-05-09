<?php    
    require_once "proto_manager.php";

    const DB_NAME = 'bitfinex';
    const HISTORY_MIN_TS = '2015-01-01 00:00:00'; // minimal history start for initial download

    define('EXCHANGE_START', strtotime_ms('2013-04-01 00:00')); // указывать лучше день начала работы биржи, до минут
    define('EXCHANGE_START_SEC', floor(EXCHANGE_START / 1000));

    $curl_default_opts->SetCompressed();

    abstract class BitfinexDownloadManager 
        extends DownloadManager {


        public function __construct(string $symbol, string $data) {
            global $tmp_dir;  
            $this->exchange = 'bitfinex';
            $this->rest_api_root = 'https://api-pub.bitfinex.com/v2/';
            $this->tmp_dir = $tmp_dir;
            $this->db_name = DB_NAME;
            // print_r($this->tables);
            $this->rate_limiter = new RateLimiter(15);
            $this->rate_limiter->SpreadFill();
            parent::__construct($symbol, $data);
        }

        protected function  CreateWebsocket()  {
            $this->ws = new BitfinexClient(2);
            $this->ws_connect_t = time();
            $this->ws_active = false;
        }     
        

        protected function ImportUpdateWS(mixed $data, string $context): int  {            
            if (is_string($data[1]) && 'hb' != $data[1]) 
                log_cmsg("~C94#WS_UPDATE:~C00 %s", $data[1]);            
            return 0;
        }


        protected function ImportDataWS(mixed $data, string $context): int {
            global $verbose; 
            $id = $data[0];
            if (!isset($this->subs_map[$id])) {
                log_cmsg("~C94#WS_IGNORE:~C00 not subscribed for data id = %s ", $id); 
                return 0; // another data?
            }

            $multi = $data[1];           

            $saver = 0 == $this->ws_recv_packets % 500 ? 'file_put_contents' : 'file_add_contents';
            $log_name = "{$this->tmp_dir}/ws_data_{$this->ws_data_kind}.log";
            $saver ($log_name, tss().' = '.json_encode($data). "\n");

            if (is_string($multi))
                return $this->ImportUpdateWS($data, $context); // heartbeat info or updates

            $symbol = $this->subs_map[$id];
            $downloader = $this->GetLoader ($symbol);
            if (!$downloader) {
                log_cmsg("~C91#WS_UNKNOWN:~C00 symbol %s ", $symbol); 
                return 0 ;  // WTF???
            }           
            
            $imported = 0;            
            $type = 'update';
            // recheck first row is compat
            if (is_array($multi) && isset($multi[0]))  {
                $inserted = 0;
                $test = $multi[0]; // first record of snapshot or MTS field of first record
                if ($this->VerifyRow($test)) {                    
                    $type = 'snapshot:'.count($multi);
                }                    
                elseif ($this->VerifyRow($multi)) {                    
                    $multi = [$multi]; // update mode (for candles)
                } 
                else 
                    log_cmsg("~C31 #WS_WARN_UNDETECTED:~C00 first %s: %s", gettype($test), json_encode($test));                 

                
                $imported = $downloader->ImportWS($multi, "-$type"); // directly to DB                
                if ($imported > 0) {                    
                    $downloader->ws_loads += $imported;
                    $downloader->ws_time_last = time();
                } else {                    
                    $dump = json_encode($multi);
                    $dump = substr($dump, 0, 1000). '...';
                    log_cmsg("~C94#WS_SKIPPED:~C00 symbol %s %s $type count = %d, dump start: %s", 
                        $symbol, gettype($multi), count($multi), $dump);  
                }

                if ($verbose > 3)
                    log_cmsg("~C97 #WS_IMPORT~C00: %s source %d, imported %d, inserted %d rows", $symbol, count($multi), $imported, $inserted);
            }
            else
                log_cmsg("~C91#WS_WRONG_DATA:~C00 %s", var_export($multi, true));              
             

            return $imported;            
        } // function ImportDataWS

        
        protected function on_subscribe(int $id, string $key) {            
            $sub_prefix = "{$this->ws_data_kind}:";
            $this->subs_map[$id] = str_replace($sub_prefix, '', $key);  // for candles            
            $sym = $this->subs_map[$id];
            $loader = $this->GetLoader($sym);        
            if (is_object($loader)) {
                $loader->ws_sub = true;
                unset($this->ws_sub_started[$loader->pair_id]);
                log_cmsg("~C97 #WS_SUBCRIBE:~C00 confirmed for %s @ #%d \n", $sym, $id);
            }
            else    
                log_cmsg("~C31 #WS_SUBCRIBE_UNKNOWN:~C00 confirmed for %s @ #%d \n", $sym, $id);
        }

        protected function on_ws_event(string $event, mixed $data) {
            
            $id = $data->chanId ?? 0;
            $key =  isset($data->key) ? $data->key : $data->symbol ?? '???'; // for trades and ticker

            if ('subscribed' == $event) {                
                $this->on_subscribe($id, $key);                            
            } // on subscribe            
            elseif ('info' == $event && isset($data->platform)) {
                log_cmsg("~C97 #WS_CONNECT~C00: %s", print_r($event, true));
                $this->platform_status = $data->platform->status;                

                if (1 == $data->platform->status) {
                    $this->ws_active = true;
                    $this->SubscribeWS();
                }
                else
                    log_cmsg("~C31 #WS_PROBLEM:~C00 platform status %d", $data->platform->status);
            }
            elseif ('pong' == $event) {
                // all good!
            }
            elseif ('error' == $event) {
                if (10301 === $data->code)                     
                    $this->on_subscribe($id, $key);                                             
                else
                    log_cmsg("~C31 #WS_ERROR:~C00 %s", print_r($data, true));
            }
            else
                log_cmsg("~C94 #WS_EVENT:~C00 %s: %s", $event, print_r($data, true));
        } // function on_ws_event

    }

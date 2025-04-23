<?php
    require_once "proto_manager.php";


    const DB_NAME = 'bitfinex';

    define('EXCHANGE_START', strtotime_ms('2013-04-01 00:00')); // указывать лучше день начала работы биржи, до минут
    define('EXCHANGE_START_SEC', floor(EXCHANGE_START / 1000));


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
            if (is_string($multi))
                return $this->ImportUpdateWS($data, $context); // heartbeat info or updates

            $symbol = $this->subs_map[$id];
            $downloader = $this->GetLoader ($symbol);
            if (!$downloader) {
                log_cmsg("~C91#WS_UNKNOWN:~C00 symbol %s ", $symbol); 
                return 0 ;  // WTF???
            }
           
            
            $imported = 0;
            $single_row = false;
            // recheck first row is compat
            if (is_array($multi))  {
                $inserted = 0;
                if (1 == count($multi)) {  // have signle array of rows inside array
                    $rows = $multi[0];
                    if (is_array($rows) && isset($rows[0]) && is_array($rows[0]) && $this->VerifyRow($rows[0]))
                        $multi = $rows;
                    elseif (count ($rows) <= 6)
                        print_r($rows); // single item?
                }
                elseif ($this->VerifyRow($multi)) {
                    $single_row = true;
                    $multi = [$multi]; // single row at once
                }

                $imported = $downloader->ImportWS($multi, ''); // directly to DB
                // count($ticks);
                if ($imported > 0) {                    
                    $downloader->ws_loads += $imported;
                    $downloader->ws_time_last = time();
                } else {
                    $is_row = $single_row ? 'single row' : 'multiple rows';
                    $dump = json_encode($multi);
                    $dump = substr($dump, 0, 1000). '...';
                    log_cmsg("~C94#WS_SKIPPED:~C00 symbol %s %s $is_row count = %d, dump start: %s", 
                        $symbol, gettype($multi), count($multi), $dump);  
                }

                if ($verbose > 3)
                    log_cmsg("~C97 #WS_IMPORT~C00: %s source %d, imported %d, inserted %d rows", $symbol, count($multi), $imported, $inserted);
            }
            else
                log_cmsg("~C91#WS_WRONG_DATA:~C00 %s", var_export($multi, true));              
             

            return $imported;            
        } // function ImportDataWS

        
        protected function on_ws_event($event, $data) {
            if ('subscribed' == $event) {
                $id = $data->chanId;
                if (isset($data->key))
                    $this->subs_map[$id] = str_replace($this->ws_data_kind.':', '', $data->key);  // for candles
                elseif (isset($data->symbol)) 
                    $this->subs_map[$id] = $data->symbol; // for trades and ticker

                log_cmsg("~C97 #WS_SUBCRIBE:~C00 confirmed for %s @ #%d \n", $this->subs_map[$id], $id);
            } // on subscribe
            if ('info' == $event && isset($data->platform)) {
                log_cmsg("~C97 #WS_CONNECT~C00: %s", print_r($event, true));
                if (1 == $data->platform->status)
                    $this->SubscribeWS();
            }
        } // function on_ws_event

    }

<?php
    require_once "proto_manager.php";
    require_once "bnc_websocket.php";

    const DB_NAME = 'binance';
    const HISTORY_MIN_TS = '2018-01-01 00:00:00'; // minimal history start for initial download
    define('EXCHANGE_START', strtotime_ms('2017-07-14 00:00')); 
    define('EXCHANGE_START_SEC', floor(EXCHANGE_START / 1000));

    USE WSSC\Contracts;


    abstract class BinanceDownloadManager 
        extends DownloadManager {
        public function __construct(string $symbol, string $data) {
            global $tmp_dir;  
            $this->exchange = 'binance';
            $this->rest_api_root = "https://data-api.binance.vision/api/v3/";
            $this->tmp_dir = $tmp_dir;
            $this->db_name = DB_NAME;
            // print_r($this->tables);
            $this->rate_limiter = new RateLimiter(15);
            $this->rate_limiter->SpreadFill();
            parent::__construct($symbol, $data);
        }   

        protected function  CreateWebsocket()  {          
            $rtm = $this->GetRTMLoaders();            
            if (0 == count($rtm)) return;
            $this->ws = new BinanceClient ($this->ws_data_kind, []);
            if ($this->ws->isConnected()) {
                $this->ws_active = true;
                $this->SubscribeWS();
            }
        }

       

        protected function on_ws_event(string $event, mixed $data) {
            if ('subscribe' == $event && is_array($data)) {
                foreach ($data as $line) {
                    $symbol = str_replace("@{$this->ws_data_kind}", '', $line);
                    $symbol = strtoupper($symbol);
                    $downloader = $this->GetLoader ($symbol);
                    if (null === $downloader) {
                        log_cmsg("~C91#WS_IGNORE_SUB:~C00 no downloader  for %s ", $symbol);
                        continue;
                    }
                    $downloader->ws_sub = true;
                    unset($this->ws_sub_started[$downloader->pair_id]);
                    log_cmsg("~C97#WS_SUBSCRIBED:~C00 %s", $symbol);
                }
            } elseif ('ping' == $event && isset($data->payload)) {
                $ts = format_tms($data->payload);
                if ($data->payload > time_ms() - 300000)                
                    log_cmsg("~C94#WS_PING:~C00 payload %s", color_ts($ts));
                $this->ws->send($data->payload, 'pong');
            }
        }


        protected function SubscribeWS() {
            $keys =  array_keys($this->GetRTMLoaders());
            $already = 0;
            $added = 0;            
            $ws = $this->ws;
            $list = [];
            if (!$keys) return;            

            foreach ($keys as $pair_id) {       
                $downloader = $this->Loader ($pair_id);                                
                if ($downloader->ws_sub) {
                    $already ++;
                    continue;
                }                
                $list []= strtolower("{$downloader->symbol}@{$this->ws_data_kind}");
            }
            if (0 == count($list)) return;

            log_cmsg("~C97 #WS_SUBSCRIBE~C00: already subscribed %d / %d, add = %s", $already, count($keys), json_encode($list));
            if (is_object($ws) && $ws instanceof BinanceClient && count($list) > 0) {
                $ws->Subscribe ($list);
            }

        }

        public function ProcessRecord(stdClass $rec) {
            $ws = $this->ws;
            if (!$ws instanceof BinanceClient) return; // this only for VisualStudio PHPSense

            try {
                

                // typical response without error and data
                if (isset($rec->id) && null === $rec->result) {
                    $id = $rec->id;
                    $rqs = $ws->requests[$id];
                    $method = $rqs['method'] ?? 'nope';
                    if ('SUBSCRIBE' == $method)
                        $this->on_ws_event('subscribe', $rqs['params']);
                    
                }
                elseif (isset($rec->e) && isset($rec->E) && isset($rec->s))
                    $this->ImportDataWS($rec, 'WebSocket-event');
                else
                    log_cmsg("~C94 #WS_RAW:~C00 unknown input %s", json_encode($rec));
                
                
            }
            catch (Exception $E) {
                log_cmsg("~C91#EXCEPTION(ProcessRecord):~C00 %s in %s:%d from: %s", 
                            $E->getMessage(), $E->getFile(), $E->getLine(), $E->getTraceAsString());
                log_cmsg("~C93#PARAMS_DATA:~C00 %s", print_r($rec, true));
            }

        }

    }

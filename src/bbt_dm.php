<?php
    require_once 'proto_manager.php';
    require_once 'bbt_websocket.php';

    const DB_NAME = 'bybit';    
    const HISTORY_MIN_TS = '2023-01-01 00:00:00'; // minimal history start for initial download
    define('EXCHANGE_START', strtotime_ms('2018-03-01 00:00')); 
    define('EXCHANGE_START_SEC', floor(EXCHANGE_START / 1000));

    $curl_default_opts->SetCompressed('gzip');

    USE WSSC\Contracts;

    abstract class BybitDownloadManager         
        extends DownloadManager {

        public  $category = 'linear';
        private $pings = 0;
        public function __construct(string $symbol, string $data) {
            global $tmp_dir;  
            $this->exchange = DB_NAME;
            $this->rest_api_root = "https://api.bybit.com/";
            $this->tmp_dir = $tmp_dir;
            $this->db_name = DB_NAME;
            // print_r($this->tables);
            $this->rate_limiter = new RateLimiter(180);  // for candles 6000 / 4 = 1500, for ticks 6000 / 25 = 240
            $this->rate_limiter->SpreadFill();
            parent::__construct($symbol, $data);
        }   

        protected function  CreateWebsocket()  {          
            $rtm = $this->GetRTMLoaders();            
            if (0 == count($rtm)) return;
            $this->ws = new BybitClient (['category' => $this->category] );
            if ($this->ws->isConnected()) {
                $this->ws_active = true;
                $this->SubscribeWS();
            }
        }
       

        protected function on_ws_event(string $event, mixed $data) {
            if ('subscribe' == $event && is_array($data)) {                
                foreach ($data as $line) {                   
                    $list = explode('.', $line);
                    [$kind, $intv, $symbol] = $list;
                    $loader = $this->GetLoader ($symbol);
                    if (!is_object($loader)) {
                        log_cmsg("~C91#WS_IGNORE_SUB:~C00 no downloader  for %s: %s", $line, $symbol);
                        continue;
                    }
                    $loader->ws_sub = true;
                    unset($this->ws_sub_started[$loader->pair_id]);
                    log_cmsg("~C97#WS_SUBSCRIBED:~C00 %s", $symbol);
                }
            } elseif ('ping' == $event && isset($data->payload)) {
                $ts = format_tms($data->payload);
                if ($data->payload > time_ms() - 300000)                
                    log_cmsg("~C94#WS_PING:~C00 payload %s", color_ts($ts));
                $this->ws->send($data->payload, 'pong');
            } elseif ('pong' == $event) {
                $this->pings ++;
                if (0 == $this->pings % 500)
                    log_cmsg("~C94 #WS_PING:~C00 count = %d", $this->pings);
                $this->ws->last_ping = time();
            }
        }


        protected function SubscribeWS() {
            $keys =  array_keys($this->GetRTMLoaders());
            $already = 0;
            $added = 0;            
            $ws = $this->ws;
            $list = [];
            if (!$keys) return;            

            $intv = 'kline' == $this->ws_data_kind ? '1.' : ''; // 1m data for candles

            foreach ($keys as $pair_id) {       
                $downloader = $this->Loader ($pair_id);                                
                if ($downloader->ws_sub) {
                    $already ++;
                    continue;
                }                
                $this->ws_sub_started[$pair_id] = time();
                $downloader->ws_sub_count ++;
                if ($downloader->ws_sub_count < 5)
                    $list []= "{$this->ws_data_kind}.$intv{$downloader->symbol}";
            }
            if (0 == count($list)) return;
            
            if (is_object($ws) && $ws instanceof BybitClient && count($list) > 0) {
                log_cmsg("~C97 #WS_SUBSCRIBE~C00: already subscribed %d / %d, add = %s", $already, count($keys), json_encode($list));
                try {
                    $ws->Subscribe ($list);
                } 
                catch (Exception $E) {
                    log_cmsg("~C91#WS_EXCEPTION(Subscribe):~C00 %s", $E->getMessage());
                    $this->ws = null;
                }
            }
        }

        public function ProcessRecord(stdClass $rec) {
            $ws = $this->ws;
            if (!$ws instanceof BybitClient) return; // this only for VisualStudio PHPSense

            try {   
                
                if (isset($rec->req_id) && isset($rec->op) && !isset($rec->data)) {                    
                    $rqs = $ws->requests[$rec->req_id] ?? [];
                    $method = $rec->op;
                    $err_already = 'error:already subscribed,topic:';

                    if (isset($rec->success) && false === $rec->success && str_in($rec->ret_msg, $err_already)) {                        
                        $rec->success = true;                        
                        log_cmsg("~C31#WS_EVENT:~C00 already subscribed %s", str_replace($err_already, '', $rec->ret_msg));
                    }
                    if (isset($rec->ret_msg) && 'pong' !== $rec->ret_msg)
                        log_cmsg("~C94 #WS_RAW:~C00 %s => %s", json_encode($rec), json_encode($rqs));                   

                    if ('subscribe' == $method && $rec->success && isset($rqs['args']))
                        $this->on_ws_event('subscribe', $rqs['args']);                    
                    elseif ('pong' == $rec->ret_msg)
                        $this->on_ws_event('pong', $rec->req_id);
                    else
                        log_cmsg("~C33#WS_EVENT:~C00 unknown %s", json_encode($rec));
                }
                elseif (isset($rec->data))
                    $this->ImportDataWS($rec, 'ProcessRecord');
                else
                    log_cmsg("~C31 #WS_RECORD:~C00 unknown %s", json_encode($rec));
                
            }
            catch (Exception $E) {
                log_cmsg("~C91#EXCEPTION(ProcessRecord):~C00 %s in %s:%d from: %s", 
                            $E->getMessage(), $E->getFile(), $E->getLine(), $E->getTraceAsString());
                log_cmsg("~C93#PARAMS_DATA:~C00 %s", print_r($rec, true));
            }

        }

    }

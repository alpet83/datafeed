<?php

    include_once 'lib/common.php';
    include_once 'lib/esctext.php';
    include_once 'lib/cex_websocket.php';

    class  BinanceClient extends CEXWebSocketClient {
    
        protected $id_counter = 0;

        public   $requests = [];

        public function __construct(string $stream, array $params, mixed $config = false) {
            $url = "wss://data-stream.binance.vision/ws/$stream".http_build_query($params);
            log_cmsg ("~C97#WS_CREATE:~C00 using %s", $url);
            parent::__construct($url, $config);                                 
        }  
    
        public function ping() { 
            // not implemented, but handling for server ping
            // $this->send(time_ms(), 'ping');
        }
        public function  subscribe(array $params) {      
            $id = $this->id_counter ++;
            $params = ['method' => 'SUBSCRIBE', 'params' => $params, 'id' => $id];            
            $this->requests[$id] = $params;
            $json = json_encode($params);
            $this->send($json);   
        }
        
        public function unsubscibe(array $params) {
            $id = $this->id_counter ++;
            $params = ['method' => 'UNSUBSCRIBE', 'params' => $params, 'id' => $id];
            $this->requests[$id] = $params;
            $json = json_encode($params);
            $this->send($json);   
        }        
        
    } // class BinanceClient  

    


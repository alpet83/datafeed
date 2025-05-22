<?php

    include_once 'lib/common.php';
    include_once 'lib/esctext.php';
    include_once 'lib/cex_websocket.php';

    class  BybitClient extends CEXWebSocketClient {

        private int $req_id = 1000;
        public  $requests = [];
    
        public function __construct(array $params, mixed $config = false) {
            $cat = $params['category'] ?? 'linear';
            unset($params['category']);
            $url = "wss://stream.bybit.com/v5/public/$cat?".http_build_query($params);
            log_cmsg ("~C97#WS_CREATE:~C00 using %s", $url);
            parent::__construct($url, $config);           
        }  

        public function ping() {
            $json = sprintf('{"op": "ping", "req_id": "%d"}', $this->req_id ++);
            $this->send($json);
        }
        public function  subscribe(array $params) {                  
            $params = ['op' => 'subscribe', 'args' => $params, 'req_id' => $this->req_id];
            $this->requests [$this->req_id ++] = $params;
            $json = json_encode($params);
            $this->send($json);   
        }
        
        public function unsubscibe(array $params) {
            $params = ['op' => 'unsubscribe', 'args' => $params, 'req_id' => $this->req_id ++];
            $json = json_encode($params);
            $this->send($json);   
        }
    } // class BitMEXClient  


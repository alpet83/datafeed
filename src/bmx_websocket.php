<?php

    include_once 'lib/common.php';
    include_once 'lib/esctext.php';
    include_once 'lib/cex_websocket.php';

    class  BitMEXClient extends CEXWebSocketClient {
    
        public function __construct(array $params, mixed $config = false) {
            $url = "wss://ws.bitmex.com/realtime?".http_build_query($params);
            log_cmsg ("~C97#WS_CREATE:~C00 using %s", $url);
            parent::__construct($url, $config);           
        }  
    
        public function  subscribe(array $params) {      
            $params = ['op' => 'subscribe', 'args' => $params];
            $json = json_encode($params);
            $this->send($json);   
        }
        
        public function unsubscibe(array $params) {
            $params = ['op' => 'unsubscribe', 'args' => $params];
            $json = json_encode($params);
            $this->send($json);   
        }
    } // class BitMEXClient  


<?php
    include_once 'lib/common.php';
    include_once 'lib/esctext.php';
    include_once 'lib/cex_websocket.php';

    class  BitfinexClient extends CEXWebSocketClient {
    
        public function __construct($api = 2, $config = false) {        
            parent::__construct("wss://api-pub.bitfinex.com/ws/$api", $config);           
        }  
    
        public function  subscribe(array $params) {        
            $params['event'] = 'subscribe';        
            if (!isset($params['channel'])) {
                log_cmsg("~C91#WS_SUBSCRIBE_FAILED:~C00 no channel id");
                return;
            }
            $json = json_encode($params);
            $this->send($json);   
        }

        public function unsubscibe(array $params) {
            $params = ['event' => 'unsubscribe'];
            if (!isset($params['chanId'])) {
                log_cmsg("~C91#WS_UNSUBSCRIBE_FAILED:~C00 no channel id");
                return;
            }
            $json = json_encode($params);
            $this->send($json);   
        }
    
    } // class BitfinexClient  


<?php

 include_once('lib/common.php');
 include_once('lib/esctext.php');


 require 'vendor/autoload.php';
 use WSSC\WebSocketClient;
 use \WSSC\Components\ClientConfig;

 class  BitfinexClient extends WebSocketClient {
 
    public function __construct($api = 2, $config = false) {
        if (!$config) {
            $config = new ClientConfig();
            $config->setTimeout(2);               
        } 
        parent::__construct("wss://api-pub.bitfinex.com/ws/$api", $config);   
        if (isset($this->socket) && is_resource($this->socket)) {            
            $result = stream_set_read_buffer($this->socket, 1048576); 
            log_cmsg("~C93 #WS_CONFIG:~C00 set socket read buffer: %s", 0 == $result ? 'OK': "~C91 $result");            
        }
    }  
  
    public function  subscribe($channel, $params = false) {
        if (!$params)
            $params = array(); 
        $params['event'] = 'subscribe';
        $params['channel'] = $channel;
        $json = json_encode($params);
        $this->send($json);   
    }
    
    public function reconnect() {
        $this->connect($this->config);
    }
    public function unsubscibe($chid) {
        $params = array('event' => 'unsubscribe', 'chanId' => $chid);
        $json = json_encode($params);
        $this->send($json);   
    }

    public function unreaded() {
        if (isset($this->socket) && is_resource($this->socket)) {
            $mtd = stream_get_meta_data($this->socket);
            if ($mtd && is_array($mtd) && isset($mtd['unread_bytes']))
                return $mtd['unread_bytes'];                
        }                  
        return 0;
    }  
 
 } // class BitfinexClient  

?>
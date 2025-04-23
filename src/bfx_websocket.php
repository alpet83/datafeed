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
        if (isset($this->unread_bytes) && $this->uread_bytes > 0)
            return $this->unread_bytes;

        if (isset($this->socket) && is_resource($this->socket)) {
            $mtd = stream_get_meta_data($this->socket);
            if ($mtd && is_array($mtd) && isset($mtd['unread_bytes']))
                return $mtd['unread_bytes'];                
        }                  
        return 0;
    }  
 
 } // class BitfinexClient  

?>
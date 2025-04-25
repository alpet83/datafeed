<?php

 include_once('lib/common.php');
 include_once('lib/esctext.php');


 require 'vendor/autoload.php';
 use WSSC\WebSocketClient;
 use \WSSC\Components\ClientConfig;

 class  BitMEXClient extends WebSocketClient {
 
    public function __construct(array $params, mixed $config = false) {
        if (!$config) {
          $config = new ClientConfig();
          $config->setTimeout(30);     
        } 
        $url = "wss://ws.bitmex.com/realtime?".http_build_query($params);
        log_cmsg ("~C97#WS_CREATE:~C00 using %s", $url);
        parent::__construct($url, $config);           
        if (isset($this->socket) && is_resource($this->socket)) {            
            $result = stream_set_read_buffer($this->socket, 1048576); 
            log_cmsg("~C93 #WS_CONFIG:~C00 set socket read buffer: %s", 0 == $result ? 'OK': "~C91 $result");            
        }
    }  
  
    public function  subscribe(mixed $args = []) {      
        $params['op'] = 'subscribe';
        $params['args'] = $args;
        $json = json_encode($params);
        $this->send($json);   
    }
    
    public function reconnect() {
        $this->connect($this->config);
    }
    public function unsubscibe(mixed $args) {
        $params = array('op' => 'unsubscribe', 'args' => $args);
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

 
 } // class BitMEXClient  

?>
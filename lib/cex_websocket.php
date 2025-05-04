<?php
    require_once 'lib/esctext.php'; 
    require_once 'vendor/autoload.php';
    
    use WSSC\WebSocketClient;
    use \WSSC\Components\ClientConfig;

    $g_ws = null;


    abstract class  CEXWebSocketClient extends WebSocketClient {

        public $last_ping = 0;

        public function __construct(string $api_url, $config = false) {
            if (!$config) {
                $config = new ClientConfig();
                $config->setTimeout(30);               
            } 
            parent::__construct($api_url, $config);   
            stream_set_blocking($this->getSocket(), false);

            if (isset($this->socket) && is_resource($this->socket)) {            
                $result = stream_set_read_buffer($this->socket, 1048576); 
                log_cmsg("~C93 #WS_CONFIG:~C00 set socket read buffer: %s", 0 == $result ? 'OK': "~C91 $result");            
            }
        }  
        
        public function getSocket() {
            return $this->socket;
        }

        public function ping() {
            $this->send('ping', 'ping');
            $this->last_ping = max ($this->last_ping, time() - 10); // for prevent frequent ping
        }

        public function onPing(ConnectionContract $conn, $msg) {
            log_cmsg("~C93 #WS_PING:~C00 %s", json_encode($msg));
            $this->last_ping = time(); 
            $this->send('pong', 'pong');
        }
        public function onPong(ConnectionContract $conn, $msg) {
            log_cmsg("~C93 #WS_PONG:~C00 %s", json_encode($msg));
        }

        public function reconnect() {
            $this->connect($this->config);
        }
        abstract public function  subscribe(array $args);
        abstract public function unsubscibe(array $args); 

        public function unreaded() { 
            if (!is_resource($this->getSocket()))
                return 0; // not connected
            
            $v = stream_socket_recvfrom($this->getSocket(),  65535, STREAM_PEEK); // This can enough for check unreaded data...
            return is_string($v) ? strlen($v) : 0;                       
        }  

    } // class CEXWebSocketClient  


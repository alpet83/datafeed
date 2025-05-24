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

            for ($attempt = 0; $attempt < 5; $attempt++) 
                try {
                    parent::__construct($api_url, $config);   
                    break;
                } 
                catch (Exception $E) {
                    log_cmsg("~C91 #WS_CONNECT_FAILED:~C00 %s", $E->getMessage());                
                    sleep(10);
                }
            if (!$this->isConnected()) return;
            $sock = $this->getSocket();
            if (is_resource($sock)) {
                stream_set_blocking($sock, false);
                $result = stream_set_read_buffer($this->socket, 1048576);
                log_cmsg("~C93 #WS_CONFIG:~C00 set socket read buffer: %s", 0 == $result ? 'OK': "~C91 $result");
            }

        }  
        
        public function getSocket(): ?resource {
            return $this->socket ?? null;
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
            $sock = $this->getSocket();
            if (!is_resource($sock))
                return 0; // not connected
            
            $v = stream_socket_recvfrom($sock,  65535, STREAM_PEEK); // This can enough for check unreaded data...
            return is_string($v) ? strlen($v) : 0;                       
        }  

    } // class CEXWebSocketClient  


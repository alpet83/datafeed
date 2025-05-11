#!/usr/bin/php
<?php
    $last_exception = null;
    ob_implicit_flush();
    set_include_path(".:./lib");
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/db_config.php';
    require_once 'lib/clickhouse.php';
    require_once 'lib/rate_limiter.php';
    require_once 'ticks_proto.php';
    require_once 'bmx_websocket.php';
    require_once 'proto_manager.php';
    require_once 'bmx_dm.php';
    require_once 'vendor/autoload.php';

    $tmp_dir = '/tmp/bmx';
    define('REST_ALLOWED_FILE', $tmp_dir.'/rest_allowed.ts');    

    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time();
    if (file_exists(REST_ALLOWED_FILE)) {
        $rest_allowed_t = file_get_contents(REST_ALLOWED_FILE);
        log_cmsg("#DBG: RestAPI allowed after %s", format_TS( $rest_allowed_t));    
        while (time() < $rest_allowed_t) {
            $elps = $rest_allowed_t - time();
            log_cmsg("#WARN: start will be delayed for $elps seconds, due RestAPI BAN applied");
            flush();
            set_time_limit(60);      
            sleep(30);
        }
    }    

    file_put_contents($tmp_dir.'/ticks_dl.ts', date(SQL_TIMESTAMP)); 
    error_reporting(E_ERROR | E_WARNING | E_PARSE);
    mysqli_report(MYSQLI_REPORT_ERROR);  
    
    $manager = false;

    class BitMEXTicksDownloader extends TicksDownloader {                    
        
        protected $last_requests = [];
        
        public    $last_import = '';


        public   function   __construct(BitMEXDownloadManager $mngr, stdClass $ti) {
            $this->loader_class = 'BitMEXTicksDownloader';         
            $this->data_name = 'ticks';   
            parent::__construct($mngr, $ti);                                    
            $this->rest_api_url = 'https://www.bitmex.com/api/v1/';            
            $this->CreateTables();            
            $this->RegisterSymbol('ticks', $this->pair_id);            
            $this->volume_tolerance = 0.5;
        } // constructor

   
        public function HistoryFirst(): bool|int {            
            // получение начала истории тиков
            if ($this->history_first > 0)
                return $this->history_first;
            $params = ['symbol' => $this->symbol, 'start' => 0, 'count' => 2];
            $url = "{$this->rest_api_url}trade";
            $json = $this->api_request($url, $params, SECONDS_PER_DAY); // not ask for 24H
            $data = json_decode($json);
            if (is_array($data) && count($data) > 0) {
                $rec = $data[0];
                $min = strtotime(HISTORY_MIN_TS);
                $t_first = $this->TimeStampDecode($rec->timestamp, 1); // need seconds
                return $this->history_first = max($min, $t_first); // ограничение глубины данных в прошлое!!
            }
            return false;    
        }

        public function     ImportTicks(array $data, string $source, bool $is_ws = true): ?TicksCache {            
            $side_map = ['Sell' => false, 'Buy' => true, 'Dummy' => 2];            
            
            if (0 == count($data)) {
                log_cmsg("~C91#ERROR(ImportTicks):~C00 attempt convert empty data from source %s ", $source);
                return null;
            }            
            $filtered = [];           
            $mgr = $this->get_manager();
            $tmp_dir = $mgr->tmp_dir;
            $this->last_cycle = $mgr->cycles;
            $result = new TicksCache($this);
            $keys = [];

            foreach ($data as $rec) 
                if (isset($rec->trdMatchID) && isset($rec->timestamp) && $rec->symbol == $this->symbol) {
                    $t = $this->TimeStampDecode($rec->timestamp);
                    if ($t < EXCHANGE_START) {
                        log_cmsg("~C31#WARN(ImportTicks):~C00 timestamp outbound %s", $rec->timestamp);
                        continue;
                    }
                    if ($t === null)  {
                        log_cmsg("~C31#WARN:~C00 cannot decode timestamp %s in %s", $rec->timestamp, json_encode($rec));
                        continue;
                    }                        
                    $ts = $this->TimeStampEncode($t);                                        
                    $buy = false;
                    if (isset($rec->side)) {
                        $buy = $side_map[$rec->side] ?? 3;                        
                    }
                    else
                        log_cmsg("~C31#WARN:~C00 not set side for %s ", json_encode($rec));                        

                    $tno = $rec->trdMatchID;                                        
                    $key = str_replace('-', '', $ts.substr($tno, -10)); // compacting for sorting possiblility, hope last 10 chars of trade_no is unique for every 10000 sequential ticks                     
                    $key = str_replace(' ', '', $key);
                    $key = str_replace(':', '', $key);
                    $key = substr($key, 2);  // year 20xx => short
                    $keys []= $key;
                    $result->AddRow($t, $buy, $rec->price, $rec->size, $tno, $key);
                }
                else 
                    $filtered []= $rec;
                
            $expected = max(1, count($data) - 50);         
            if (count($result) < count($keys)) {
                file_put_contents("$tmp_dir/bad_keys_{$this->ticker}.txt", implode("\n", $keys));
                log_cmsg("~C31#WARN_OVERWRITE:~C00 %s, %d rows with same keys. Dump of keys saved ", $this->symbol, count($result) - count($keys));
            }

            if (count($data) > 500 && count($result) < $expected && count($filtered) > 0) {                
                log_cmsg("~C31#WARN:~C00 too small imported ticks for %s, from  %d filtered %d rows, first filtered: %s", 
                            $this->symbol, count($filtered), count($data), json_encode($filtered[0]));
                return null;
            }    

            $result->OnUpdate();           
            if (!$is_ws && 'WebSocket' == $source) {
                log_cmsg("~C91#WARN:~C00 trying import realtime data to cache, but source %s", $source);
                $is_ws = true;
            }                        
            if ($is_ws)                                 
                $this->SaveToDB( $result, false);                                                
            return $result;
        }        

        public function LoadTicks(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 1000): ?array {
            $url = "{$this->rest_api_url}trade";
            $params = ['symbol' => $this->symbol, 'count' => $limit, 'columns' => 'side,price,size,trdMatchID', 'filter' => '{"trdType":"Regular"}'];
            $tkey = $backward_scan ? 'endTime' : 'startTime';
            $params[$tkey] = $ts_from;
            $params['reverse'] = $backward_scan ? 'true' : 'false';
            return $this->LoadData($block, $url, $params, $ts_from, $backward_scan);
        }
       
    }

    class  TicksDownloadManager
            extends BitMEXDownloadManager {        
        public $start_cache = [];
                                     
        public $tmp_dir = '';
                

        public function __construct($symbol) {
            $this->ws_data_kind = 'trade';            
            $this->loader_class = 'BitMEXTicksDownloader';
            parent::__construct($symbol, 'ticks');                        
        } // constructor

     
        protected function SelfCheck(): bool {
            global $mysqli_df;
            $mysqli = sqli();
            if (!$mysqli || !$mysqli->ping()) {
                log_cmsg("~C91 #FAILED:~C00 connection to MySQL DB is lost, trying reconnect...");
                $mysqli = init_remote_db(DB_NAME);
                sleep(30);
                return false;
            }         
            if (!$mysqli_df || !$mysqli_df->ping()) {
                log_cmsg("~C91 #FAILED:~C00 connection to ClickHouse DB is lost, trying reconnect...");
                $mysqli_df = ClickHouseConnectMySQL();
                $mysqli_df->select_db(DB_NAME);
                sleep(30);
                return false;
            }
            
            $minute = date('i');
            if (7 == $minute % 10) {
                if (!is_object($mysqli_df->replica) || !$mysqli_df->replica->ping()) {
                    $mysqli_df->replica = ClickHouseConnectMySQL('db-remote.lan:9004');                    
                    $mysqli_df->replica->select_db(DB_NAME);
                }
            }
            return true;
        }


        protected function Loader(int $index): BitMEXTicksDownloader {
            return $this->loaders[$index];
        }

        public function VerifyRow(mixed $row): bool {
            return  is_object($row) && isset($row->trdMatchID) && isset($row->timestamp) && isset($row->price);
        }
    } // class TicksDownloadManager

    $ts_start = pr_time();
    $hour = gmdate('H');      
    $manager = null;
    RunConsoleSession('bmx');
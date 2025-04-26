#!/usr/bin/php
<?php
    $last_exception = null;

    ob_implicit_flush();
    set_include_path(".:./lib:/usr/sbin/lib");
    require_once "common.php";
    require_once 'esctext.php';
    require_once "db_tools.php";
    require_once "lib/db_config.php";
    include_once "clickhouse.php";
    require_once "rate_limiter.php";
    require_once "ticks_proto.php";
    require_once "bfx_websocket.php";
    require_once "proto_manager.php";
    require_once "bfx_dm.php";
    require_once 'vendor/autoload.php';


    define('IDX_TID',   0);
    define('IDX_MTS', 1);
    define('IDX_AMOUNT', 2);
    define('IDX_PRICE', 3);

    echo "<pre>\n";
    $tmp_dir = '/tmp/bfx';

    define('REST_ALLOWED_FILE', $tmp_dir.'/rest_allowed.ts');
    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time() + 40;

    file_put_contents($tmp_dir.'/candle_dl.ts', date(SQL_TIMESTAMP));     
    error_reporting(E_ERROR | E_WARNING | E_PARSE);    
    mysqli_report(MYSQLI_REPORT_ERROR);  
    
    ini_set('display_errors', true);
    ini_set('display_startup_errors', true);
    $manager = false;   

    class BitfinexTicksDownloader 
        extends TicksDownloader {    
        public function __construct(BitfinexDownloadManager $mgr, stdClass $ti) {            
            parent::__construct($mgr, $ti);            
            $this->CreateTables();                
            $this->RegisterSymbol('ticks', $this->pair_id);                
            $this->default_limit = 1000;
        }

        public function HistoryFirst(): bool|int {            
            // получение начала истории тиков
            $params = ['start' => 0, 'limit' => 1, 'sort' => 1];
            $url = $this->rest_api_url."trades/{$this->symbol}/hist";
            $json = $this->api_request($url, $params, SECONDS_PER_DAY);  // not ask for 24h
            $data = json_decode($json);
            /* 
                [0]	ID	int	ID of the trade
                [1]	MTS	int	Millisecond epoch timestamp
                [2]	AMOUNT	float	How much was bought (positive) or sold (negative)
                [3]	PRICE	float	Price at which the trade was executed
            */
            if (is_array($data) && count($data) > 0) {
                $rec = $data[0];
                $min = strtotime(HISTORY_MIN_TS); // not need load more
                $t_first =$rec[IDX_MTS] / 1000; // need seconds
                return max($min, $t_first); // ограничение глубины данных в прошлое!!
            }
            return false;    
        }

        public function   ImportTicks(array $data, string $source, bool $direct_sync = true): ?TicksCache {
            $rows = [];            
            if (0 == count($data)) {
                log_cmsg("~C31#WARN_SKIP_IMPORT:~C00 empty data from %s ", $source);
                return null;
            }                
            $mgr = $this->get_manager();
            $this->last_cycle = $mgr->cycles;

            $result = new TicksCache($this);
            $block = $this->last_block;                        
            foreach ($data as $n => $rec) 
                if ($mgr->VerifyRow($rec)) {
                    $t = $rec[IDX_MTS];
                    if ($t < EXCHANGE_START) {
                        log_cmsg("~C31#WARN_OUTBOUND: ~C00 tick timestamp %s < EXCHANGE_START", color_ts($t));
                        continue;
                    }                    
                    $amount = $rec[IDX_AMOUNT];                    
                    $result->AddRow($t, $amount > 0, $rec[IDX_PRICE],  abs($amount), $rec[IDX_TID]);                    
                }
                elseif ($n < 10)
                    log_cmsg("~C31#WARN_SKIP_IMPORT($n):~C00 %s from %s ", var_export($rec, true), $source);

            ksort($rows);           

            if ($direct_sync) {
                $result->Store($block);
                $this->SaveToDB($result, false);                        
            }

            return $result;
        }

        public function LoadTicks(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 10000): ?array {
            $params = ['limit' => $limit, 'sort' => $backward_scan ? -1 : 1];                
            $tkey = $backward_scan ? 'end' : 'start';
            $params[$tkey] = $this->TimeStampDecode($ts_from);                                         
            $url = $this->rest_api_url."trades/{$this->symbol}/hist";
            return $this->LoadData($block, $url, $params, $ts_from, $backward_scan);
        }

    }

    class TicksDownloadManager 
        extends BitfinexDownloadManager {


        public function __construct(string $symbol) {
            $this->loader_class = 'BitfinexTicksDownloader';                        
            $this->ws_data_kind = 'trades';
            $this->ws = null;            
            parent::__construct($symbol, 'ticks');
            $this->default_limit = 10000;
            $this->rate_limiter = new RateLimiter(15);
            $this->rate_limiter->SpreadFill();            
        }

        

        protected function ImportUpdateWS(mixed $data, string $context): int {
            global $verbose;
            $id = $data[0];

            $symbol = $this->subs_map[$id];
            $downloader = $this->GetLoader ($symbol);
            if (!$downloader) {
                log_cmsg("~C91#WS_UNKNOWN:~C00 symbol %s ", $symbol); 
                return 0 ;  // WTF???
            }

            
            if (is_string($data[1])) {                    
                $imported = 0;
                $code = $data[1];
                if ('hb' == $code) return 0;

                if ('te' == $code || 'tu' == $code) {                        
                    $tick = $data[2] ?? null;
                    if ($this->VerifyRow($tick)) {
                        $imported =  $downloader->ImportWS([$tick], "$context update-$code"); // direct sync                        
                        $downloader->ws_time_last = time();
                        $downloader->ws_loads += $imported;                       
                        if ($verbose > 3)
                             log_cmsg("~C94#WS_UPDATE:~C00 code %s = %d", $code, $imported);                            
                    }
                    else
                        log_cmsg("~C31#WS_UPDATE:~C00 not a tick?: %s", print_r($tick, true));
                }
                else  
                    log_cmsg("~C94#WS_UPDATE:~C00 unknown %s", json_encode($data[1]));
                return $imported;
            }                
            log_cmsg("~C31#WS_SKIP_UPDATE:~C00 %s %s ", $symbol, json_encode($data));                                      
            return 0;
        }

        protected function Loader(int $index): ?BitfinexTicksDownloader {
            return $this->loaders[$index];
        }            

        protected function SelfCheck(): bool {
            global $mysqli_df;
            $mysqli = sqli();
            if (!$mysqli || !$mysqli->ping()) {
                log_cmsg("~C91 #FAILED:~C00 connection to MySQL DB is lost, trying reconnect...");
                $mysqli = init_remote_db('datafeed');
                sleep(30);
                return false;
            }         
            if (!$mysqli_df || !$mysqli_df->ping()) {
                log_cmsg("~C91 #FAILED:~C00 connection to ClickHouse DB is lost, trying reconnect...");
                $mysqli_df = ClickHouseConnectMySQL();
                sleep(30);
                return false;
            }
            
            $minute = date('i');
            if (7 == $minute % 10) {
                if (!is_object($mysqli_df->replica) || !$mysqli_df->replica->ping()) {
                    $mysqli_df->replica = ClickHouseConnectMySQL('db-remote.lan:9004');                    
                }
            }
            return true;
        }
        protected function SubscribeWS() {
            $keys =  array_keys($this->loaders);
            foreach ($keys as $n_loader) {        
                $downloader = $this->Loader ($n_loader);                                    
                $params = ['symbol' => $downloader->symbol];
                log_cmsg("~C97 #WS_SUB~C00: symbol = %s", $downloader->symbol);
                if ($this->ws instanceof BitfinexClient)
                    $this->ws->subscribe('trades', $params);
            }
        } // function SubscribeWS

        public function VerifyRow(mixed $row): bool {
            return (is_array($row) && 4 == count($row) && 
                        is_int($row[IDX_MTS]) && is_numeric($row[IDX_PRICE]) && is_numeric($row[IDX_AMOUNT]));
        }
    }

    $ts_start = pr_time();
    echo ".";

    date_default_timezone_set('UTC');
    set_time_limit(15);    
    
    $db_name_active = 'nope'; 
    $symbol = 'all';

    if ($argc && isset($argv[1])) {
        $symbol = $argv[1];
        if (isset($argv[2]))
            $verbose = $argv[2];
    }  
    else
        $symbol = rqs_param("symbol", 'all');         

    $pid_file = sprintf($tmp_dir.'/ticks_dl@%s.pid', $symbol);
    $pid_fd = setup_pid_file($pid_file, 300);        
    $hour = date('H');
    $log_name = sprintf('/logs/bfx_ticks_dl@%s-%d.log', $symbol, $hour); // 24 logs rotation
    $log_file = fopen(__DIR__.$log_name, 'w');
    flock($log_file, LOCK_EX);

    if (file_exists(REST_ALLOWED_FILE)) {
        $rest_allowed_t = file_get_contents(REST_ALLOWED_FILE);
        log_cmsg("#DBG: RestAPI allowed after %s", gmdate(SQL_TIMESTAMP, $rest_allowed_t));    
        if (time() < $rest_allowed_t) {
            $elps = $rest_allowed_t - time();
            log_cmsg("#WARN: RestAPI BAN applied up to %s", color_ts($rest_allowed_t));                 
            set_time_limit(60);                      
        }
    }    

    echo ".\n";
    log_cmsg("~C97 #START:~C00 trying connect to DB...");
    $mysqli = init_remote_db(DB_NAME);
    if (!$mysqli) {
        log_cmsg("~C91 #FATAL:~C00 cannot initialze DB interface! ");
        die("ooops...\n");
    }   

    $mysqli_df = ClickHouseConnectMySQL(null, null, null, DB_NAME);  
    if ($mysqli_df)
        log_cmsg("~C93 #START:~C00 MySQL interface connected to~C92 %s@$db_name_active~C00 ", $db_servers[0] ); 
    else
        error_exit("~C91#FATAL:~C00 cannot connect to ClickHouse DB via MySQL interface! ");

    $mysqli_df->replica = ClickHouseConnectMySQL('db-remote.lan:9004', null, null, DB_NAME);
    if (is_object($mysqli_df->replica)) {
        log_cmsg("~C103~C30 #WARN_REPLICATION:~C00 %s connected", $mysqli_df->replica->host_info);        
    }        

    $mysqli->try_query("SET time_zone = '+0:00'");
    log_cmsg("~C93 #START:~C00 connected to~C92 localhost@$db_name_active~C00 MySQL"); 
    $elps = -1;  
    $hstart = floor(time() / 3600) * 3600;
    $manager = new TicksDownloadManager($symbol);
    main($manager);    
    fclose($log_file);
    flock($pid_fd, LOCK_UN);
    fclose($pid_fd);  
    system("bzip2 -f --best $log_name");
    $log_file = false;
    unlink($pid_file);
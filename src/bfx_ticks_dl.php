#!/usr/bin/php
<?php
    $last_exception = null;
    ob_implicit_flush();    
    require_once __DIR__.'/proto_manager.php';

    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/db_config.php';
    require_once 'lib/clickhouse.php';
    require_once 'lib/rate_limiter.php';
    require_once "ticks_proto.php";
    require_once "bfx_websocket.php";    
    require_once "bfx_dm.php";
    require_once 'vendor/autoload.php';


    const IDX_TID = 0;
    const IDX_MTS = 1;
    const IDX_AMOUNT = 2;
    const IDX_PRICE = 3;

    echo "<pre>\n";
    $tmp_dir = '/tmp/bfx';

    define('REST_ALLOWED_FILE', $tmp_dir.'/rest_allowed.ts');
    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time() + 40;    
    
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
            $this->default_limit = 10000;
        }

        public function HistoryFirst(): bool|int {            
            // получение начала истории тиков
            if ($this->history_first > 0)
                return $this->history_first;
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
                $t_first =$rec[IDX_MTS] / 1000; // need seconds
                log_cmsg("~C97#HISTORY_FIRST:~C00  %s for %s", color_ts($t_first), $this->symbol);
                return $this->history_first = $t_first; // ограничение глубины данных в прошлое!!
            }
            return false;    
        }

        public function   ImportTicks(array $data, string $source, bool $is_ws = true): ?TicksCache {
            $rows = [];            
            if (0 == count($data)) {
                log_cmsg("~C31#WARN_SKIP_IMPORT:~C00 empty data from %s ", $source);
                return null;
            }                

            $t_start = pr_time();
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
            $elps = pr_time() - $t_start;
            if ($elps > 1)
                log_cmsg("~C96 #PERF_IMPORT_TICKS:~C00 %1.1f seconds for %5d rows, avg speed %d / sec", $elps, count($data), count($data) / $elps);

            if ($is_ws) {
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
                        $downloader->ws_raw_data []= $tick;
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
                $mysqli_df = ClickHouseConnectMySQL(null, null, null, DB_NAME);
                sleep(30);
                return false;
            }
            
            $minute = date('i');
            if (7 == $minute % 10) {
                if (MYSQL_REPLICA && !is_object($mysqli_df->replica) || !$mysqli_df->replica->ping()) {
                    $mysqli_df->replica = ClickHouseConnectMySQL('db-remote.lan:9004', null, null, DB_NAME);                    
                }
            }
            return true;
        }
        protected function SubscribeWS() {
            $keys =  array_keys($this->GetRTMLoaders());
            $already = 0;
            $added = 0;
            foreach ($keys as $pair_id) {        
                $downloader = $this->Loader ($pair_id);                                                     
                if ($downloader->ws_sub) {
                    $already ++;
                    continue;
                }
                $params = ['channel' => 'trades', 'symbol' => $downloader->symbol];
                log_cmsg("~C97 #WS_SUB~C00: %s = %d", $downloader->symbol, $downloader->data_flags);
                if ($this->ws instanceof BitfinexClient) {
                    $this->ws->subscribe( $params);
                    $added ++;
                }
            }
            if ($added > 0)
                log_cmsg("~C97 #WS_SUBSCRIBE:~C00 %d added, already %d confirmed", $added, $already);
        } // function SubscribeWS

        public function VerifyRow(mixed $row): bool {
            return  is_array($row) && 4 == count($row) && 
                        is_int($row[IDX_MTS]) && is_numeric($row[IDX_PRICE]) && is_numeric($row[IDX_AMOUNT]);
        }
    }

    $ts_start = pr_time();    
    $hour = gmdate('H');      
    $manager = null;
    RunConsoleSession('bfx');

    
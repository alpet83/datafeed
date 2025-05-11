#!/usr/bin/php
<?php
    $last_exception = null;

    /* DISCLAIMER: Из-за огромного объема тиковых данных при малом лимите данная реализация загружает исключительно агреггированные сделки, очень похожие на тики по технической сути. 
       Реализация загрузка чистых тиков тоже возможна с небольшими переделками, но займет выгрузка истории огромное количество времени по предварительным тестам. */

    ob_implicit_flush();
    set_include_path(".:./lib");
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/db_config.php';
    require_once 'lib/clickhouse.php';
    require_once 'lib/rate_limiter.php';
    require_once 'ticks_proto.php';
    require_once 'bnc_websocket.php';
    require_once 'proto_manager.php';
    require_once 'bnc_dm.php';
    require_once 'vendor/autoload.php';


    echo "<pre>\n";
    $tmp_dir = '/tmp/bnc';


    define('REST_ALLOWED_FILE', $tmp_dir.'/rest_allowed.ts');
    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time() + 40;    

    file_put_contents("$tmp_dir/ticks_dl.ts", date(SQL_TIMESTAMP));     
    error_reporting(E_ERROR | E_WARNING | E_PARSE);    
    mysqli_report(MYSQLI_REPORT_ERROR);      
    ini_set('display_errors', true);
    ini_set('display_startup_errors', true);
    $manager = false;   

    class BinanceTicksDownloader 
        extends TicksDownloader {    
        public function __construct(BinanceDownloadManager $mgr, stdClass $ti) {            
            parent::__construct($mgr, $ti);            
            $this->CreateTables();                
            $this->RegisterSymbol('ticks', $this->pair_id);                
            $this->default_limit = 1000;
        }

        public function HistoryFirst(): bool|int {            
            // получение начала истории тиков
            if ($this->history_first > 0)
                return $this->history_first;
            $params = ['fromId' => 0, 'limit' => 1, 'symbol' => $this->symbol ];
            $url = "{$this->rest_api_url}historicalTrades";
            $json = $this->api_request($url, $params, SECONDS_PER_DAY);  // not ask for 24h
            $data = json_decode($json);
            
            if (is_array($data) && count($data) > 0) {
                $rec = $data[0];
                $min = strtotime(HISTORY_MIN_TS); // not need load more
                $t_first = $rec->time / 1000; // need seconds
                return $this->history_first = max($min, $t_first); // ограничение глубины данных в прошлое!!
            }
            return false;    
        }

        public function   ImportTicks(array $data, string $source, bool $is_ws = true): ?TicksCache {
            $rows = [];            
            if (0 == count($data)) {
                log_cmsg("~C31#WARN_SKIP_IMPORT:~C00 empty data from %s ", $source);
                return null;
            }                
            $mgr = $this->get_manager();
            $this->last_cycle = $mgr->cycles;

            $result = new TicksCache($this);
            $block = $this->last_block;                        
            if ($mgr->VerifyRow($data[0])) { // агр. тиков очень много, но они почти всегда однородные
                foreach ($data as $rec) {
                    $t = $rec->T;
                    if ($t < EXCHANGE_START) {
                        log_cmsg("~C31#WARN_OUTBOUND: ~C00 tick timestamp %s < EXCHANGE_START", color_tms($t));
                        continue;
                    }                                                   
                    $result->AddRow($t, $rec->m, $rec->p,   $rec->q, $rec->f);
                }
            }
            else
               log_cmsg("~C31#WARN_SKIP_IMPORT(0):~C00 %s from %s ", var_export($data[0], true), $source);

            ksort($rows);           
            if ($is_ws) {
                $result->Store($block);
                $this->SaveToDB($result, false);                        
            }

            return $result;
        }

        public function LoadTicks(DataBlock $block, string $ts_from, bool $backward_scan = false, int $limit = 1000): ?array {
            /* $best_id = $this->NearestTrade($ts_from);
            // из-за ограничений параметров, сканирование влево ограничено эмпирическим смещением
            if ($backward_scan)
                $best_id -= $limit;
            log_cmsg("~C93 #DBG_LOAD_TICKS({$this->ticker}):~C00 for %s detected id #%d", $ts_from, $best_id); 
            $best_id = max(0, $best_id); //*/

            $params = ['limit' => $limit, 'symbol' => $this->symbol];                
            $tk = $backward_scan ? 'endTime' : 'startTime';
            $params[$tk] = strtotime_ms($ts_from);
            $url = "{$this->rest_api_url}aggTrades";
            $res =  $this->LoadData($block, $url, $params, $ts_from, $backward_scan);
            $hdrs = $this->last_api_headers;
            preg_match('/^X-MBX-USED-WEIGHT-(\d*\S):\D*(\d*)/mi', $hdrs, $m);
            if (count ($m) > 2) {
                $ce = [];
                preg_match('/^Content-encoding: (\S+)/mi', $hdrs, $ce);
                $enc = $ce[1] ?? 'none';
                log_cmsg("~C94 #RATE_LIMIT:~C00 %s, request time %.1f s, encoding %s", $m[0], $this->last_api_rqt, $enc); 
                if ($m[2] >= 5000) { // TODO: use limit from API
                    $rl = $this->get_manager()->rate_limiter; 
                    $rl->max_rate -= 10;
                    $rl->max_rate = max($rl->max_rate, 15);
                    $rl->SpreadFill();
                }
            }
            return $res;
        }

        protected function NearestTrade(string $ts): int  {  // костыль детектирования номера сделки по времени
            if (strlen($ts) < 10) {
                log_cmsg("~C31#WARN(NearestTrade):~C00 invalid timestamp [%s]", $ts);
                return -1;
            }
            $t = strtotime_ms($ts);
            $cache = $this->cache;            
            if (!$cache instanceof TicksCache) return -2;
            if (count($cache) > 0 && $cache->Covers_ms($t)) {                
                $id = $cache->FindRow($t, true);
                $lk = $cache->lastKey();
                if ($id > 0) {                    
                    return $id;
                }                                
                elseif ($lk > 0) {                    
                    $row = $cache[$lk];                    
                    log_cmsg("~C33#WARN(NearestTrade):~C00 can't find %s in cache, return %d for %s", $ts, $id, color_tms($row[TICK_TMS]));
                    return $lk;
                }
            } 
            // по сути обращение к индексным данным самой биржи, т.к. для тиков по времени нет параметров
            $params = ['limit' => 1, 'symbol' => $this->symbol, 'startTime' => $t];            
            $url = "{$this->rest_api_url}aggTrades";  // compressed data, may be good enough instead ticks for 99% traders...
            $json = $this->api_request($url, $params, 180);  
            $data = json_decode($json);
            if (is_array($data) && count($data) > 0 && is_object($data[0])) { 
                log_cmsg("~C94 #DBG_AGGR_TRADE:~C00 for %s = %s", $ts, $json);
                return $data[0]->f;
            }
            else
                log_cmsg("~C91 #ERR_AGGR_TRADE:~C00 for %s = %s", $ts, $json);            
            return -3;
        }

    }

    class TicksDownloadManager 
        extends BinanceDownloadManager {


        public function __construct(string $symbol) {
            $this->loader_class = 'BinanceTicksDownloader';                        
            $this->ws_data_kind = 'aggTrade';            
            $this->ws = null;            
            parent::__construct($symbol, 'ticks');
            $this->default_limit = 10000;
            $this->rest_api_root = 'https://api.binance.com/api/v3/'; 
            $this->rate_limiter = new RateLimiter(180);
            $this->rate_limiter->SpreadFill();            
        }

        protected function ImportDataWS(mixed $data, string $context): int {
            $valid = is_object($data) && str_contains('aggTrade', $data->e) && isset($data->t);

            if (!$valid) {
                log_cmsg("~C31#WS_IGNORE(ImportDataWS):~C00 can't detect data %s ", json_encode($data));
                return 0;
            }

            $loader = $this->GetLoader($data->s);
            if (!$loader) {
                log_cmsg("~C31#WS_IGNORE(ImportDataWS):~C00 can't detect loader for %s ", $data->s);
                return 0;
            }
            
            $raw = new stdClass();            
            $raw->time = $data->T;
            $raw->id = $data->f;
            $raw->price = $data->p;
            $raw->qty = $data->q;
            $raw->isBuyerMaker = $data->m;
            $raw->isBestMatch = $data->M;
            return $loader->ImportWS([$raw], $context);            
        }
                

        protected function Loader(int $index): ?BinanceTicksDownloader {
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
            $keys =  array_keys($this->GetRTMLoaders());
            $already = 0;
            $added = 0;
            foreach ($keys as $pair_id) {        
                $downloader = $this->Loader ($pair_id);                                                     
                if ($downloader->ws_sub) {
                    $already ++;
                    continue;
                }
                $params = ['channel' => 'aggTrade', 'symbol' => $downloader->symbol];
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
            return is_object($row) && isset($row->T) && isset($row->f) && isset($row->p) && isset($row->q);
        }
    }

    $ts_start = pr_time();    
    $hour = gmdate('H');      
    $manager = null;
    RunConsoleSession('bnc');

    
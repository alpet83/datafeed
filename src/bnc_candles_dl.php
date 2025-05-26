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

    require_once "candle_proto.php";
    require_once "bnc_websocket.php";
    require_once "bnc_dm.php";

    $tmp_dir = '/tmp/bnc';
    define('REST_ALLOWED_FILE', "$tmp_dir/rest_allowed.ts");    

    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time();

    function chdb_conn():? ClickHouseDB\Client {
        global $chdb;
        return $chdb;
    }

    if (file_exists(REST_ALLOWED_FILE)) {
        $rest_allowed_t = file_get_contents(REST_ALLOWED_FILE);
        log_cmsg("#DBG: RestAPI allowed after %s", gmdate(SQL_TIMESTAMP, $rest_allowed_t));    
        while (time() < $rest_allowed_t) {
            $elps = $rest_allowed_t - time();
            log_cmsg("#WARN: start will be delayed for $elps seconds, due RestAPI BAN applied");
            flush();
            set_time_limit(60);      
            sleep(30);
        }
    }  
    
    error_reporting(E_ERROR | E_WARNING | E_PARSE);
    mysqli_report(MYSQLI_REPORT_ERROR);  

    class BinanceCandleDownloader extends CandleDownloader {     
        public   function   __construct(DownloadManager $mngr, stdClass $ti) {
            $this->days_per_block = 1;
            $this->default_limit = 1000; // include excess for flood            
            parent::__construct($mngr, $ti);         
            $this->blocks_at_once = 10;  // exchange can return 1000 candles with rate over 1 per second. Main limits is DB performance... but many blocks at once is also high mem usage
            $this->CreateTables();            
            $this->RegisterSymbol('candles', $this->pair_id);                       
            if (0 === stripos($ti->symbol, 'BTCUSD') || 0 === stripos($ti->symbol, 'ETHUSD'))
                $this->normal_delay = 30;
        }

        public   function    ImportCandles(array $data, string $source, bool $is_ws = true): ?CandlesCache {
            global $verbose;
            $this->last_error = '';
            $mysqli_df = sqli_df();
            $mgr = $this->manager;
            $uptime = $mgr->Uptime();
            $this->last_cycle = $mgr->cycles;

            if (!$this->table_corrected) 
                $this->CorrectTables();
                    
            if (0 == count($data)) {
                $this->last_error = 'void source data';
                return null;
            }

            $dbg_from  = time() - SECONDS_PER_DAY * 5;  // start of day
            $now = time() * 1000;
            $cnt = 0;
            $rcnt = count($data);
            $updated = 0;
            $invalid = 0;
            $strange = 0;            
            $flood = 0;  
            $dups = 0;                       
            $last_block = $this->last_block;

            $result = new CandlesCache($this);           
            $result->interval = $this->current_interval;
            $result->mark_flags = $is_ws ? CANDLE_FLAG_RTMS : 0;
            $tk = time();
            $intraday = $this->current_interval < SECONDS_PER_DAY;
            /* [
                1499040000000,      // Kline open time
                "0.01634790",       // Open price
                "0.80000000",       // High price
                "0.01575800",       // Low price
                "0.01577100",       // Close price
                "148976.11427815",  // Volume
                1499644799999,      // Kline Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "0"                 // Unused field, ignore.
            ]  */
            foreach ($data as $rec) {
                if (!$mgr->VerifyRow($rec)) {
                    $invalid ++;
                    continue;
                }

                [$tms, $open, $high, $low, $close, $volume, $ctime, $qa_vol, $trades] = $rec;
                $tk = floor($tms / 60000) * 60;
                if ($is_ws && $intraday && $tk < $last_block->max_avail) {
                    $dups ++;
                    continue;
                }

                $flags = $intraday ? 0 : $trades;

                if (isset($result[$tk]))
                    $dups ++;
                else
                    $result->AddRow($tk, $open, $close, $high, $low, $volume, $flags);
            }  

            $this->ProcessImport($result, $is_ws, $source, $updated, $rcnt, $flood, $strange);
            return $result;
        } // ImportCandles

        public function LoadCandles(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 1000): ?array {
            $url = "{$this->rest_api_url}klines";
            $intervals = [60 => '1m', 3600 => '1h', 86400 => '1d'];                        
            $intv = $intervals[$this->current_interval] ?? '1m';
            $params = ['symbol' => $this->symbol, 'interval' => $intv, 'limit' => $limit];
            $end = false;
            if ($backward_scan) {
                $end = strtotime($ts_from);
                $start = $end - $limit * $this->current_interval;  // not effective, but no way
            } else {
                $start = strtotime($ts_from);                
            }
            $params['startTime'] = $start * 1000;
            if ($end)
                $params['endTime'] = $end * 1000;

            $res = $this->LoadData($block, $url, $params, $ts_from, $backward_scan);            
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

        public function LoadDailyCandles(int $per_once = 1000, bool $from_DB = true): array|null {
            $mysqli = sqli();            
            // HATE COPYPASTE, BUT ... SORRY
            $stored = parent::LoadDailyCandles($per_once, $from_DB);            
            $table_name = "{$this->table_name}__1D";

            $after = EXCHANGE_START_SEC;
            $range = time() - $this->HistoryFirst();
            $range /= SECONDS_PER_DAY;           

            if (count ($stored) > $range * 0.9) {
                $after = array_key_last($stored);
                $after = min (floor_to_day(time()), $after);
                $after = floor_to_day($after);
            }

            $ts_from = format_ts($after);
            $orig_table = $this->table_name;            
            $updates = [];
            try {
                $candles = new CandlesCache($this); 
                $candles->interval = $this->current_interval = SECONDS_PER_DAY;
                $this->table_name = $table_name;
                $data = $this->LoadCandles($candles, $ts_from, false, $per_once);
                if (is_array($data) && count($data) > 0) {
                    $part = $this->ImportCandles($data, 'REST-API-1D', true);
                    $part->Store($candles);
                }
            }   
            finally {
                $this->current_interval = 60;
                $this->table_name = $orig_table;
            } 

            if (0 == count($candles)) 
                log_cmsg("~C91#ERROR_SERIOUS:~C00 failed load/import daily candles via %s, last err %s", 
                        $this->last_api_request, $this->last_error);                                
            else  {
                $tk = $candles->lastKey();                    
                $last = $candles->last();
                $updates = $candles->Export();                  
                log_cmsg("~C92#SUCCESS:~C00 loaded %d daily candles, interval %d, trying save to %s, lasst %s : %s", 
                        count($candles), $this->current_interval, $this->table_name, color_ts($tk), json_encode($last, JSON_NUMERIC_CHECK) );                                                                    
            }

            return $this->daily_map = array_replace($stored, $updates);

        }

    } // Class BinanceCandleDownloader


    class  CandleDownloadManager
        extends BinanceDownloadManager {

        private $last_group_dl = 0;

        public function __construct($symbol) {
            $this->ws_data_kind = 'kline_1m';
            $this->db_name = DB_NAME;
            $this->loader_class = 'BinanceCandleDownloader';            
            parent::__construct($symbol, 'candles');
            $this->default_limit = 1000;
        } // constructor

        protected function ImportDataWS(mixed $data, string $context): int {
            $valid = is_object($data) && str_contains('kline', $data->e) && isset($data->k);

            if (!$valid) {
                log_cmsg("~C31#WS_IGNORE(ImportDataWS):~C00 can't detect data %s ", json_encode($data));
                return 0;
            }
            $symbol = strtoupper($data->s);
            $loader = $this->GetLoader($symbol);
            if (!is_object($loader) ) {
                log_cmsg("~C91#WS_UNKNOWN(ImportData):~C00 symbol %s ", $symbol); 
                return 0;
            }
            // $mts = $data->E;
            $body = $data->k;
            // reformat as REST API row
            $keys = explode(', ', 't, o, h, l, c, v, T, q, n, V, q, B');
            $raw = [];
            foreach ($keys as $k)
                $raw []= $body->$k;            
            if ($loader instanceof BinanceCandleDownloader) {
                $imported =  $loader->ImportWS([$raw], $context);
                $loader->ws_loads += $imported;
                if ($imported > 0)
                    $loader->ws_time_last = time();
            }
            return 0;
        }

        protected function SelfCheck(): bool {
            return DBCheckReconnect($this);
        }
               
        protected function Loader(int $index): ?BinanceCandleDownloader {
            return $this->loaders[$index];
        }

        public function VerifyRow(mixed $row): bool {
            // MTS is int,   open, close, high, low, volume are float
            return is_array($row) && count($row) == 12 && is_int($row[0]) && is_numeric($row[5]); // MTS is int
        }
    }    

    $ts_start = pr_time();
    $hour = gmdate('H');
    $hstart = floor(time() / 3600) * 3600;     
    $manager = null;
    RunConsoleSession('bnc');
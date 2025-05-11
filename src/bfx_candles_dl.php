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
    require_once "candle_proto.php";
    require_once "bfx_websocket.php";
    require_once "proto_manager.php";
    require_once "bfx_dm.php";
    require_once 'vendor/autoload.php';


    echo "<pre>\n";
    $tmp_dir = '/tmp/bfx';

    define('REST_ALLOWED_FILE', $tmp_dir.'/rest_allowed.ts');
    $log_stdout = true;
    $verbose = 3;
    $rest_allowed_t = time();
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

    file_put_contents($tmp_dir.'/candle_dl.ts', date(SQL_TIMESTAMP));     
    error_reporting(E_ERROR | E_WARNING | E_PARSE);    
    mysqli_report(MYSQLI_REPORT_ERROR);  
    
    ini_set('display_errors', true);
    ini_set('display_startup_errors', true);
    $manager = false;   

    class BitfinexCandleDownloader 
        extends CandleDownloader {    
       

        public   function   __construct(BitfinexDownloadManager $mngr, stdClass $ti) {
            parent::__construct($mngr, $ti);
            $this->CreateTables();
            $this->RegisterSymbol('candles', $this->pair_id);
            if (strpos($this->symbol, 'BTC') || strpos($this->symbol, 'ETH'))
                $this->normal_delay = 30; 
            $this->blocks_at_once = 5;
            $this->days_per_block = 1;
            $this->default_limit = 1450; // enough less 10k, for 5 days/blocks
            $this->volume_tolerance = 0.55;
        } // constructor
 

        public   function    ImportCandles(array $data, string $source,  bool $is_ws = true): ?CandlesCache {
            global $verbose;            
            if (!is_array($data)) {
                log_cmsg("~C91 #WARN:~C00 decode failed for json data, type = %s? ", gettype($data));
                return null;
            }
            if (!$this->table_corrected) 
                $this->CorrectTables();

            if ($this->manager->VerifyRow($data)) {
                log_cmsg("~C91 #STRANGE:~C00 directly single row %s passed from %s: %s ", json_encode($data), $source, format_backtrace());                
                $data = [$data];
            }

            $mgr = $this->get_manager();
            $this->last_cycle = $mgr->cycles;

            $result = new CandlesCache($this);;      
            $result->interval = $this->current_interval;    
            $result->key = 'imported';

            $now = time_ms();
            $cnt = 0;            
            $checked = 0;
            $updated = 0;            
            $row = [];
            $errs = [];

            foreach ($data as $row) {
                if (isset($row[0]) && is_int($row[0])) 
                    $checked ++;
                else  {
                    log_cmsg("~C91#WRONG_ROW:~C00 %s from %s", json_encode($row), format_backtrace());
                    break;
                }

                $tk_ms = array_shift($row) * 1;
                $tk = $tk_ms / 1000;
                if ($tk_ms >= $now + 1000) {
                    log_cmsg("~C91 #REJECT:~C00 attempt import candle from future %s", date_ms(SQL_TIMESTAMP3, $tk_ms));
                    continue;
                }          
                $this->first_ts = 0 == $this->first_ts ? $tk_ms : min($this->first_ts, $tk_ms);          
                $this->newest_ms = max($this->newest_ms, $tk_ms);

                $tk = floor($tk / 60) * 60; // round to minutes, for using as key
                if (isset($result[$tk]))                                         
                    $updated ++;                
                else
                    $cnt ++; // detect adding new
                $row = $this->CheckCandle($tk, $row, $errs); 
                $result[$tk] = $row;                    
            }                                    
            $result->OnUpdate();

            if ('WebSocket' == $source && !$is_ws) {
                log_cmsg("~C91 #WARN:~C00 indirect import realtime data from %s", debug_backtrace());
                $is_ws = true;
            }

            $this->ProcessImport($result, $is_ws, $source, $updated, count($data));          
            return $result;
        }  // function ImportCandles         

        public function LoadCandles(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 5 * 24 * 60): ?array {
            $url = $this->rest_api_url;              
            // if ($block->recovery && 1 == $this->days_per_block && $this->current_interval < SECONDS_PER_DAY)                $limit = min(1600, $limit);
            $params = ['limit' => $limit, 'sort' => $backward_scan ? -1 : 1];
            $path = "candles/trade:1m:{$this->symbol}/";            
            if ($this->is_funding)
                $path = "candles/trade:1m:{$this->symbol}:p30/";                 
            $path .= 'hist'; //
            $tkey = $backward_scan ? 'end' : 'start';              
            $t = strtotime_ms($ts_from);   // in ms 
            $params[$tkey] = round($t / 1000) * 1000; // на практике получается ругань при указании мс
            $res = $this->LoadData($block, $url.$path, $params, $ts_from, $backward_scan);                                    
            return $res;
        }

        public function LoadDailyCandles(int $per_once = 1000, bool $from_DB = true): ?array { 


            $res = parent::LoadDailyCandles($per_once, $from_DB); // used for limiting
            // if (is_array($res) && count($res) > 0)  return $res;
            $this->SaveToDB($this->cache); // flush cache before filling            
            $url = $this->rest_api_url."candles/trade:1D:{$this->symbol}/hist";  
            $params = ['limit' => $per_once, 'sort' => 1];                 
            
            $range = time() - $this->HistoryFirst();
            $range /= SECONDS_PER_DAY;
            $cursor = EXCHANGE_START_SEC;

            if (is_array($res) && count($res) > $range * 0.9) {  // в большинстве случаев не обязательно перезагружать целиком
                $cursor = array_key_last($res);
                $cursor -= SECONDS_PER_DAY * 7;                
            }            
            $json = 'fail';            

            $orig_table = $this->table_name;
            $orig_table = str_replace('__1D', '', $orig_table);
            $table_name = "{$orig_table}__1D";

            if (!$this->CreateTables())
                throw new Exception("~C91#ERROR:~C00 failed create tables ");
            
            $map = [];
            try {
                $this->table_name = $table_name;
                $candles = new CandlesCache($this); 
                $candles->interval = $this->current_interval = SECONDS_PER_DAY;
                
                log_cmsg("~C93 #LOAD_DAILY:~C00 requesting daily candles for %s from exchange", $this->symbol);
                $attempts = 10;                

                while ($attempts -- > 0) {
                    $params['start'] = $cursor * 1000; // param in ms
                    $json = $this->api_request($url, $params, -1);
                    $data = json_decode($json);
                    if (!is_array($data) || count($data) == 0) break;
                    $part = $this->ImportCandles($data, 'REST-API-1D', false);                                    
                    $imp = count($part);                          
                    if ($imp > 0) {              
                        $part->Store($candles);       
                        $this->SaveToDB($part, true);                    
                        $candles->OnUpdate();
                        $cursor = $candles->lastKey() + SECONDS_PER_DAY; // timestamp of latest
                    }                        
                    if (time() - $cursor <= SECONDS_PER_DAY) break;    
                    if ($imp < $this->default_limit) break;            
                }            
                
                if (0 == count($candles)) 
                    log_cmsg("~C91#ERROR:~C00 failed load daily candles via %s, response = %s", 
                                $this->last_api_request, substr($json, 0, 100));                                
                else  {
                    $map = array_replace($res, $candles->Export());
                    
                    $ft = $candles->firstKey();
                    $first = $candles->first();
                    log_cmsg("~C92#SUCCESS:~C00 loaded %d daily candles, trying save to %s, first %s : %s", 
                                count($candles), $this->table_name, color_ts($ft), json_encode($first) );                
                }
            } 
            finally {
                $this->table_name = $orig_table;             
                $this->current_interval = 60;
            }           
            $tmp_dir = $this->get_manager()->tmp_dir;
            file_save_json("$tmp_dir/{$this->symbol}_candles_1d.json", $map);
            return $map;
        }
        
    } // class BitfinexCandleDownloader

    class  CandleDownloadManager
      extends BitfinexDownloadManager {
        public function __construct($symbol) {
            $this->ws_data_kind = 'trade:1m';
            $this->ws_channel = 'candles';
            $this->loader_class = 'BitfinexCandleDownloader';            
            parent::__construct($symbol, 'candles');                                  
        } // constructor       

        protected function SelfCheck(): bool {
            return DBCheckReconnect($this);
        }
               
        protected function Loader(int $index): ?BitfinexCandleDownloader {
            return $this->loaders[$index];
        }
        
        protected function SubscribeWS() {
            $keys =  array_keys($this->GetRTMLoaders());
            $already = 0;
            $added = 0;            
            $ws = $this->ws;
            foreach ($keys as $pair_id) {       
                $downloader = $this->Loader ($pair_id);                                
                if ($downloader->ws_sub) {
                    $already ++;
                    continue;
                }                
                if (is_object($ws) && $ws instanceof BitfinexClient) {
                    $key = "{$this->ws_data_kind}:{$downloader->symbol}";                     
                    $params = ['channel' => 'candles','key' => $key];                                    
                    log_cmsg("~C97 #WS_SUB_ADD~C00: symbol = %s", $downloader->symbol);    
                    $added ++;
                    $ws->subscribe($params);
                }
            }
            if ($added > 0) 
                log_cmsg("~C04~C97 #WS_SUB_TOTAL:~C00 %u pairs already subscribed, %u added", $already, $added);
        } // function SubscribeWS        

        public function VerifyRow(mixed $row): bool {
            // MTS is int,   open, close, high, low, volume are float
            return is_array($row) && count($row) == 6 && is_int($row[0]) && is_numeric($row[4]); // MTS is int
        }
    }


    $ts_start = pr_time();
    $hour = gmdate('H');
    $hstart = floor(time() / 3600) * 3600;
    echo ".";
    $manager = null;
    RunConsoleSession('bfx');
?>
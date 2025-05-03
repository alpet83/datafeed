<?php
    include_once('lib/common.php');
    include_once('lib/db_tools.php');
    require_once('proto_manager.php');


    const DL_FLAG_HISTORY  = 0x01;
    const DL_FLAG_REALTIME = 0x02;
    const VERIFY_TS_DEFAULT = '2012-01-01 00:00';

    // offsets in cache row
    const CANDLE_OPEN = 0;
    const CANDLE_CLOSE = 1;
    const CANDLE_HIGH = 2;
    const CANDLE_LOW = 3;
    const CANDLE_VOLUME = 4;     
    const CANDLE_FLAGS = 5;

    enum BLOCK_CODE: int {
        case INVALID = -1;
        case NOT_LOADED = 0;
        case VOID = 1;
        case PARTIAL = 2;
        case FULL = 3;
    }
    function compare_blocks($a, $b): int {
        $ta = $a->lbound;
        $tb = $b->lbound;
        if ($ta == $tb) {
            log_cmsg("~C91#ERROR(compare_blocks):~C00 same lbound for %s and %s", strval($a), strval($b));
            return 0;
        }
        return $ta < $tb ? 1 : -1;  
    }

    function swap_values(&$a, &$b) {
        $t = $a;
        $a = $b;
        $b = $t;
    }


    function floor_to_day(int $t): int {
        return floor($t / SECONDS_PER_DAY) * SECONDS_PER_DAY;
    }


    function verify_timestamp(float|int $t, string $comment = '', string $ref = 'VERIFY_TS_DEFAULT')  {      
        $ref_t = str_in($ref, ':') || str_in($ref, '-') ? strtotime($ref) : constant($ref);  
        $ref_t = is_string($ref_t) ? strtotime($ref_t) : $ref_t; // convert to int
        if ($t < $ref_t) 
            throw new ErrorException("~C91#ERROR:~C00 timestamp $t is before $ref ($ref_t): ".format_ts($t). " $comment");
        return $t;
    }

    function verify_timestamp_ms(int $t,  string $comment = '', string $ref = 'VERIFY_TS_DEFAULT'): int {      
        $ref_t = str_in($ref, ':') || str_in($ref, '-') ? strtotime_ms($ref) : constant($ref);  
        $ref_t = is_string($ref_t) ? strtotime_ms($ref_t) : $ref_t; // convert to int
        if ($t < $ref_t) 
            throw new ErrorException("~C91#ERROR:~C00 timestamp $t is before $ref ($ref_t): ".format_tms($t)." $comment");
        return $t;
    }

    function color_ts(mixed $ts) {
        if (null === $ts)
            return 'NULL!!!';

        $t = $ts;
        if (is_numeric($t)) {
            verify_timestamp($t, 'from color_ts', '2001-01-01 00:00:00');
            $ts = format_ts( round($t));
        }
        else     
            $t = strtotime($ts); 
        
        if (str_in($ts, '1970'))
            throw new Exception("~C91#ERROR:~C00 invalid timestamp: $ts"); 

        $today = date('Y-m-d');                
        $ex = date('Y-').date('m-', $t);
        $ts = str_replace($today, '~C96', $ts); // light cyan out only time for today        
        $ts = str_replace($ex, date('M ', $t), $ts); // current Year-month => month name 
        $ts = str_replace( 'T', ' ~C32', $ts); // remove T
        $ts = str_replace( '00:00:00.000', 'midnight', $ts);
        $ts = str_replace( '00:00:00', 'midnight', $ts);
        $ts = str_replace( '12:00:00.000', 'noon', $ts);
        $ts = str_replace( '12:00:00', 'noon', $ts);
        return trim($ts, 'Z ');
    }

    function b2s(bool $b, array $map = ['~C91failed', 'success']): string {
        return $b ? $map[1] : $map[0];
    }

    /**
     * Summary of DataBlock - класс метаданных о загружаемых блоках. По сути это временной диапазон в секундах!
     */
        


    /**
     * DataDownloader class - самый базовый класс загрузчика данных
     */
    abstract class DataDownloader {

        protected   $tables = array();
        public      $ticker = '';       // database universal pair name
        public      $exchange = '';
        public      $symbol = '';       // exchange supported pair name
        public      $pair_id = 0;       // currently assigned for symbol id, usually affected for trading.* tables

        public      $data_flags = 0;    // download options set: 1 - use REST, 2 - use WS
        public      $rest_api_url = '';
        
        public      $manager = null;   // owner class

        public      $normal_delay = 300;
        public      $rest_errors = 0;

        public      $rest_time_last = 0;

        /**  SQL table_name in datafeed DB   */
        public      $table_name = '';
        /**  SQL template filename for creating new table   */
        protected   $table_proto = ''; 
        /**  ClickHouse template filename for creating new table   */
        protected   $table_proto_ch = ''; 


        public      $time_precision = 1; // like MySQL where 1 - seconds, 3 - ms, 6 - us        

        public      $ws_data_last = 0;  // latest timestamp in WebSocket data

        public      $ws_time_last = 0;  // клиентское время приема последних данных
        public      $ws_newest = 0;     // лучшее время данных полученных через ws
        public      $ws_sub = null;    // ws subscription desc or true 
        public      $ws_channel   = 0; // id for WebSocket sub         
        public      $ws_sub_start = 0; // last subscription attempt

        public      $ws_log_file = '/dev/null';

        public      $rest_loads = 0;
        public      $ws_loads = 0;

        /** цикл в котором последний раз осуществлялся импорт любых данных */
        public      $last_cycle = 0;         

        protected   $ticker_info = null;

        public function __construct(DownloadManager $mngr, \stdClass $ti)
        {                        
            $this->tables = $mngr->GetTables(); // copy tables from manager           
            $this->exchange = $mngr->exchange;
            $this->manager = $mngr;
            $this->ticker = $ti->ticker;
            $this->symbol = $ti->symbol;
            $this->data_flags = $ti->enabled;
            $this->pair_id = $ti->pair_id;            
            $this->ticker_info = $ti;
            $this->rest_api_url = $mngr->rest_api_root;            
        }

        abstract public function db_select_value(string $column, string $params = ''): mixed;
        abstract public function db_select_row(string $column, string $params = '', int $mode = MYSQLI_OBJECT): mixed;
        abstract public function db_select_rows(string $column, string $params = '', int $mode = MYSQLI_OBJECT): mixed;

        
        public function CreateTables (): bool {
            global $chdb;                        
            $mysqli = sqli();
            $mysqli_df = sqli_df();                            
            $mgr = $this->get_manager();
            $result = true;            
            if (str_in($this->table_proto, 'mysql'))                 
                $result = $this->CreateTable($mysqli, $this->table_proto, $this->table_name);                      

            if ($mysqli != $mysqli_df && !$mysqli_df->is_clickhouse())              
                $result &= $this->CreateTable($mysqli_df, $this->table_proto, $this->table_name);            
                        
            $ch_proto = str_replace('mysql', 'clickhouse', $this->table_proto);           
            $this->table_proto_ch = $ch_proto;
            if ($mysqli_df->is_clickhouse()) 
                $result &= $this->CreateTable($mysqli_df, $ch_proto, $this->table_name); 
            elseif ($chdb)                 
                $result &= $this->CreateTable($chdb, $ch_proto, $this->table_name);             

            $query = file_get_contents('download_schedule.sql'); // make scheduler table
            $mysqli->try_query($query);
            $mysqli_df->try_query("COMMIT;\n");                            
            return $result;
        }

        public function CreateTable(mixed $conn, string $file_name, string $table_name): bool {
            $search = '@TABLENAME';           
            if ($conn instanceof mysqli_ex) {
                if ($conn->table_exists($table_name)) return true;
                return $this->ProcessTemplate($conn, $file_name, $search, $table_name);
            } 
            elseif ($conn instanceof ClickHouseDB\Client) {
                $stmt = $conn->select("SHOW TABLES LIKE '{$table_name}'");
                if ($stmt && !$stmt->isError()) {
                    if ($stmt->count() > 0) return true;
                    return $this->ProcessTemplate($conn, $file_name, $search, $table_name);
                }
            }
            return false;
        }

        public function IsHistorical() {
          return boolval($this->data_flags & DL_FLAG_HISTORY);
        }

        public function IsRealtime() {
            return boolval($this->data_flags & DL_FLAG_REALTIME);
        }

        abstract public function ImportWS(mixed $data, string $context): int;

        public function RegisterSymbol($enable_data, int $pair_id) {
            global $mysqli;
            $ticker_map = $this->tables['ticker_map'] ?? 'ticker_map';
            $table_cfg = $this->tables['config'] ?? $this->tables['data_config'];
            if (null === $table_cfg)
                throw new Exception("FATAL: not exists table config in tables map");

            $query = "INSERT INTO $ticker_map (`ticker`, `symbol`, `pair_id`)\n";
            $query .= "\tVALUES('{$this->ticker}', '{$this->symbol}', $pair_id)\n";
            $query .= "\tON DUPLICATE KEY UPDATE `symbol` = '{$this->symbol}'";
            $mysqli->try_query($query);
            $id = $mysqli->select_value('id', $ticker_map, "WHERE `ticker` = '{$this->ticker}'"); 
            if (is_int($id) && $enable_data) {
                $query = "INSERT INTO `$table_cfg` (id_ticker, load_$enable_data) VALUES($id, 3)\n";
                $query .= "ON DUPLICATE KEY UPDATE load_$enable_data = 3";
                $res = $mysqli->try_query($query);
                if (!$res)
                    log_cmsg("~C91 #ERROR:~C00 failed activate load_$enable_data for~C92 %s~C00", var_export($id, true));
              // $mysqli->try_query("UPDATE `$table_cfg` SET `load_candles` = 1 WHERE `id_ticker` = $id;");
            }
        }

        protected function ProcessTemplate(mixed $conn, string $file_name, string $search, string $replace): bool {
            if (null === $conn) {
                log_cmsg("~C31#WARN:~C00 Connection is null, can't process template from %s", $file_name);
                return false;
            }

            if (!file_exists($file_name))
                throw new Exception("FATAL: not exists template file: $file_name");                
            
            $query = file_get_contents($file_name);                
            $query = str_ireplace($search, $replace, $query);
            if ($conn instanceof mysqli_ex) {
                $result = $conn->try_query($query);
                if ($result) {
                    log_cmsg("~C93 #PROCESS_TEMPLATE_MYSQL:~C00 query from %s applied for %s ", $file_name, $replace);
                    return true;
                }
                else
                    log_cmsg ("~C91#ERROR:~C00 %s failed to process %s: %s", $conn->server_info, $query, $conn->error);

            } elseif ($conn instanceof ClickHouseDB\Client) {
                $result = $conn->write($query);
                if (is_object($result)) {
                    if ($result->isError())
                        log_cmsg ("~C91#ERROR:~C00 ClickHouse failed to process %s: %s",  $query, $result->dump());
                    else {
                        log_cmsg("~C93 #PROCESS_TEMPLATE_CLICKHOUSE:~C00 query from %s applied for %s ", $file_name, $replace);
                        return true;                        
                    }
                }
            }
            return false;
        }

        abstract public  function   RestDownload(): int;
      
        public function WSElapsed() {
          return time() - $this->ws_time_last;
        }
      
    };


    /** 
     * BlockDataDownloader class - загрузчик данных по блокам, где каждый блок имеет временной диапазон (например день или больше). 
     * Основной функционал: восстановление исторических данных в БД, через синхронную загрузку оных через REST API
     */
    



?>
#!/usr/bin/php
<?php
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_config.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/clickhouse.php';
    require_once 'lib/child_process.php';
    require_once 'lib/bitmex_common.php';

    date_default_timezone_set('UTC');

    $mysqli = init_remote_db('bitmex');    
    $mysqli_df = ClickHouseConnectMySQL(null, null, null, 'bitmex');

    if (!is_object($mysqli_df)) 
        error_exit("~C91#ERROR:~C00 cannot connect to ClickHouse DB");        
    
    class ForkProcess extends ChildProcess {
        public $day = '';
        public function __construct() {
            parent::__construct();            
            $this->cwd = __DIR__;
            $this->env_vars = getenv();
        }
        protected function OnExit() {
            global $min_tms, $max_tms, $symbols;           
            $out = $this->GetStdout();
            parent::OnExit();

            $t = strtotime_ms($this->day);            
            if (!str_in($out, '#TIMERANGE')) {
                log_cmsg("~C91#FORK_END_WARN:~C00\n %s", $out);                            
            }

            log_cmsg("~C93#FORK_END:~C00\n%s", $out);            
            // $out = str_replace("\n", ' ', $out);
            preg_match('/\[(20.*)\]..\[(20.*)\]/', $out, $matches);
            if (3 == count($matches)) {
                $min_tms = min($min_tms, strtotime_ms($matches[1]));
                $max_tms = max($max_tms, strtotime_ms($matches[2]));
                log_cmsg("~C94 #FORK_RANGE:~C00 %s .. %s ", $matches[1], $matches[2]);
            } 
            else {
                $min_tms = min($min_tms, $t);
                $max_tms = max($max_tms, $t + 86400 * 1000 - 1);                  
            }            
            preg_match('/#SYMBOLS: (.*)/', $out, $matches);
            if (2 == count($matches)) {
                $add = json_decode($matches[1], true);
                foreach ($add as $symbol)
                    $symbols[$symbol] = 1;
            }
        }
    }  // class ForkProcess  

    $min_tms = time_ms();
    $max_tms = 0;
    $symbols = [];
    $parts = [];
    
    $total_rows = 0;
    
    $tmap = $mysqli->select_map('symbol,ticker', 'ticker_map');

    function load_rows(string $file_name): array {
        $file = null;
        $gzip = str_in($file_name, '.gz');
        $file = $gzip ? gzopen($file_name, 'r') : fopen($file_name, 'r');

        if ($file === false) {
            log_cmsg("~C91#FAILED:~C00 to open file: %s", $file_name);
            return [];
        }

        $header = fgetcsv($file);
        if ($header === false) {
            log_cmsg("~C91#FAILED:~C00 to open file: %s", $file_name);            
            return [];
        }
        log_cmsg("~C96#INFO:~C00 header: %s, processing entire file %s", json_encode($header), $file_name);

        $raw = [];
        $tms = [];
        // Первая проблема архивов BitMEX: сортировка по времени в файле лишь фрагментарная: в первую очередь по символу, поэтому в БД при прямом импорте ляжет каша (относительно)
        while ($row = fgetcsv($file)) {
            if ($row === false) {                
                break;
            }            
            $row = array_map('trim', $row);
            $tms []= $row[0];
            $raw []= $row;
        }                
        if ($gzip)
           gzclose($file);    
        else
           fclose($file);           
        array_multisort($raw, SORT_ASC, $tms); 
        $tms = [];
        $file = null;        
        return $raw;
    } // load_rows

    function import_single(string $file_name): int {
        global $mysqli_df, $min_tms, $max_tms, $tmap, $symbols, $total_rows;
        if (!file_exists($file_name)) {
            log_cmsg("~C91 #ERROR:~C00 not exists %s", $file_name);
            return 0;
        }              

        
        $rows = [];
        $t = time_ms();        

        $lmin_t = $t;        
        $lmax_t = 0;
        $t_start = pr_time();     
        $f_start = $t_start; 
        
        $raw = load_rows($file_name);
        if (!$raw) return 0;
        gc_collect_cycles();  
        $usage = memory_get_usage();
        log_cmsg("~C96 #PERF:~C00 unpacked and sorted %d rows in %.3f s", count($raw), pr_time() - $t_start);        
        $t_start = pr_time();
        $id_mapper = null;
        $day = '1970-01-01';        

        foreach ($raw as $n_row => $row) {
            // 0:timestamp, 1:symbol, 2:side, 3:size, 4:price, 5:tickDirection, 6:trdMatchID, 7:grossValue, 8:homeNotional, 9:foreignNotional, 10:trdType
            // 2024-07-26D00:11:34.470133966,100BONKUSDT,Buy,37000,0.0027734,PlusTick,00000000-006d-1000-0000-0009b24ca7de,102615800,37000,102.6158,Regular 
            // needs: ts, symbol, price, amount, buy, trade_no            
            $tt = false;
            if (count($row) >= 11)
                [$timestamp, $symbol, $side, $size, $price, $tick_dir, $match_id, $gross_v, $hn, $fn, $tt] = $row;
            else
                [$timestamp, $symbol, $side, $size, $price, $tick_dir, $match_id, $gross_v, $hn, $fn] = $row;            

            $raw[$n_row] = false; // free memory fast

            if (is_string($tt) && 'Settlement' == $tt || '' == $size) {
                log_cmsg("~C94 #NOT_TICK:~C00 %s: %s ", $tt, implode(',', $row));
                continue;
            }
            $row = null;
            
            $timestamp = substr($timestamp, 0, 26); // need microseconds
            $day = substr($timestamp, 0, 10);            
            $timestamp [10] = ' '; // replace D/T with space                     
            if (null === $id_mapper)
                $id_mapper = new TradesMapper($mysqli_df, $day); // load map for day, if available

            $t = strtotime_ms($timestamp);                        
    
            $lmax_t = max($lmax_t, $t);
            $lmin_t = min($lmin_t, $t);            
            $symbols[$symbol] = 1;
            $buy = $side == 'Buy' ? 1 : 0;
            $match_id = $id_mapper->map($match_id, $t); // here real match_id will replaced by serialized
            $rows []= sprintf("('%s', '%s', %s, %s, %s, '%s')", $timestamp, $symbol, $price, $size, $buy, $match_id);
        } // while

        $raw = [];
        $usage = max($usage, memory_get_usage());

        $id_mapper->flush();
        $id_mapper = null;
        
        $elps = pr_time() - $t_start;
        $start = date_ms(SQL_TIMESTAMP3, $lmin_t);
        $end   = date_ms(SQL_TIMESTAMP3, $lmax_t);

        $added = count($rows);

        log_cmsg("~C96#PERF:~C00 loaded %d rows, decode time %5.3f s, range %s .. %s",
                        $added, $elps, $start, $end);
        $min_tms = min($lmin_t, $min_tms);
        $max_tms = max($lmax_t, $max_tms);    

        $query = "INSERT INTO `ticks__archive` (ts, symbol, price, amount, buy, trade_no) VALUES";
        $query .= implode(",\n", $rows);       

        $usage = max($usage, memory_get_usage());
        $muse = $usage / 1048576;
        
        if ($mysqli_df->query($query)) {
            $elps = pr_time() - $f_start + 0.0001;
            log_cmsg("~C93#OK:~C00 added to archive. Memory usage %.1fMiB (%.3f / row). Total time %.0f sec, rate %.1f rows / sec ", 
                        $muse, $usage / floatval($added), $elps, $added / $elps);
            $total_rows += $added;
        }
        else
            log_cmsg("~C91#FAILED:~C00 not inserted rows");
        $rows = [];
        $query = '';
        gc_collect_cycles();  
        return count($symbols);
    } // function import_single

    $file_name = $argv[1] ?? 'test.csv';    
    $start = false;
    $end = false;

    if ($argc > 3) {  // manual resync
        $start = $argv[2];
        $end = $argv[3];
        $force = true;
    }


    if (str_in($file_name, '*')) {
        $cpm = new ChildProcessManager();
        $cpm->max_processes = 2;
    
        $file_mask = $file_name;
        preg_match('/\/(20\d\d)/', $file_mask, $matches); // scaning year
        $params = '1'; 
        if (isset($matches[1])) {
            $year = $matches[1];
            $params = "toYear(ts) = $year";
        }                    
        log_cmsg("~C96 #LOAD_COUNT_MAP:~C00 processing bigdata, params: %s", $params);        
        $avail = $mysqli_df->select_map('DATE(ts),COUNT(*)', 'ticks__archive', "WHERE $params GROUP BY DATE(ts)");
        foreach ($avail as $date => $count) {
            $k = str_replace('-', '', $date);
            $avail[$k] = $count;
        }
        exec("ls $file_name",$list);        
        $t_start = $start ? strtotime($start) : 0;

        $list = array_reverse($list);
        foreach ($list as $file_name) {            
            if (!str_in($file_name, '.csv')) continue;
            preg_match('/(20\d\d\d\d\d\d)/', $file_name, $matches); // scaning date in filename
            $date = $matches[1] ?? '<no date>';
            $t = strtotime($date);
            $d = date('d', $t);
            $day = date('Y-m-d', $t);
            if (1 == $d)            
                $parts [$day] = 1;                         

            if (isset($avail[$date]) && $avail[$date] > 10000) { 
                log_cmsg("~C93 #SKIP:~C00 seems already imported %s, exists count %d", $file_name, $avail[$date]);
                if ($t_start > 0 && $t <= $t_start)
                    $min_tms = min($min_tms, $t * 1000); // range extend for resync 
                continue;
            }                          

            // reduce chance memory leak accum - using child process
            while ($cpm->BusyCount() >= $cpm->max_processes) {
                usleep(100000); // wait for free process
            }
            $proc = $cpm->Allocate("./bmx_import_csv.php $file_name nosync", 'ForkProcess');            
            $proc->day = $day;          
            if ($proc->Open()) {
                $pst = $proc->GetStatus();
                log_cmsg("~C97 #NEW_DATA:~C00 for %s checking %s with child %s", $date, $file_name, $pst['pid'] );
            }
            else
                error_exit("~C91#FATAL:~C00 cannot fork self with command %s", $proc->cmd);
            // import_single(trim($file_name));         
        } // foreach

        while ($cpm->BusyCount() > 0) {
            log_cmsg("~C93 #CHILD_WORKS:~C00 waiting for %d processes", $cpm->BusyCount());
            sleep(10); // wait for free process
        }
    }
    else
        import_single($file_name);       
    
    

    if (!$start && !$end) {
        $start = date_ms(SQL_TIMESTAMP3, $min_tms);
        $end = date_ms(SQL_TIMESTAMP3, $max_tms);
    }

    $force = false;

    log_cmsg("~C93 #TIMERANGE:~C00 [$start]..[$end] in %s rows", format_qty($total_rows)); // NO colors in [], need parse!
    if (isset($argv[2]) && str_in($argv[2], 'nosync')) {
        log_msg("#SYMBOLS: %s", json_encode(array_keys($symbols)));
        die();
    }
    
    foreach ($tmap as $symbol => $ticker) {
        if (!isset($symbols[$symbol]) && !$force) continue;
        $table_name = "ticks__{$ticker}";                        
        try {
            $code = $mysqli_df->show_create_table($table_name);
            if (!str_in($code, 'ReplacingMergeTree(ts)')) {
                log_cmsg("~C31 #TABLE_RECREATE:~C00 code is obsolete: %s", $code);
                $mysqli_df->try_query("REPLACE TABLE  $table_name ENGINE  ReplacingMergeTree(ts) ORDER BY trade_no PARTITION BY toStartOfMonth(ts)  AS SELECT * FROM $table_name");
            }
            $mysqli_df->try_query("ALTER TABLE $table_name ADD INDEX IF NOT EXISTS ts ts TYPE set(0)  GRANULARITY 16;");
            $mysqli_df->try_query("ALTER TABLE $table_name MATERIALIZE INDEX ts");

            $bounds = "ts >= '$start' AND ts <= '$end'";
            $prev_count = $mysqli_df->select_value("COUNT(*)", $table_name, "FINAL WHERE $bounds");            
            $query = "DELETE FROM $table_name WHERE $bounds "; // cleanup before
            if ($prev_count > 0) {
                $droped = 0;      
                ksort($parts);          
                $ignored = [];
                $day_ms = 86400 * 1000;
                $min_tms = floor($min_tms / $day_ms) * $day_ms;
                $last_month = date_ms('Y-m-01', $max_tms);
                $last_month_tms = strtotime_ms($last_month);
                foreach ($parts as $part => $used)  {
                    $t_part = strtotime_ms($part); // раздел занимает один месяц в БД, удаление целиком экономит десятки секунд
                    if ($t_part >= $min_tms && $t_part <= $last_month_tms) {
                        log_cmsg("~C32 #CLEANUP_DROP_PARTITION:~C00 due %s in [%s .. %s]", $part, $start, $last_month);
                        $droped += $mysqli_df->try_query("ALTER TABLE $table_name DROP PARTITION ('$part')") ? 1 : 0; // optimize deletion in mass op
                    }
                    else
                        $ignored []= $part;
                }
                
                $avail  = $mysqli_df->select_value("COUNT(*)", $table_name, "FINAL WHERE $bounds");    
                log_cmsg("~C96 #PERF_CLEANUP:~C00 removing existing data. Was droped %d / %d partitions for less writes (except %s).", 
                            $droped, count($parts), json_encode($ignored));
                if ($avail > 0 && $mysqli_df->try_query($query))
                    log_cmsg("~C94 #PERF_DELETE:~C00 slow cleanup complete, deleted %s rows", '~C95'.format_qty($avail));
            }
            log_cmsg("~C96 #PERF_INSERT:~C00 starting transfer data ticks__archive to %s ", $table_name);
            $t_start = pr_time();
            $query = "INSERT INTO $table_name SELECT ts,  price, amount, buy, trade_no FROM `ticks__archive` WHERE symbol = '$symbol' AND $bounds";
            if ($mysqli_df->try_query($query)) {
                $elps = pr_time() - $t_start;
                $info = $mysqli_df->select_row("COUNT(*) as count,SUM(amount) as volume", $table_name, "FINAL WHERE $bounds", MYSQLI_OBJECT);
                log_cmsg("~C97\t #SYNCED:~C00 elapsed %4.1f sec, ticks count in period changed %d >>> %d, saldo volume = %s ", 
                                 $elps, $prev_count, $info->count, format_qty($info->volume));
            }
            else
                log_cmsg("~C91#FAILED_SYNC:~C00 to table %s ", $table_name);
        }  catch (Exception $E) {
            log_cmsg("~C91#EXCEPTION(SYNC):~C00 Table %s, message: %s", $table_name, $E->getMessage());
        }
    } // foreach        

    $rows = $mysqli_df->select_rows("`table`, sum(bytes) AS `size`, sum(rows) AS `rows`",                                   
                                    "system.parts", "WHERE `active` AND `database` = 'bitmex' GROUP BY `table` ORDER BY `size`", MYSQLI_OBJECT);
    $info = '';
    foreach ($rows as $row) {        
        $count = format_qty($row->rows);
        $size = $row->size / 1048576;
        if ($size > 100)
            $info .= format_color ("\t\t %-32s  size %.3f MiB in %5s rows\n", $row->table, $size, $count);        
    }
    log_cmsg("~C96 #TABLES_DISK_USAGE:~C00 displaying only > 100MiB\n%s", $info);
    

    


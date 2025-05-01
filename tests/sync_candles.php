<?php
    /* Synchronization candles data from MariaDB (or any MySQL engine) to Clickhouse DB */
    include_once 'lib/common.php';
    include_once 'lib/esctext.php';
    include_once 'lib/db_tools.php';
    include_once 'lib/db_config.php';
    require_once 'vendor/autoload.php';
    require_once 'lib/clickhouse.php';

    date_default_timezone_set('UTC');
    echo '...';
    set_time_limit(60);
    while (true) {
        $log_file = fopen(__DIR__.'/logs/sync_candles.log', 'w');        
        if(!$log_file || !flock($log_file, LOCK_EX)) {
            echo '***';    
            echo format_color("~C91#WAIT:~C00 waiting for log file lock...\n");
            sleep(5);
        }
        else
            break;
    }
    echo " log file created\n";
    log_cmsg("~C97#DBG: Connecting to databases...");
    $chdb = ClickHouseConnect(CLICKHOUSE_USER, CLICKHOUSE_PASS );
    if (!is_object($chdb))
        error_exit("~C91#FATAL:~C00 can't connecto to ClickHouse\n");

    $mysqli = init_remote_db('datafeed');
    if (!$mysqli)
        error_exit("~C91#FATAL:~C00 can't connect to MariaDB\n");

    $mysqli->try_query("SET time_zone = '+0:00'");
    $exch_list = ['bitfinex', 'bitmex'];
    $table_proto = file_get_contents('clickhouse_candles_table.sql');
    // $source_proto = file_get_contents('clickhouse_candles_source.sql');

    /* // this code only works if trader have privs to create DB
    $dbc = $db_config['datafeed'];  
    list($db_user, $db_pass) = $dbc;  
    $res = $db->write("CREATE DATABASE IF NOT EXISTS mysql_datafeed ENGINE = MySQL('127.0.0.1:3306', 'datafeed', '$db_user', '$db_pass')");    
    if ($res->isError()) 
        log_cmsg("~C91#WARN~C00: ClickHouse create database failed~C93 %s", $res->info());      
    else
        $db->write("GRANT SELECT ON mysql_datafeed.* TO $db_user");
    */     
    

    $single = $renew = false; 
    if (isset($argv[1])) {
        $param = $argv[1];          
        $renew = str_in($param, 'renew');
        $single = str_in($param, 'single'); // means sync only last candle
    }
    
    $inserts = 0;
    $t_start = time();
    $t_start = floor($t_start / 300) * 300;  
    $uptime = 0;

    function ch_create_table(string $db_name, string $table_name) {
        global $chdb, $table_proto, $tables;
        if (isset($tables[$table_name])) return true;
        $table_qfn = "`$db_name`.`$table_name`";
        $query = str_replace('`@TABLENAME`', $table_qfn, $table_proto);
        try {   
            log_cmsg("~C93#QUERY:~C00 %s", $query);
            $chdb->write($query); // (RE)CREATE TABLE
        } catch (Exception $E) {
            log_cmsg("~C91##EXCEPTION~C00: ClickHouse create table/view failed~C93 %s~C00, stack:\n~C97 %s~C00", $E->getMessage(), $E->getTraceAsString());
        }        
    }
    function ch_create_view(string $time_table, string $table_qfn) {
        global $chdb;
        try {
            $query = "CREATE OR REPLACE VIEW $time_table AS SELECT ts AS `max_ts` FROM $table_qfn ORDER BY ts DESC LIMIT 1;"; // latest timestampo
            $chdb->write($query);
        }catch (Exception $E) {
            log_cmsg("~C91##EXCEPTION~C00: ClickHouse create table/view failed~C93 %s~C00, stack:\n~C97 %s~C00", $E->getMessage(), $E->getTraceAsString());
        } 
    }


    function sync_for_tickers(array $tickers) {
        global $chdb, $mysqli, $exch, $t_start, $renew, $inserts, $minute, $single;

        $stats = $mysqli->select_map('TABLE_NAME,TABLE_ROWS,DATA_LENGTH', 'information_schema.tables', "WHERE table_schema='$exch'", MYSQLI_OBJECT);

        $synced = 0;
        foreach ($tickers as $nt => $ticker) {
            $table_name = "candles__$ticker";  
            $table_qfn = "`$exch`.`$table_name`";
            $time_table = str_ireplace('candles', 'lts', $table_qfn);

            if ($renew) {
                $chdb->write("TRUNCATE TABLE IF EXISTS $table_qfn");
                log_cmsg("~C31#TRUNCATE:~C00 %23s cleanup requested", $table_qfn);
                $synced ++;
                continue;
            }

            $stat = $stats[$table_name] ?? null;            
            $total_count = is_object($stat) ? $stat->TABLE_ROWS : $mysqli->select_value('COUNT(*)', $table_qfn);                       

            $uptime = time() - $t_start;
            if ($uptime >= 250) break;
            set_time_limit(60);
            log_cmsg("~C93#SYNC_CHECK~C00: uptime %3d, processing %s (%d / %d) ", $uptime, $table_qfn, $nt, count($tickers));
            
            ch_create_table($exch, $table_name); // (RE)CREATE TABLE
            ch_create_view($time_table, $table_qfn); // (RE)CREATE VIEW

            $hard_past = $start = '2010-01-01 00:00:00';                     
            
            if (!$renew) {
                $res = $chdb->select("SELECT toTimeZone(max_ts, 'UTC') as mts FROM $time_table;");
                if ($res->count() > 0)
                    $start = max($res->fetchOne()['mts'], $start);            
            }            


            $ts_first = $mysqli->select_value('ts', $table_name, 'ORDER BY ts ASC'); // first candle in MySQL
            if ($start == $hard_past)
                $start = $ts_first;
            if ($start === null || strlen($start) < 10) {
                log_cmsg("~C91#WARN~C00: no data for sync in table %s", $table_qfn);
                continue;
            }

            $avail =  $mysqli->select_value('ts', $table_name, 'ORDER BY ts DESC');
            $data_lag = strtotime($avail) - strtotime($start);            
            $exist_count = 0;
            $stmt = $chdb->select("SELECT COUNT(*) as count FROM `$table_name`");
            if (is_object($stmt) && !$stmt->isError())  {
                $exist_count = $stmt->fetchOne('count');                              
            } 

            $tst = pr_time();        
            $query = "INSERT INTO $table_name (ts,`open`,`close`, high, low, volume)\n";            
            $query .= "\tSELECT ts,open,close,high,low,volume FROM mysql__{$exch}.$table_name\n"; 

            $full_fill = 0;
            if ($exist_count > 0)
                $full_fill = 100 * $exist_count /  $total_count;

            $incomplete = $full_fill < 90;

            if ($minute <= 1 && $data_lag < 100 && !$incomplete)  {
                $start = gmdate('Y-m-d H:i:00', strtotime($start) - 86400); // full day resync
                $data_lag = 86400;
            }                               

            $tag = '~C93#SYNC~C00';           

            $strict = '';
            if ($incomplete) {
                $tag = '~C04~C93#SYNC_FULL~C00';
                $start = $ts_first;
            }
            elseif ($single || $data_lag < 60) { // much faster                                
                $strict = "\t WHERE ts = '$avail';\n";
                $tag = '~C33#SYNC_LAST~C00';
            } else {                
                $qstart = "'$start'"; // from max ts in ClickHouse
                // AND ( uts <= toStartOfMinute(now(), 'UTC') )
                $strict = "\tWHERE ( ts >= $qstart );"; // WARN: only positive timezone UTC+xx usabe!
                $tag = '~C93#SYNC_DAY~C00';
            }

            log_cmsg("$tag: Exists %.2f%% from %d candles, requesting data from %s up to %s, lag = %d s...", $full_fill, $total_count, $start, $avail, $data_lag);                        

            $query .= $strict;
            
            if ($incomplete)
                $src_count = $total_count;
            else
                $src_count = $mysqli->select_value('COUNT(*)', $table_qfn, $strict);

            log_cmsg(". #QUERY: %s", $query);
            try { 
                $qstart = pr_time();
                $res = $chdb->write($query);                
                $elps = pr_time() - $qstart;
                $inserts ++;
                if (is_object($res) && !$res->isError()) {
                    log_cmsg("~C94#PERF~C00: for %d rows insert time %.3f sec, result: %f \n", $src_count, $elps, $res->totalTimeRequest());
                    $synced ++;
                }
                else
                    log_cmsg("~C91#FAILED:~C00 %s", $res->info());
            } catch (Exception $E) {
                log_cmsg("~C91##EXCEPTION~C00: ClickHouse insert query failed~C93 %s~C00, stack:\n~C97 %s~C00", $E->getMessage(), $E->getTraceAsString());
            }        
          
            $elps = pr_time() - $tst;
            if ($elps > 35)  break;      
        } // foreach tickers
        return $synced;
    } // sync_for_tickers


    $minute = date('i') * 1;
    foreach ($exch_list as $exch)
        try {            
            log_cmsg("~C97#PROCESSING:~C00 exchange & DB %s", $exch);
            $mysqli->select_db($exch);
            $chdb->database($exch);
            $tables = $chdb->showTables();
            $cfg = "data_config";
            $tickers = $mysqli->select_col('ticker', "ticker_map", "INNER JOIN $cfg ON $cfg.id_ticker = id WHERE load_candles >= 1 ");
            if (!is_array($tickers) || 0 == count($tickers)) {
                log_cmsg("~C31#WARN~C00: retrieve tickers failed for %s", $exch);
                continue;
            }
            $res = sync_for_tickers($tickers);
            printf(" processed %d / %d tickers for %s\n", $res, count($tickers), $exch);
            $rows = LoadQueryStats($chdb, $inserts, 1);
            if (is_array($rows))
                foreach ($rows  as $row) {                
                    if (!is_array($row)) continue;
                    $wr = $row['written_rows'];
                    $rr = $row['result_rows'];
                    $table = $row['tables'][1];
                    log_cmsg("#STAT: in %s written %d from %d rows", $table, $wr, $rr);
                }   
            else
                log_cmsg("~C91#ERROR:~C00 LoadQueryStats failed: %s", var_export($rows, true));

        } catch (Exception $E) {
            log_cmsg("~C91##EXCEPTION~C00: %s from: %s", $E->getMessage(), $E->getTraceAsString()); 
    } // foreach exch    
    
    // log_cmsg("#TABLES: %s", json_encode($tables));
    $mysqli->close();    
?>
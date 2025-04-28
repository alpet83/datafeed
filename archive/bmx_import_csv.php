<?php
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_config.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/clickhouse.php';

    date_default_timezone_set('UTC');

    $mysqli = init_remote_db('bitmex');    
    $mysqli_df = ClickHouseConnectMySQL(null, null, null, 'bitmex');

    if (!is_object($mysqli_df)) 
        error_exit("~C91#ERROR:~C00 cannot connect to ClickHouse DB");        
    
    $min_t = time_ms();
    $max_t = 0;
    $symbols = [];

    function import_single(string $file_name) {
        global $mysqli_df, $min_t, $max_t, $symbols;
        if (!file_exists($file_name)) {
            log_cmsg("~C91 #ERROR:~C00 not exists %s", $file_name);
            return;
        }
        
        $file = null;
        $file = str_in($file_name, '.gz') ? gzopen($file_name, 'r') : fopen($file_name, 'r');

        if ($file === false) {
            log_cmsg("~C91#FAILED:~C00 to open file: %s", $file_name);
            return;
        }

        $header = fgetcsv($file);
        if ($header === false) {
            log_cmsg("~C91#FAILED:~C00 to open file: %s", $file_name);            
            return;
        }

        log_cmsg("~C96#INFO:~C00 header: %s, processing entire file %s", json_encode($header), $file_name);
        $rows = [];
        $t = time_ms();
        $symbols = [];

        $lmin_t = $t;        
        $lmax_t = 0;
        $t_start = pr_time();

        while ($row = fgetcsv($file)) {
            if ($row === false) {                
                break;
            }
            $row = array_map('trim', $row);
            // 0:timestamp, 1:symbol, 2:side, 3:size, 4:price, 5:tickDirection, 6:trdMatchID, 7:grossValue, 8:homeNotional, 9:foreignNotional, 10:trdType
            // 2024-07-26D00:11:34.470133966,100BONKUSDT,Buy,37000,0.0027734,PlusTick,00000000-006d-1000-0000-0009b24ca7de,102615800,37000,102.6158,Regular 
            // needs: ts, symbol, price, amount, buy, trade_no            

            $tt = false;
            if (count($row) >= 11)
                [$timestamp, $symbol, $side, $size, $price, $tick_dir, $match_id, $gross_v, $hn, $fn, $tt] = $row;
            else
                [$timestamp, $symbol, $side, $size, $price, $tick_dir, $match_id, $gross_v, $hn, $fn] = $row;
            if (is_string($tt) && 'Settlement' == $tt || '' == $size) {
                log_cmsg("~C94 #NOT_TICK:~C00 %s: %s ", $tt, implode(',', $row));
                continue;
            }
            
            $timestamp = substr($timestamp, 0, 23);
            $timestamp [10] = ' '; // replace D/T with space            
            $t = strtotime_ms($timestamp);
            $lmax_t = max($lmax_t, $t);
            $lmin_t = min($lmin_t, $t);            
            $symbols[$symbol] = 1;
            $buy = $side == 'Buy' ? 1 : 0;
            $rows []= sprintf("('%s', '%s', %s, %s, %s, '%s')", $timestamp, $symbol, $price, $size, $buy, $match_id);
        }

        $elps = pr_time() - $t_start;
        $start = date_ms(SQL_TIMESTAMP3, $lmin_t);
        $end   = date_ms(SQL_TIMESTAMP3, $lmax_t);

        log_cmsg("~C96#PERF:~C00 loaded %d rows, decode time %5.3f s, range %s .. %s",
                        count($rows), $elps, $start, $end);
        $min_t = min($lmin_t, $min_t);
        $max_t = max($lmax_t, $max_t);    
        // $bounds = "ts >= '$start' AND ts <= '$end'";
        // $query = "DELETE FROM `ticks__archive` WHERE $bounds "; // cleanup before        
        // if ($mysqli_df->try_query($query)) log_cmsg("~C96#PREF:~C00 cleanup complete");        

        $query = "INSERT INTO `ticks__archive` (ts, symbol, price, amount, buy, trade_no) VALUES";
        $query .= implode(",\n", $rows);
        if ($mysqli_df->try_query($query))
            log_cmsg("~C93#OK:~C00 processed source %d rows", count($rows));
        else
            log_cmsg("~C91#FAILED:~C00 not inserted %d rows", count($rows));

        fclose($file);

    }

    $file_name = $argv[1] ?? 'test.csv';

    $avail = $mysqli_df->select_map('DATE(ts),COUNT(*)', 'ticks__archive', 'GROUP BY DATE(ts)');
    foreach ($avail as $date => $count) {
        $k = str_replace('-', '', $date);
        $avail[$k] = $count;
    }


    if (str_in($file_name, '*')) {
        exec("ls $file_name",$list);        
        $list = array_reverse($list);
        foreach ($list as $file_name) {            
            if (!str_in($file_name, '.csv')) continue;
            preg_match('/(20\d\d\d\d\d\d)/', $file_name, $matches); // scaning date in filename
            $date = $matches[1] ?? '<no date>';
            if (isset($avail[$date]) && $avail[$date] > 10000) { 
                log_cmsg("~C93 #SKIP:~C00 seems already imported %s, exists count %d", $file_name, $avail[$date]);
                continue;
            }                            
            else    
                log_cmsg("~C97 #NEW_DATA:~C00 for %s checking %s", $date, $file_name);
            import_single(trim($file_name));         
        }
    }
    else
        import_single($file_name);

    $start = date_ms(SQL_TIMESTAMP3, $min_t);
    $end = date_ms(SQL_TIMESTAMP3, $max_t);

    $force = false;
    if ($argc > 2) {  // manual resync
        $start = $argv[2];
        $end = $argv[3];
        $force = true;
    }

    log_cmsg("~C93 #TIMERANGE:~C00 %s .. %s", $start, $end);
    $tmap = $mysqli->select_map('symbol,ticker', 'ticker_map');
    foreach ($tmap as $symbol => $ticker) {
        if (!isset($symbols[$symbol]) && !$force) continue;
        $table_name = "ticks__{$ticker}";                        
        try {
            $t_start = pr_time();
            $bounds = "ts >= '$start' AND ts <= '$end'";
            $prev_count = $mysqli_df->select_value("COUNT(*)", $table_name, "FINAL WHERE $bounds");            
            $query = "DELETE FROM $table_name WHERE $bounds "; // cleanup before
            $mysqli_df->try_query($query); 
            $query = "INSERT INTO $table_name SELECT ts,  price, amount, buy, trade_no FROM `ticks__archive` WHERE symbol = '$symbol' AND $bounds";
            if ($mysqli_df->try_query($query)) {
                $elps = pr_time() - $t_start;
                $info = $mysqli_df->select_row("COUNT(*) as count,SUM(amount) as volume", $table_name, "FINAL WHERE $bounds", MYSQLI_OBJECT);
                log_cmsg("~C97 #SYNCED:~C00 symbol %8s => %15s in %4.1f sec, ticks count in period changed %d >>> %d, saldo volume = %s ", 
                            $symbol, $table_name, $elps, $prev_count, $info->count, format_qty($info->volume));
            }
            else
                log_cmsg("~C91#FAILED_SYNC:~C00 to table %s ", $table_name);
        }  catch (Exception $E) {
            log_cmsg("~C91#EXCEPTION(SYNC):~C00 Table %s, message: %s", $table_name, $E->getMessage());
        }
    } // foreach        
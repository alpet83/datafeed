<?php
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/db_config.php';
    require_once 'lib/clickhouse.php';
    ob_implicit_flush();
    
    $exch = rqs_param('exchange', 'bitmex');
    $data = rqs_param('data', 'candles');
    $t_flt = rqs_param('ticker', 'all');
    $force_ch = rqs_param('force_ch', 0);
    $show_month = rqs_param('show_month', false);
    $sch_date = rqs_param('schedule', null);
    $rebuild_date = rqs_param('schedule_rebuild', null);

    $force_rebuild = rqs_param('force_rebuild', 0);

    $exch = strtolower($exch);

    error_reporting(E_ERROR | E_WARNING | E_PARSE);
    mysqli_report(MYSQLI_REPORT_ERROR);  

    $mysqli = init_remote_db($exch);
    $mysqli_df = ClickHouseConnectMySQL(null, null, null, $exch);  
    if (!is_object($mysqli_df) || !$mysqli_df instanceof mysqli_ex)
        error_exit("FATAL: ClickHouse connection failed for exchange %s", $exch);

    $tmap = $mysqli->select_map('ticker,symbol', 'ticker_map');
    $load_mode = rqs_param('load_mode', null);

    $updated = '';

    function id_ticker(string $ticker) {
        global $mysqli;
        return $mysqli->select_value('id', 'ticker_map', "WHERE ticker = '$ticker'");
    }

    if (null !== $load_mode && 'all' != $t_flt) {
        $id_ticker = id_ticker($t_flt);
        if (null === $id_ticker)
            error_exit("FATAL: Ticker %s not found in ticker_map", $t_flt);

        $res = $mysqli->try_query("UPDATE data_config SET load_{$data} = $load_mode WHERE id_ticker = $id_ticker");
        $updated = $res ? "OK: config updated for $t_flt to $load_mode" : "FAILED: config update for $t_flt to $load_mode";
    }


    $tables = ('ticks' == $data || $force_ch) ? $mysqli_df->show_tables($exch, "{$data}__%") : $mysqli->show_tables($exch, "{$data}__%");

    if (!is_array($tables) || 0 == count($tables))
        error_exit("FATAL: No %s tables found for exchange %s", $data, $exch);

    $details_url = $_SERVER['PHP_SELF'] . "?exchange=$exch&data=$data&ticker=$t_flt&force_rebuild=$force_rebuild&show_month="; 
    $sched_url = "$details_url$show_month&schedule";     
    $scheduled = false;

    function schedule_task(string $ticker, string $kind, string $date){
        global $mysqli, $sch_date, $scheduled;
     
        $tv =  rqs_param('target_vol', null) ?? $mysqli->select_value('volume', "candles__{$ticker}__1D", "WHERE DATE(ts) = '$sch_date'") ?? 1;
        $query = "INSERT IGNORE INTO download_schedule (ticker, kind, date, target_volume) VALUES\n";
        $query .= "('$ticker', '$kind', '$date', $tv)";
        $scheduled = $mysqli->try_query($query);
        $added = $mysqli->affected_rows;
        $query = "DELETE FROM download_history WHERE ticker = '$ticker' AND kind = '$kind' AND date = '$date'";  // removing blockade
        if ($scheduled)
            $mysqli->try_query($query);
        return $added;
    }

    if ('all' !== $t_flt && ($sch_date || $rebuild_date) && in_array($data, ['candles', 'ticks']))  {
        $date = $sch_date ?? $rebuild_date;
        $kind = $rebuild_date ? 'rebuild-c' : $data;
        schedule_task($t_flt, $kind, $date);
    }
?>
<!DOCTYPE html>
<HTML>
  <HEAD>
    <TITLE>Datafeed Report</TITLE>
    <STYLE type="text/css">
        td, th { padding-left: 4pt;
           padding-right: 4pt; 
           text-shadow: 1px 1px 3px #202020;
        } 

        table { 
                border-collapse: collapse;
        }        
        .dark-font td { color: #0a0a01; }
        .light-font td { color: #fefede; }

        .ra { text-align: right; }
        .error { color: red; }
        .microtext {
                font-size: 8pt;
            font-family: 'Arial';
        }
    </STYLE>
    <SCRIPT type="text/javascript">
        function details(date) {            
            <?php print "url = '$details_url';\n"; ?>;
            document.location = url + date;
        }
        function schedule(date, target_vol) {            
            <?php print "url = '$sched_url';\n"; ?>;
            document.location = url + '=' + date + '&target_vol=' + target_vol;
        }
        function schedule_rebuild(date) {            
            <?php print "url = '$sched_url';\n"; ?>;
            document.location = url + '_rebuild=' + date;
        }

    </SCRIPT>
  </HEAD>
  <BODY>
    <?php
        function print_return(string $sub = '') {
            global $exch, $data;
            printf ("<input type='button' value='Return' onclick=\"document.location='%s?exchange=%s&data=%s%s'\">\n", $_SERVER['PHP_SELF'], $exch, $data, $sub);
        }
        
        echo "'\t<H1>Datafeed Report</H1>";
        $conn = 'ticks' == $data || $force_ch ? $mysqli_df : $mysqli;  
        $vcol = 'ticks' == $data ? 'amount' : 'volume';                    

        printf("DB Server: %s @ %s<br/>\n", $mysqli->server_info, $mysqli->host_info);
        print "$updated\n";
        if ($scheduled && $sch_date)
            printf("<h3>Scheduled %s for %s</h3>\n", $data, $sch_date);

        if ('all' !== $t_flt && $show_month) {
            $ticker = $t_flt;            
            $table_name = "{$data}__$ticker";
            if (!$conn->table_exists($table_name))
                error_exit("FATAL: Table %s not exists", $table_name);
            $m_start = strtotime($show_month);
            $m_end = strtotime("+1 month", $m_start);
            $ds_start = date('Y-m-d', $m_start);
            $ds_end = date('Y-m-d', $m_end);
            $strict = "ts >= '$ds_start 00:00:00' AND ts < '$ds_end 00:00:00'";
            $check = $mysqli->select_map('DATE(ts) as date,volume', "candles__{$ticker}__1D", "WHERE $strict AND volume > 0");
            $params = "WHERE ticker = '$ticker' AND kind = '$data' AND date >= '$ds_start' AND date < '$ds_end'";
            $sched = $mysqli->select_map('date, target_volume', 'download_schedule', $params);
            
            $final = $conn->is_clickhouse() ? 'FINAL' : '';            
            $map = $conn->select_map("DATE(ts) as date, COUNT(*) as count, SUM($vcol) as volume", $table_name, "$final WHERE $strict GROUP BY DATE(ts)", MYSQLI_OBJECT);
            echo "\t <H2>Ticker: <a href='$url'>$ticker</a></H2>";
            echo "\t  <TABLE border=1>\n";
            echo "<TR><th>Date</th><th>Count<th>Volume<th>Target Volume<th>Actions\n";
            $saldo_cnt = 0;
            $saldo_vol = 0;
            $saldo_tgt = 0;
            $mix = array_replace($check, $map);            
            ksort($mix);
            $shcount = 0;

            foreach ($mix as $date => $value) {
                $vol = 0;
                $count = 0;
                if (isset($map[$date])) {
                    $count = $map[$date]->count;
                    $vol = $map[$date]->volume;
                }                
                $tv = $check[$date] ?? 0;  

                $bgc = 'Canvas';
                if ($vol >= $tv)
                    $bgc = 'Green';
                elseif ($vol >= $tv * 0.995)
                    $bgc = 'LightGreen';                
                elseif ($tv > 0)
                    $bgc = 'LightRed';

                if (isset($sched[$date])) {
                    $bgc = '#FFFF80'; 
                    $tv = $sched[$date];
                }

                $saldo_cnt += $count;
                $saldo_vol += $vol;
                $saldo_tgt += $tv;

                printf ("<TR style='background-color:$bgc;'><TD>$date<TD>%s<TD>%s<TD>%s\n", 
                        format_qty($count), format_qty($vol), format_qty($tv));
                print "\t<TD>";
                $incomplete = $vol < $tv * 0.9999;
                $excess     = $vol * 0.9999 > $tv;

                if ($incomplete)                    
                    printf ("<input type='button' value='Reload' onClick='schedule(\"%s\", %f)' />&nbsp;", $date, max($tv, $vol));
                if ($excess || 'candles' == $data && $incomplete) {
                    if ($force_rebuild)
                        $shcount += schedule_task($ticker, 'rebuild-c', $date);
                    else
                        printf ("<input type='button' value='Rebuild' onClick='schedule_rebuild(\"%s\")' />", $date);
                }

                print "</TD></TR>\n";
            }
            printf("<TR><TD>Saldo<TD>%s<TD>%s<TD>%s\n", format_qty($saldo_cnt), format_qty($saldo_vol), format_qty($saldo_tgt));
            echo "</TABLE>\n";
            if ($shcount > 0)
                printf("<h3>Scheduled %d rebuild tasks</h3>\n", $shcount);

            $next_m = strtotime("+1 month", $m_start);            
            print_return("&ticker=$ticker");
            printf("<input type='button' value='Next month' onClick='details(\"%s\")'>\n", date('Y-m', $next_m));  
            $details_url = str_replace('rebuild=0', 'rebuild=1', $details_url).'&show_month='.$show_month;
            if (!$force_rebuild)
                printf("<input type='button' value='Rebuild candles' onClick='document.location=\"%s\"'>\n", $details_url);  
            die('');
        }

        $void = array_pad([], 12, ['-', 0, 0, 0, false]);

        foreach ($tables as $table_name) { 
            $ticker = str_replace("{$data}__", '', $table_name);
            if (str_contains($ticker, "_")) continue;       
            if ('all' != $t_flt && $t_flt != $ticker) continue;
            
            $check = $mysqli->select_map('DATE(ts) as date,volume', "candles__{$ticker}__1D");
            $params = "WHERE ticker = '$ticker' AND kind = '$data'";
            $sched = $mysqli->select_map('date, target_volume', 'download_schedule', $params);
            
            // printf("<!-- $params: %s -->\n", json_encode($sched));

            $total_rows = 0;
            $total_volume = 0;

            if ($conn->is_clickhouse()) 
                $map = $conn->select_map("_partition_id as date, COUNT(*) as count, SUM($vcol) as volume", $table_name, 'FINAL GROUP BY _partition_id', MYSQLI_OBJECT);
            else
                $map = $conn->select_map("DATE_FORMAT(ts, '%Y%m') as date, COUNT(*) as count, SUM($vcol) as volume", $table_name, 'GROUP BY DATE_FORMAT(ts, "%Y%m")', MYSQLI_OBJECT);
            ksort($map);
            $years = [];        
            

            // Цикл по месячным блокам!    
            foreach ($map as $date => $r) { 
                $year = substr($date, 0, 4);
                $month = substr($date, 4, 2); // date without -
                if (!isset($years[$year]))
                    $years[$year] = $void;                
                $target_vol = 0;                
                $fill_pp = 100;
                $scheduled = 0;
                $total_rows += $r->count;
                $total_volume += $r->volume;
                $dts = "$year-$month";
                foreach ($sched as $date => $sv) 
                    if (str_contains($date, $dts)) 
                        $scheduled += $sv;
                foreach ($check as $date => $tv) 
                    if (str_contains($date, $dts)) 
                        $target_vol += $tv;

                if ($target_vol > 0)
                    $fill_pp = 100 * $r->volume / $target_vol;   
                
                $years[$year][$month - 1] = [format_qty($r->count), $r->volume, $target_vol, $fill_pp, $scheduled];            
            }           

            foreach ($check as $ts => $vol) {
                $year = substr($ts, 0, 4);
                $month = substr($ts, 5, 2); // date with -
                if (!isset($years[$year]))
                    $years[$year] = $void;
                $det = $years[$year][$month - 1];
                if ('-' === $det[0] || 0 === $det[0]) {
                    $det[0] = 0;
                    $det[1] = 0; 
                    $det[2] += $vol;
                    $det[3] = 0;
                    $det[4] = false;
                }
                $years[$year][$month - 1] = $det;
            }

            $id = id_ticker($ticker) ?? 0;
            $mode = 0;
            if ($id > 0)
                $mode = $mysqli->select_value("load_$data", 'data_config', "WHERE id_ticker = $id") ?? 0;

            $url = $_SERVER['PHP_SELF'] . "?exchange=$exch&data=$data&ticker=$ticker";
            echo "\t <H2>Ticker: <a href='$url'>$ticker</a> load mode $mode</H2>";
            echo "\t  <TABLE border=1>\n";
            echo "<TR><th class='ra'>Year</th>";
            $t = strtotime('2020-01-01');
            for ($i = 0; $i < 12; $i++) {
                $month = date('M', $t);
                echo "<th class='ra'>$month";
                $t = strtotime("+1 month", $t);
            }

            ksort($years);

            foreach ($years as $year => $row) {
                printf("\t\t<TR><TD>%s\n", $year);
                foreach ($row as $month => $details) {
                    [$count, $fv, $tv, $filled, $shv] = $details;                                       
                    $bgc = 'Canvas';        
                    $filled = round($filled, 1);

                    $text = $fv > 0 ? sprintf('%s: %s / %s = %.1f%% ', 
                                              $count, format_qty($fv), format_qty($tv), $filled) : $count;
                    if ($filled >= 100.1)
                        $bgc = 'Green';
                    elseif ($filled == 100) {
                        $bgc = 'LightGreen'; // 100% ideal!
                        $text = $fv > 0 ? sprintf('%s: %s = %.1f%% ', 
                                              $count, format_qty($fv), $filled) : $count;
                    }
                    elseif ($filled > 99)
                        $bgc = '#c0ffc0'; 
                    elseif ($filled <= 99 && $tv > 0) {
                        $bgc = 'LightCoral'; // less than 90%
                        $text = sprintf('%.1f%% @ %s', $filled, format_qty($tv));
                    }
                    if ($shv)
                        $bgc = '#FFFF80'; // some scheduled
                    $month_ts = sprintf("$year-%02d", $month + 1);
                    print  "<TD style='background-color:$bgc' onClick='details(\"$month_ts\")'>$text";
                }
            }
            echo "\t  </TABLE>\n";
            $last_part = array_key_last($map);
            $year = substr($last_part, 0, 4);
            $params = $conn->is_clickhouse() ? "WHERE _partition_id = '$last_part'" : "PARTITION (p$year)";
            $last_ts = $conn->select_value('MAX(ts)', $table_name, $params);
            printf("<h3>Total rows %s, volume %s, last timestamp %s</h3>\n", 
                    format_qty($total_rows), format_qty($total_volume), $last_ts);
            if ($ticker == $t_flt) {
                printf("<!-- id_ticker %d, mode: %d -->\n", $id, $mode);                
                printf(" <form method='GET' action='%s'>\n", $_SERVER['PHP_SELF']);
                printf("\t<input type='hidden' name='exchange' value='%s'>\n", $exch);                
                printf("\t<input type='hidden' name='data' value='%s'>\n", $data);
                printf("\t<input type='hidden' name='ticker' value='%s'>\n", $ticker);
                printf("\t<input type='hidden' name='force_ch' value='%d'>\n", $force_ch);

                $opts = [
                    '0' => 'No download',
                    '1' => 'Load history',
                    '2' => 'Load latest',
                    '3' => 'Load both',
                    '7' => 'Repair missed',
                   '15' => 'Repair with replace'
                ];

                print "\t<select name='load_mode' id='mode_selector'>\n";
                foreach ($opts as $val => $desc) {
                    $selected = ($val * 1 == $mode) ? "selected='selected'" : '';
                    print "\t <option value='$val' $selected>$desc</option>\n";
                }                
                print "\t</select>\n";
                print "\t<input type='submit' value='Set'>\n";
                print "\t</form>\n";

                print_return();
            }                                
            flush();
        }
    ?>

  </BODY>
</HTML>




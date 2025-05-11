<?php
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/db_config.php';
    require_once 'lib/clickhouse.php';
    ob_implicit_flush();
    
    $exch = rqs_param('exchange', 'bitmex');
    $data = rqs_param('data', 'candles');
    $force_ch = rqs_param('force_ch', 0);

    $exch = strtolower($exch);

    $mysqli = init_remote_db($exch);
    $mysqli_df = ClickHouseConnectMySQL(null, null, null, $exch);  
    if (!is_object($mysqli_df) || !$mysqli_df instanceof mysqli_ex)
        error_exit("FATAL: ClickHouse connection failed for exchange %s", $exch);

    $tmap = $mysqli->select_map('ticker,symbol', 'ticker_map');


    $tables = ('ticks' == $data || $force_ch) ? $mysqli_df->show_tables($exch, "{$data}__%") : $mysqli->show_tables($exch, "{$data}__%");

    if (!is_array($tables) || 0 == count($tables))
        error_exit("FATAL: No %s tables found for exchange %s", $data, $exch);
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
  </HEAD>
  <BODY>
    <?php
        echo "'\t<H1>Datafeed Report</H1>";
        $void = array_pad([], 12, ['-', 0, 0, 0, false]);

        foreach ($tables as $table_name) { 
            $ticker = str_replace("{$data}__", '', $table_name);
            if (str_contains($ticker, "_")) continue;          
            $check = $mysqli->select_map('ts,volume', "candles__{$ticker}__1D");
            $sched = $mysqli->select_map('date,target_volume', 'download_schedule', "WHERE ticker = '$ticker' AND kind = '$data'");
            $conn = 'ticks' == $data || $force_ch ? $mysqli_df : $mysqli;  
            $vcol = 'ticks' == $data ? 'amount' : 'volume'; 
            printf("<!-- %s -->\n", print_r($sched, true));

            if ($conn->is_clickhouse()) 
                $map = $conn->select_map("_partition_id as date, COUNT(*) as count, SUM($vcol) as volume", $table_name, 'GROUP BY _partition_id', MYSQLI_OBJECT);
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
                $tv = $check[$date] ?? 0;
                $fill_pp = 100;
                if ($tv > 0)
                    $fill_pp = 100 * $r->volume / $tv;   

                $scheduled = 0;
                $dts = "$year-$month";
                foreach ($sched as $date => $sv) 
                    if (str_contains($date, $dts)) 
                        $scheduled += $sv;

                $years[$year][$month - 1] = [format_qty($r->count), $r->volume, $tv, $fill_pp, $scheduled];
            }
            foreach ($check as $date => $vol) {
                $year = substr($date, 0, 4);
                $month = substr($date, 5, 2); // date with -
                if (!isset($years[$year]))
                    $years[$year] = $void;
                $det = &$years[$year][$month - 1];
                if ('-' === $det[0] || 0 === $det[0]) {
                    $det[0] = 0;
                    $det[1] = 0;
                    $det[2] += $vol;
                    $det[3] = 0;
                    $det[4] = false;
                }
            }

            echo "\t <H2>Table: $table_name</H2>";
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
                    $text = $fv > 0 ? sprintf('%s = %s', $count, format_qty($fv)) : $count;
                    if ($filled > 100)
                        $bgc = 'Green';
                    elseif (100 == $filled && $fv > 0)
                        $bgc = 'LightGreen'; // 100% ideal!
                    elseif ($filled < 90 && $tv > 0) {
                        $bgc = 'LightCoral'; // less than 90%
                        $text = sprintf('%.1f%% @ %s', $filled, format_qty($tv));
                    }
                    if ($shv)
                        $bgc = '#FFFF80'; // some scheduled
                    printf("<TD style='background-color:$bgc'>%s", $text);
                }
            }
            echo "\t  </TABLE>\n";
            flush();
        }
    ?>

  </BODY>
</HTML>



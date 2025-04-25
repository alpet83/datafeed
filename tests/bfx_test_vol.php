<?php    
    require_once 'lib/common.php';
    require_once 'lib/esctext.php'; // needed for log_cmsg, works as printf but with colors    
    
    date_default_timezone_set('UTC');

    // 0: MTS, 1: open, 2: close, 3: high, 4: low, 5: volume

    $symbol = $argv[1] ?? 'tADAUSD';
    $date = $argv[2] ?? '2023-03-18';    
    $start = strtotime("$date 00:00:00Z");
    $start_ms = $start * 1000;
    $end_ms = ($start + 86400) * 1000 - 1;       
    $url = "https://api.bitfinex.com/v2/candles/trade:1m:$symbol/hist?limit=1500&sort=1&start=$start_ms"; 
    $json = file_get_contents($url);
    if ($json === false) {
        log_cmsg("~C91 #ERROR:~C00 fetching data from API\n");
        die();
    }
    $data = json_decode($json, true);
    $count = 0;
    $skipped = 0;
    $latest = 0;
    $minutes = [];
    $hours = [];
    $hsaldo = array_pad([], 24, 0);        

    $m_close = [];

    $h_close []= array_pad([], 24, 0);            
    

    foreach ($data as $rec) {
        $mts = $rec[0];        
        $rt = $mts - $start_ms; // relative time
        $tk = floor($rt / 60000);
        $hk = floor($tk / 60);       
        if ($hk > 23) continue; 

        $latest = max($latest, $mts);        
        $mv = $rec[5];
        $count ++;               
        $minutes [$tk] = $mv;        
        $hsaldo [$hk] += $mv;    
        $m_close [$tk]= $rec[2];
    }       

    $url = "https://api.bitfinex.com/v2/candles/trade:1h:$symbol/hist?limit=24&sort=1&start=$start_ms&end=$end_ms"; 
    $json = file_get_contents($url);
    if ($json === false) {
        log_cmsg("~C91 #ERROR:~C00 fetching data from API\n");
        die();
    }
    $data = json_decode($json, true);
    $accum_diff = 0;
    foreach ($data as $rec) {
        $mts = $rec[0];        
        $rt = $mts - $start_ms; // relative time        
        $tk = floor($rt / 60000); // minute key
        $hk = floor($tk / 60);    // hour key
        if ($hk > 23) continue; 

        $hv = $rec[5];
        $hours [$hk] = $hv;
        $diff = $hv - $hsaldo[$hk];
        $accum_diff += $diff;
        $diff_pp = 100 * $diff / max($hv, $hsaldo[$hk], 0.1);
        $tag = abs($diff_pp) > 0.1 ? '~C31 #MISTMATCH:' : '~C92 #MATCHED:  ';   
        $h_close[$hk] = $rec[2];
        $close = 0;

        for ($m = 59; $m > 0; $m--)
            if (isset($m_close[$tk + $m])) {
                $close = $m_close[$tk + $m];
                break;
            }

        $eqs = $close == $h_close[$hk] ? '~C97==~C00' : '~C31!=~C00';     
        log_cmsg("$tag ~C00 hour %2d, diff %-11s = %.3f%% volume = %5s, close minutes = %f $eqs %f hour", 
                        $hk, format_qty(abs($diff)), $diff_pp, format_qty($hv), $close, $h_close[$hk]);        
    }
    log_cmsg(" hours vs minutes accumulated diff %s", format_qty($accum_diff));

    $mv_sum = array_sum($minutes);
    $hv_sum = array_sum($hours);

    $ts = date('Y-m-d H:i', $start);
    log_cmsg("~C97#RESULT_SALDO:~C00 after %s symbol %s in %d minute candles total volume = %s. In %d hour candles total volume = %s. Latest candle at %sZ", 
                $ts, $symbol, $count, format_qty($mv_sum), count($hours), format_qty($hv_sum),
                date_ms('Y-m-d H:i:s', $latest));

    $url = "https://api.bitfinex.com/v2/candles/trade:1D:$symbol/hist?limit=1&sort=1&start=$start_ms";
    $json = file_get_contents($url);
    if ($json === false) {
        log_cmsg("~C91 #ERROR:~C00 fetching daily data from API\n");
        die();
    }    
    $data = json_decode($json);
    $last = array_pop($data);
    if (is_array($last)) {
        $dv = $last[5];
        $diff = $dv - $mv_sum;
        $diff_pp = 100 * $diff / max($dv, $mv_sum, 0.1);
        $tag = abs($diff_pp) > 0.1 ? '~C31 #MISTMATCH' : '~C92 #MATCHED';
        log_cmsg("$tag:~C00 for minutes saldo diff %s = %.3f%% daily volume = %s, close = %f",
                           format_qty($diff), $diff_pp, format_qty($dv), $last[2]);

        $diff = $dv - $hv_sum;
        $diff_pp = 100 * $diff / max($dv, $hv_sum, 0.1);
        $tag = abs($diff_pp) > 0.1 ? '~C31 #MISTMATCH' : '~C92 #MATCHED';
        log_cmsg("$tag:~C00 for hours saldo diff %s = %.3f%% daily volume = %s", format_qty($diff), $diff_pp, format_qty($dv));
    }

    







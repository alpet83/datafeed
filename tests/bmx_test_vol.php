<?php    
    require_once 'lib/common.php';
    require_once 'lib/esctext.php'; // needed for log_cmsg, works as printf but with colors    
    
    $symbol = $argv[1] ?? 'XBTUSD';
    $date = $argv[2] ?? '2023-10-01';
    $end = $date. ' 00:00:00';

    $start = strtotime($date) - 86400 + 60;
    $start = date('Y-m-d H:i:s', $start);
    $url = "https://www.bitmex.com/api/v1/trade/bucketed?binSize=1m&symbol=$symbol&startTime=$start&endTime=$end&count=1500";
    $json = file_get_contents($url);
    if ($json === false) {
        log_cmsg("~C91 #ERROR:~C00 fetching data from BitMEX API\n");
        die();
    }
    $data = json_decode($json);
    $vol = 0;
    $count = 0;
    $skipped = 0;
    
    foreach ($data as $rec) {
        if (isset($rec->volume)) {
            $vol += $rec->volume;
            $count ++;
        }    
        else    
            $skipped ++;
    }   

    log_cmsg("~C97#RESULT_SALDO:~C00 Time range %s .. %s for symbol %s. In  %d records total volume = %s. Skipped %d records \n ", 
                $start, $end, $symbol, $count, format_qty($vol), $skipped);
    $url = "https://www.bitmex.com/api/v1/trade/bucketed?binSize=1d&symbol=$symbol&count=1&reverse=false&startTime=$date";
    $json = file_get_contents($url);
    if ($json === false) {
        log_cmsg("~C91 #ERROR:~C00 fetching daily data from BitMEX API\n");
        die();
    }    
    $data = json_decode($json);
    $last = array_pop($data);
    if (is_object($last)) {
        $diff = $last->volume - $vol;
        $tag = abs($diff) > 0.001 * $vol ? '~C31 #MISTMATCH' : '~C92 #MATCHED';
        log_cmsg("$tag:~C00 diff %s daily volume = %s", format_qty($diff), format_qty($last->volume));
    }

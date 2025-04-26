<?php
    require_once('lib/common.php');
    require_once('lib/esctext.php');
    require_once('lib/db_tools.php');
    ob_implicit_flush();
    
    $t_symbols = 'cm__symbols';
    $t_listings = 'cm__listings';

    $path_list = ['lib', 'bot/lib', '/var/www/lib', '/usr/local/etc/php/'];
    $found = false;
    $fname = 'nope';
    foreach ($path_list as $path) {
      $fname = "$path/db_config.php"; 
      if (!file_exists($fname)) continue;      
      $found = true;
      break;          
    }  
    
    if (!$found) 
        error_exit("FATAL: db_config.php not found at host\n");        
    require_once $fname;    
    if (!isset($db_configs['datafeed'])) 
        error_exit("~C91#FATAL:~C97 datefeed~C00 DB config not exists, used %s for config", $fname);       

    $color_scheme = 'cli';
    $show_perf = rqs_param('perf', false);    
    $t_start = pr_time();
    error_reporting(E_ERROR);
    mysqli_report(MYSQLI_REPORT_ERROR);  
    $db_servers = [null, 'db-remote.lan', 'db-remote.vpn'];
    set_time_limit(5);
    $mysqli = init_remote_db('datafeed');    
    if (!$mysqli) 
        error_exit("#FATAL: can't connect to DB $db_error");
    error_reporting(E_ERROR | E_WARNING | E_PARSE);        

    $sapi_name = php_sapi_name();
    $offline = 'cli' === $sapi_name;    
    $dir = $offline ? __DIR__ : $_SERVER['DOCUMENT_ROOT'];           
    $log_file = fopen("$dir/logs/cm_symbols.log", 'w');      

    if ($show_perf) {
        $mod_t = filemtime(__FILE__);        
        log_cmsg("~C96#PERF($sapi_name):~C00 init_remote_db in %1f s, used server [%s], profiling script %s modified %s <pre>", 
                    pr_time() - $t_start, $db_alt_server, __FILE__, date('Y-m-d H:i', $mod_t));
        log_cmsg("#SERVER: %s", print_r($_SERVER, true));                    
    }
            
    $web = false;    
    $out = 'json';
    $max_rank = 1000;

    if ($offline && isset($argv) && isset($argv[1]))
        $out = $argv[1];
    elseif (isset($_SERVER) && isset($_SERVER['REMOTE_ADDR'])) {              
        $out = rqs_param('out', 'json');      
        $web = true;
        $max_rank = rqs_param('max_rank', $max_rank);
        if ('json' == $out)
            header('Content-type: application/json');
    }      


    date_default_timezone_set('UTC');
  
    $mysqli->try_query("SET time_zone = '+0:00'");
    $max_rank = intval($max_rank);    
    $src = $mysqli->select_rows('*', $t_symbols, "WHERE rank < $max_rank ORDER BY rank LIMIT 1000", MYSQLI_ASSOC);        
    if ($show_perf)
        log_cmsg("~C96#PERF:~C00 loaded %d symbols,  +%.3f s\n", count($src), pr_time() - $t_start);    


    $year = date("Y");
    $opt = '';
    $code = $mysqli->show_create_table($t_listings);

    if (str_in($code, "p$year"))
        $opt = "PARTITION (p2025)";
    else {
       $msg = format_color("~C31#PERF_WARN:~C00 no parition for year %s detected in table %s for server %s: %s", $year, $t_listings, $db_alt_server, $mysqli->server_info );
       fputs($log_file, $msg);
    }

    $res = $mysqli->select_from('cm_id, circulating_supply, supply_coef, volume24_musd, volume24_coins, gain_daily, gain_weekly', 
                                    "`$t_listings` $opt", 'ORDER BY ts DESC, cm_id DESC, symbol DESC LIMIT 1000');
    $stats_map = [];                                    
  
    if ($show_perf && is_object($res))
        log_cmsg("~C96#PERF:~C00 %d listings rows +%.3f s\n", $res->num_rows, pr_time() - $t_start);    
    
    $stats = [];
    while ($res && $row = $res->fetch_assoc())  {
        $id = array_shift($row);
        $stats_map [$id]= $row;
    }

    
    $rows = [];
    foreach ($src as $row)  {      
        // numeric conversions
        // print_r($row);      
        $row['id'] *= 1;
        $id = $row['id'];

        $row['rank'] *= 1;
        $row['last_price'] *= 1.0;
        if (1 == $id) {
            $ts = $row['ts_updated'];
            $elps = diff_minutes('now', $ts);
            if ($elps > 10) 
                error_exit("FATAL: $t_symbols table oudated for $elps minutes, test: ".json_encode($row) );        
        }

        $full = $stats_map[$id] ?? null;
        if (is_array($full)) {         
            foreach ($full as $k => $v)  
                $row[$k] = $v;
        }   

       $rows []= $row;      
    }      

    if ($show_perf)
        log_cmsg("~C96#PERF:~C00 convert and full-filling rows +%.3f s", pr_time() - $t_start);    

    if ('json' == $out)    
        echo json_encode($rows);
    if ('dump' == $out)
        echo print_r($rows);

    if ($show_perf)
        log_cmsg("~C96#PERF:~C00 dump rows +%.3f s", pr_time() - $t_start);    

?>
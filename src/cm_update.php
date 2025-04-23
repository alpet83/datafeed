#!/usr/bin/php
<?php
    chdir(__DIR__);
    require_once('lib/common.php');
    require_once('lib/esctext.php');
    require_once('lib/db_tools.php');
    require_once("lib/db_config.php");
    date_default_timezone_set('UTC');
    $minute = date('i') * 1;
    define('PID_FILE', __DIR__.'/cm_update.pid');
    const LISTINGS_TABLE = 'cm__listings';

    $tmp_symbols = 'cm__symbols_tmp';
    $t_symbols = 'cm__symbols';
    $table = LISTINGS_TABLE;


    $log_file = fopen(__DIR__.'/logs/cm_update.log', 0 == $minute ? 'w' : 'a');    

    function file_age($fname) {
        return time() - filemtime($fname);        
    }

    if (file_exists(PID_FILE) && $age = file_age(PID_FILE) < 60)
        die("#ERROR: cm_update already running, age = $age seconds!\n");

    file_put_contents(PID_FILE, getmypid());

    if (isset($argv[1])) {
        $seconds = $argv[1] * 1;
        log_msg("#WAITING: offsect = $seconds seconds...");
        set_time_limit($seconds * 5);
        sleep($seconds); // processing offset delay
    }    
    // echo "<pre>\n";    
    set_time_limit(60);

    function request_cm($api, $parameters) {
        $url = "https://pro-api.coinmarketcap.com/$api";        
        $api_key = file_get_contents('/etc/CMC-API.key');
        $api_key = trim($api_key);
        $headers = ['Accepts: application/json', "X-CMC_PRO_API_KEY: $api_key"];
        $qs = http_build_query($parameters); // query string encode the parameters
        $qs .= "&CMC_PRO_API_KEY=$api_key";
        $request = "{$url}?{$qs}"; // create the request URL


        $curl = curl_init(); // Get cURL resource
        // Set cURL options
        curl_setopt_array($curl, array(
            CURLOPT_URL => $request,            // set the request URL
            CURLOPT_HTTPHEADER => $headers,     // set the headers 
            CURLOPT_RETURNTRANSFER => 1         // ask for raw response instead of bool
        ));

        
        $response = curl_exec($curl); // Send the request, save the response
        // 
        if ($response) {
           $res = curl_getinfo($curl, CURLINFO_RESPONSE_CODE);
           echo "#RESULT($url): HTTP $res\n ";    
        }  else {
           $err = curl_error($curl);  
           echo "#CURL_ERROR: $err\n";
        }
        
        curl_close($curl); // Close request               
        return $response;
    }  // request_cm

    function price_format($price) {
        $prec = 0;

        $tresh = 100000;
        while ($tresh > 0.0001) {
           if ($price > $tresh) break;
           $prec ++;
           $tresh /= 10.0;
        }
        return sprintf('.%df', $prec);
    }

    error_reporting(E_ERROR | E_WARNING | E_PARSE);
    mysqli_report(MYSQLI_REPORT_ERROR);      

    function exit_process($msg, $code = '') {
      log_cmsg($msg);
      unlink(PID_FILE);      
      die($code);
    }
  
    log_cmsg("~C97#INIT:~C00 connecting to local DB...");    
    $mysqli = init_remote_db('datafeed');
    if (!$mysqli)     
        exit_process( sprintf("~C91#FATAL:~C00 local DB at %s inaccessible!\n", $db_servers[0]), -1);       
           

    $mysqli->try_query("SET time_zone = '+0:00'");
    $last = $mysqli->select_value('ts', LISTINGS_TABLE, 'ORDER BY `ts` DESC');
    if ($last == false) 
       $last = 0;
    else   
       $last = strtotime($last);
    $now = time();
    $elps = $now - $last;
    $minute = $minute % 5;
    log_cmsg("~C93 #TIMING:~C00 elapsed = $elps sec, minute offset = $minute");
    if ($elps < 60) 
       exit_process("#BREAK: no time for update, due frequency policy.", 0);        

    $mysqli->try_query("CREATE TABLE IF NOT EXISTS `$table` LIKE `$t_symbols`;");  // if table locked, where we hang       
    if (time() - $now > 60) 
        exit_process("#BREAK: table lock/creation took too long", -1);

    $res = request_cm('v1/key/info', []);    
    // print_r(json_decode($res)); // print json decoded response    

    $limit = 100;
    if (0 == $minute)
        $limit = 500;

    $res = request_cm('v1/cryptocurrency/listings/latest', ['start' => '1', 'limit' => $limit]); 
    $tsrq = date(SQL_TIMESTAMP);
    $res = str_replace('},{', "},\n  {", $res);
    $res = str_replace(',"data":', ",\n\t\"data\":", $res);
    $obj  = json_decode($res);
    check_mkdir('data');
    if (is_object($obj) && isset($obj->data) && is_array($obj->data)) {
        file_put_contents("data/cm_top$limit.json", $res);
    } 
    else 
      exit_process("~C91#FATAL:~C00 json_decode failed for $res\n", -2);
       
    if (0 == count($obj->data))  
      exit_process("~C91#FATAL:~C00 no rows returned by API", -3);

    

    $rows = array();
    set_time_limit(150);
    $symbols = array();

    // распаковка подробностей по каждой монете
    foreach ($obj->data as $info)  { 

        $coef = 1;
        if ($info->total_supply > 1e9)      $coef = 1000000000; // billions
        elseif ($info->total_supply > 1e6)  $coef = 100000;     // millions
        elseif ($info->total_supply > 1000) $coef = 1000;       // kilos

        $csup = $info->circulating_supply / $coef;
        $tsup = $info->total_supply / $coef;
        $id = $info->id;
        $symbol = strval($info->symbol);

        

        $t = strtotime($info->last_updated); // seems timestamps always rounded to minutes, may have huge lag
        $ts = gmdate(SQL_TIMESTAMP, $t);

        //  sprintf("('%s', $id, '%s', %.5f, %.5f, %u, ", $ts, $symbol, $csup, $tsup, $coef)/
        $row  = [$ts, $id, $symbol, $csup, $tsup, $coef];

        $quote = $info->quote->USD;        
        $volume_coins = 0;

        if (!isset($info->quote->$symbol)) {
            $volume_coins = $quote->volume_24h / $quote->price;            
        }
        else  {
          $self  = $info->quote->$symbol;                
            $volume_coins = $self->volume_24h;
        }  

        $pfmt = price_format($quote->price);
        $price = sprintf("%$pfmt", $quote->price);
        
        $symbols[$id] = ['symbol' => substr($symbol,0, 10), 'name' => substr($info->name, 0, 24), 
                         'rank' => $info->cmc_rank, 'ts_updated' => $ts, 'last_price' => $price];
        
        if ('BTC' == $symbol)
           print_r($quote);

        $row []= $price;
        $row []= round($quote->volume_24h / 1e6, 5);
        $row []= round( $volume_coins, 1);
        $row []=  isset($quote->percent_change_24h) ? round( $quote->percent_change_24h, 2) : 0.0000;
        $row []=  isset($quote->percent_change_7d) ? round($quote->percent_change_7d, 2) :  0.00001;        
        $cols = array_keys($row);
        $srow = $mysqli->pack_values($cols, $row);
        $rows []= "($srow)";                             
    }    
       
    function SaveData($mysqli, array $symbols,  array $rows) {
        global $minute, $table, $t_symbols, $tmp_symbols; 
        $res = -1;
        try {
            
            $columns = 'ts, cm_id, symbol, circulating_supply, total_supply, supply_coef, price, volume24_musd, volume24_coins, gain_daily, gain_weekly';
            $cols = explode(',', str_replace(' ', '' , $columns));

            $query = "INSERT IGNORE INTO `$table` ($columns)\n";     
            if (0 == $minute) { // save history every 5 minutes
                $query .= ' VALUES '.implode(",\n", $rows).";";       
                log_cmsg("~C94#ROWS(1):~C00 count = %d ",count($rows));                   
                if ($mysqli->try_query($query)) {
                    file_put_contents('logs/cm_update-0.sql', $query);
                    log_cmsg("~C97#INSERT:~C00 added %d to %s", $mysqli->affected_rows, $table);
                }
                else  {
                    file_put_contents('logs/failed-cm_update-0.sql', $query);
                    log_cmsg("~C91#FAILED:~C00 try_query for insert %d returned false \n", count($rows));
                }
            }    


            $mysqli->try_query("TRUNCATE TABLE `$tmp_symbols`");
            $rtm_cols = 'ts_updated, id, symbol, name, rank, last_price';

            $query = "INSERT INTO `$tmp_symbols` ($rtm_cols)\n VALUES";            
            $rows = [];            
            $rtm_cols = str_replace(' ', '', $rtm_cols); //  ['ts_updated', 'id', 'symbol', 'name', 'rank', 'last_price'];
            foreach ($symbols as $id => $rec) {           
                // if (is_array($rec) && !isset($rec['last_update'])) $rec['last_update'] = date(SQL_TIMESTAMP);                                     
                $rec['id'] = $id;
                $row = $mysqli->pack_values($rtm_cols, $rec);                  
                $rows []= "($row)";
            }
            $query .= implode(",\n", $rows).";";

            if ($mysqli->try_query($query)) { // push rows in temp table
                file_put_contents('logs/cm_update-1.sql', $query);
                log_cmsg("~C96#PERF:~C00 in temporary table stored %d rows", $mysqli->affected_rows);
            }
            else  {
                file_put_contents('logs/failed-cm_update-1.sql', $query);
                log_cmsg("~C91#FAILED:~C00 try_query for insert %d returned false \n", count($rows));
                return 0;
            }            
            
            log_cmsg("~C94#SYMBOLS[0]:~C00 %s", json_encode($rows[0], JSON_NUMERIC_CHECK));
            
            $query = "INSERT INTO `$t_symbols` SELECT * FROM `$tmp_symbols` AS CMT\n";
            $query .= " ON DUPLICATE KEY UPDATE ts_updated = CMT.ts_updated, last_price = CMT.last_price";            

            if ($mysqli->try_query($query)) {
                $res = $mysqli->affected_rows;             
                file_put_contents('logs/cm_update-2.sql', $query);
                log_cmsg("~C94#INSERT:~C00 affected %d rows in %s", $res, $t_symbols);                 
            }
            else {
                file_put_contents('logs/failed-cm_update-2.sql', $query);
                log_cmsg("~C91#FAILED:~C00 try_query for insert %d returned false \n", count($rows));
            }
            
        }
        catch (Exception $e) {
            log_cmsg("~C91#EXCEPTION:~C00 %s: ~C97".$e->getTraceAsString(), $e->getMessage());
        }
        return ($res >= 0);
    }
    
    SaveData($mysqli, $symbols, $rows);  // local DB saving
     
    $lines = file('/etc/hosts');     
    $rsync = false;
    // processing all available remote DBs, for sync
    foreach ($lines as $line) {
        $line = str_replace("\t", ' ', trim($line));
        $ip = strtok( $line, ' ');
        $hosts = $line;
        $start = strpos($hosts, 'db-remote.');   // possible enum tokens, but scan faster   
        if (false === $start) continue;                
        $host = substr($hosts, $start); 
        $host = strtok($host, ' ');
        $host = trim($host);
        if (strlen($host) < 8) continue;
        log_cmsg("~C95#TEST:~C00 $line => %s", $host);            
        $db_servers = [$host];            
        
        if (isset($db_configs['remote']))
            $db_configs['datafeed'] = $db_configs['remote']; // WARN: using different creds 

        $mysqli = init_remote_db('datafeed');
        if ($mysqli) {
            log_cmsg("~C93#SYNC:~C00 Trying remote DB at~C92 %s:%s~C00...", $host, $ip); 
            $rsync = SaveData($mysqli, $symbols, $rows);           
            break;
        }   
        else
            log_cmsg("~C91#WARN:~C00 Remote DB at~C92 %s:%s~C00 inaccessible:~C97 %s\n", $host, $ip, $db_error);       
    }                    
    if (!$rsync) 
        log_cmsg("~C91#WARN:~C00 no remote DBs synced! Checked hosts:~C97 ".implode("\t", $lines));
    exit_process("#ENDED: all ops completed.");
?>
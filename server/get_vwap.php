<?php
   require_once 'lib/common.php';
   if (!function_exists('rqs_param'))
       die("#FATAL: rqs_param not defined\n");

   require_once 'lib/db_tools.php';

   if (file_exists('bot'))
      require_once "bot/lib/db_config.php";
   else
      require_once "lib/db_config.php";   
      
   $inj_flt = 'PHP:SQL';

   $pair_id = rqs_param('pair_id', 1) * 1;
   $limit = rqs_param('limit', 10) * 1;

   $hour_back = date('Y-m-d H:i:00', time() - 3600); // typically  over hour
   $ts_from = rqs_param('ts_from', $hour_back, $inj_flt, REGEX_TIMESTAMP_FILTER);   
   $exch = rqs_param('exchange', 'any', $inj_flt, '/(\w+)/'); 
   $host = rqs_param('host', 'db-local.lan', $inj_flt, '/(\S+)/');

   $exch = strtolower($exch);

   $db_servers = [$host, 'db-remote.vpn'];
   error_reporting(E_ERROR | E_WARNING | E_PARSE);
   mysqli_report(MYSQLI_REPORT_ERROR);  
     
   $mysqli = init_remote_db('datafeed'); // default
   if (!$mysqli)
      die("#FATAL: can't connect to database datafeed at $host\n");

   function format_result(float $avg): string {
      $pp = max(1, 6 - log10($avg)); // ex: 1000-9900 = 3, 10000-99000 = 2
      $pp = min(10, $pp);              
      return number_format($avg, $pp, '.', '');
   }

   function print_result(float $avg, bool $exit = true) {
      echo format_result($avg);
      if ($exit)
         exit(0);
   }
   
   // TODO: calculate from spread walk history, due last candles possible outdated      
   function load_from_candles(int $pair_id, string $ts_from, int $limit, float &$vwap) {
      global $mysqli, $db_name; 
      $id_map = $mysqli->select_map('pair_id,ticker', "ticker_map");
      if (!$id_map) die("#FATAL: not exists ticker_map for $db_name\n");   
      if (!isset($id_map[$pair_id])) return "#ERROR: not registered pair_id #$pair_id\n";
      $pair = strtolower($id_map[$pair_id]);
      $table = "candles__$pair";
      if (!table_exists($table))
         return "#FATAL: not exists $table";
      $rows = $mysqli->select_rows('*', $table, "WHERE ts >= '$ts_from' ORDER BY `ts` DESC LIMIT $limit", MYSQLI_ASSOC);  
      if (!is_array($rows))   
         return "#FATAL: error retrieving candles from $table: {$mysqli->error} ";
      if (count($rows) < $limit - 5)
         return "#ERROR: too small candles count loaded = ".count($rows);

      $rows = array_reverse($rows);
      $vol = 0;
      $result = 0;
      foreach ($rows as $row)  {
         $result += $row['close'] * $row['volume'];
         $vol += $row['volume'];
      }  
      if ($vol > 0) {       
         $vwap = $result / $vol;         
      }  
         else  
           return "#ERROR: for loaded candles volume = $vol\n";
     return "#OK";    
   }

   $sources = ['binance', 'bitfinex', 'bitmex', 'bybit']; // remove not used exchanges
   if ('any' != $exch && 'all' != $exch) 
       $sources = [$exch];

   $res = [];
   $accum = [];

   foreach ($sources as $db_name) {
      if (!$mysqli->select_db($db_name)) continue;

      $vwap = 0;
      $key = "$db_name-candles";
      $err = load_from_candles($pair_id, $ts_from, $limit, $vwap);      
      if ('#OK' != $err) {
         $res []= $err;
         $accum[$key] = $err;
         continue;
      }
      if ('any' == $exch || $db_name == $exch)          
         print_result($vwap); 
      $accum [$key] = format_result($vwap);
   }
   
  
   // not possible return VWAP, trying return EMA from tickers
   function load_from_tickers(int $pair_id, string $ts_from, int $limit, float &$avg): string {
      global $mysqli;
      $table = "ticker_history";
      if (!$mysqli->table_exists($table))
           return "#FAIL: not exists $table";

      $rows = $mysqli->select_rows('bid,ask,last,fair_price', $table, 
                  "WHERE (pair_id = $pair_id) AND (ts >= '$ts_from') ORDER BY `ts` DESC LIMIT $limit", MYSQLI_ASSOC);
      if (!$rows || count($rows) < 3)
           return "#FAIL: error retrieving ticker history from $table: {$mysqli->error} ".var_export($rows, true);
      $rows = array_reverse($rows);
      $avg = 0;

      foreach ($rows as $row) {
         $price = $row['last'];
         if ($row['fair_price'] > 0)
             $price = $row['fair_price']; 
         $price = max($row['bid'], $price);
         $price = min($row['ask'], $price);
         $avg = (0 == $avg) ? $price : $avg * 0.8 + $price * 0.2; // EMA 5         
      }      
      return "#OK";
   }
   
   $sources = ['binance', 'bitfinex', 'bitmex'];

   foreach ($sources as $src) {      
      if (!$mysqli->select_db($src)) continue;
      $avg = 0;
      $test = load_from_tickers($pair_id, $ts_from, $limit, $avg);
      if ('#OK' != $test) {
         $res []= $test;
         continue;      
      }
      if ('any' == $exch || $src == $exch)
         print_result($avg);
      $accum["$src-tickers"] = format_result($avg);
   }   

   if (count($accum) > 0) {
      echo json_encode($accum);
      exit(0);
   }
   echo json_encode($res);
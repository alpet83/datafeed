<?php
   require_once 'lib/common.php';
   if (!function_exists('rqs_param'))
       die("#FATAL: rqs_param not defined\n");

   require_once 'lib/db_tools.php';

   if (file_exists('bot'))
      require_once "bot/lib/db_config.php";
   else
      require_once "lib/db_config.php";   
      

   $pair_id = rqs_param('pair_id', 1);
   $limit = rqs_param('limit', 10);
   $ts_from = rqs_param('ts_from', date('Y-m-d H:i:00', time() - 3600));
   $exch = rqs_param('exchange', 'bitfinex'); 
   $host = rqs_param('host', 'db-local.lan');

   $exch = strtolower($exch);

   $db_servers = [$host, 'db-remote.vpn'];
   error_reporting(E_ERROR | E_WARNING | E_PARSE);
   mysqli_report(MYSQLI_REPORT_ERROR);  
     
   $mysqli = init_remote_db($exch);
   if (!$mysqli)
      die("#FATAL: can't connect to database datafeed at $host\n");
   // TODO: calculate from spread walk history, due last candles possible outdated      
   $id_map = $mysqli->select_map('pair_id,ticker', "ticker_map");
   if (!$id_map) die("#FATAL: not exists ticker_map for $exch\n");
   function load_from_candles($pair_id, $ts_from, $limit) {
      global $mysqli, $id_map; 
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
         $vwap = sprintf('%.f', $result / $vol);
         $pp = max(1, 6 - log10($vwap)); // ex: 1000-9900 = 3, 10000-99000 = 2
         $pp = min(10, $pp);         
         // $vwap = round($vwap, floor($pp));
         echo number_format($vwap, floor($pp), '.', '');
      }  
         else  
           return "#ERROR: for loaded candles volume = $vol\n";
     return "#OK";    
   }

   $res = [];
   $res []= load_from_candles($pair_id, $ts_from, $limit);
   if ($res[0] == '#OK') die(''); 
  
   // not possible return VWAP, trying return EMA from tickers
   function load_from_tickers($pair_id, $ts_from, $limit) {
      global $mysqli, $id_map;
      $table = "ticker_history";
      if (!$mysqli->table_exists($table))
           return "#FAIL: not exists $table";
      $rows = $mysqli->select_rows('bid,ask,last,fair_price', $table, "WHERE (pair_id = $pair_id) AND ts >= '$ts_from' ORDER BY `ts` DESC LIMIT $limit", MYSQLI_ASSOC);
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
         if (0 == $avg) 
           $avg = $price;
         else  
           $avg = $avg * 0.8 + $price * 0.2; // EMA 5         
      }
      $pp = max(1, 6 - log10($avg)); // ex: 1000-9900 = 3, 10000-99000 = 2
      $pp = min(10, $pp);  
      echo number_format($avg, $pp, '.', '');
      return "#OK";
   }

   if ($mysqli->table_exists('trading.binance__ticker_history'))
       $mysqli->select_db('trading');

   $sources = ['binance', 'bitfinex', 'bitmex'];
   foreach ($sources as $src) {
      $mysqli->select_db($src);
      $test = load_from_tickers($pair_id, $ts_from, $limit);
      if ('#OK' == $test) die();
      $res []= $test;
   }   
   echo json_encode($res);
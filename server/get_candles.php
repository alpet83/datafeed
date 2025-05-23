<?php
   set_include_path(get_include_path() . PATH_SEPARATOR . '..'.PATH_SEPARATOR . 'bot');
   require_once 'lib/common.php';
   require_once 'lib/db_tools.php';   
   require_once "lib/db_config.php";        

   $inj_flt = 'PHP:SQL';
   $pair_id = rqs_param('pair_id', 1) * 1;
   $limit = rqs_param('limit', 1000) * 1;
   $before = date('Y-m-d H:i:00', time() - 60000);
   $ts_from = rqs_param('ts_from', $before, $inj_flt, REGEX_TIMESTAMP_FILTER);
   $exch = rqs_param('exchange', 'bitfinex', $inj_flt, '/(\w+)/'); 
   $exch = strtolower($exch);

   error_reporting(E_ERROR | E_WARNING | E_PARSE);
   mysqli_report(MYSQLI_REPORT_ERROR);  
     
   $mysqli = init_remote_db($exch);
   if (!$mysqli)
      die("#FATAL: can't connect to database datafeed\n");

   $id_map = $mysqli->select_map('pair_id,ticker', "ticker_map");
   if (!$id_map) die("#FATAL: not exists ticker_map for $exch\n");
   if (!isset($id_map[$pair_id])) die("#ERROR: not registered pair_id #$pair_id\n");
   $pair = strtolower($id_map[$pair_id]);
   $table = "candles__$pair";
   if (!table_exists($table))
      die("#FATAL: not exists $table");
   $rows = $mysqli->select_rows('*', $table, "WHERE ts >= '$ts_from' ORDER BY `ts` DESC LIMIT $limit", MYSQLI_ASSOC);  
   if (!is_array($rows))   
      die("#FATAL: error retrieving candles from $table: {$mysqli->error} ");
   $rows = array_reverse($rows);
   header('Content-type: application/json');
   echo json_encode($rows);
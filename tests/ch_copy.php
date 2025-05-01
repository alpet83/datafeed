<?php
    /* This script can be used for loading ticks from remote ClickHouse DB with full table 'ticks__archive'.  
       Before processing need create credentials collection bmx_remote and add couple privileges for current user:
        GRANT REMOTE, CREATE TEMPORARY TABLE, NAMED COLLECTION ON `bmx_remote` to %username%
    */
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_config.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/clickhouse.php';
    
    $db_name = 'bitmex';    

    $mysqli = init_remote_db($db_name);

    $tmap = $mysqli->select_map('symbol,ticker', 'ticker_map');

    $conn = ClickHouseConnectMySQL(null, null, null,$db_name); // datafeed
    $list = $conn->try_query("SHOW TABLES LIKE 'ticks__%'");


    $start_copy = false;
    while ($list && $row = $list->fetch_assoc()) {
        $table_name = array_pop($row);
        log_cmsg("~C97#PROCESSING:~C00 %s.%s", $conn->active_db(), $table_name);              
        $ticker = str_replace('ticks__', '', $table_name);
        $sym = array_search($ticker, $tmap);    
        if ('FILUSD' == $sym)
            $start_copy = true;

        if (false === $sym || !$start_copy) continue;
        try {            
            $parts = $conn->select_map('partition,active', 'system.parts', "WHERE table = '$table_name' AND active = 1 AND database = '$db_name'");                        
            foreach ($parts as $part => $active)
                if ($part < '2023-10-01')  {
                    $res = $conn->try_query("ALTER TABLE `$table_name` DROP PARTITION ('$part')");
                    log_cmsg("~C33 #DROP_PARTITION:~C00 %s %s", $part, $res ?  'success' : '~C91 failed');
                }


            log_cmsg("~C96 #PERF_START_SYNC:~C00 symbol %s ", $sym);
            $t_start = pr_time();
            
            $res = $conn->try_query("INSERT INTO `$table_name` SELECT ts, price, amount, buy, trade_no FROM remote(bmx_remote, table='ticks__archive') WHERE symbol = '$sym';");
            $elps = pr_time() - $t_start;
            if ($res) {
                $table_qfn = $conn->active_db().'.'.$table_name;
                log_cmsg("~C32 #SUCCESS:~C00 full time %1.f sec, stats:\n ", $elps);
                $strict = "WHERE  (query_kind = 'Insert') AND (type = 'QueryFinish') AND (tables[1]  = '$table_qfn') ORDER BY event_time_microseconds DESC LIMIT 1";
                $stats = $conn->select_row('event_time_microseconds, written_rows, result_rows, tables', 'system.query_log', $strict);                
                print_r($stats);
                $conn->try_query("OPTIMIZE TABLE `$table_name` FINAL DEDUPLICATE"); 
            }
            else
                log_cmsg("~C91 #FAILED_INSERT:~C00 sorry for that");            

        } catch (Throwable $E) {
            $msg = $E->getMessage();
            log_cmsg("~C91 #EXCEPTION:~C00 on table %s: %s", $table_name, $msg);            
        }
    }


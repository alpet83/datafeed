<?php
    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_config.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/clickhouse.php';

    $db_name = $argv[1] ?? 'bitmex';
    $test = $argv[2] ?? 'check';
    $needle = $argv[3] ?? 'tmp';


    $conn = ClickHouseConnectMySQL(null, null, null,$db_name); // datafeed
    $list = $conn->try_query('SHOW TABLES');

    function copy_partitions(string $source, string $target): int {
        global $conn, $db_name;
        $parts = $conn->select_map('partition,active', 'system.parts', "FINAL WHERE table = '$source' AND active = 1 AND database = '$db_name'");
        if (empty($parts))
            return 0;
        $good = 0;
        foreach ($parts as $part => $active) 
            try {
                if ($conn->try_query("INSERT INTO `$target` SELECT * FROM `$source` WHERE _partition_value.1 = '$part'"))
                    $good ++;
            }
            catch (Throwable $E) {
                log_cmsg("~C31 #FAILED:~C00 partition %s [%s] not readable?: %s", $source, $part, $E->getMessage());                
            }
        return $good;
    }

    while ($list && $row = $list->fetch_assoc()) {
        $table_name = array_pop($row);
        log_cmsg("~C97#CHECKING:~C00 %s.%s", $conn->active_db(), $table_name);        

        if (str_contains($table_name, '_1d') && !str_contains($table_name, '__1D') || 
            str_contains($table_name, $db_name) || 'drop' == $test && str_contains($table_name, $needle))  {
            log_cmsg("~C31 #DROP_WRONG:~C00 `%s`", $table_name);
            $conn->try_query("DROP TABLE `$table_name`");
            continue;        
        }

        if (!$conn->table_exists($table_name)) {
            log_cmsg("~C31 #STRANGE:~C00 not exists %s", $table_name);
            continue;
        }


        try {            
            if ('check' == $test)
                $conn->try_query("CHECK TABLE `$table_name`");
            if ('read' == $test) 
                log_cmsg(' ~C93#SIZE:~C00 %d', $conn->select_value('COUNT(*)', $table_name));
            if ('optimize' == $test) 
                $conn->try_query("OPTIMIZE TABLE `$table_name` FINAL DEDUPLICATE");
        } catch (Throwable $E) {
            $msg = $E->getMessage();
            log_cmsg("~C91 #EXCEPTION:~C00 on table %s: %s", $table_name, $msg);
            if (str_in($msg, 'Unknown codec family code')) {
                $mysqli = $conn;

                $temp_name = "{$table_name}_tmp";
                $res = $mysqli->try_query("CREATE TABLE $temp_name  CLONE AS `$table_name`") &&
                       $mysqli->try_query("TRUNCATE TABLE `$table_name`");                       
                if ($res)
                    log_cmsg("~C92 #REPAIR_STEP1:~C00 create temporary clone %s and truncated", $temp_name);
                else {
                    log_cmsg("~C91 #REPAIR_STEP1_FAILED:~C00 something goes wrong.");
                    continue;
                }
                if ($parts = copy_partitions($temp_name, $table_name)) {
                    log_cmsg("~C92 #REPAIR_STEP2:~C00 copied %d partitions, dropping %s", $parts, $temp_name);
                    $mysqli->try_query("DROP TABLE `$temp_name`");
                }
            }
            
        }
    }


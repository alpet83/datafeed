#!/usr/bin/php
<?php

require_once 'lib/common.php';
require_once 'lib/esctext.php';
require_once 'lib/db_tools.php';
require_once 'lib/db_config.php';

date_default_timezone_set('UTC');

const CONTROL_TABLE = 'loader_control';
const ACTIVITY_TABLE = 'loader_activity';
const DEFAULT_LOOP_SECONDS = 15;
const DEFAULT_HEARTBEAT_SECONDS = 30;

$loaders_dir = getenv('DATAFEED_MANAGER_LOADERS_DIR');
if (!$loaders_dir) {
    $loaders_dir = '/app/datafeed/src';
}
$log_path = getenv('DATAFEED_MANAGER_LOG_PATH');
if (!$log_path) {
    $log_path = '/app/var/log/datafeed/main.log';
}
$loop_seconds = intval(getenv('DATAFEED_MANAGER_LOOP_SECONDS') ?: DEFAULT_LOOP_SECONDS);
$loop_seconds = max(3, $loop_seconds);
$heartbeat_seconds = intval(getenv('DATAFEED_MANAGER_HEARTBEAT_SECONDS') ?: DEFAULT_HEARTBEAT_SECONDS);
$heartbeat_seconds = max(10, $heartbeat_seconds);
$manager_db_name = getenv('DATAFEED_MANAGER_DB_NAME') ?: 'datafeed';
$manager_sql_dir = rtrim(getenv('DATAFEED_MANAGER_SQL_DIR') ?: '/app/datafeed/sql', '/\\');

$stop_requested = false;
$running = [];
$manager_id = getmypid();
$manager_host = substr(gethostname() ?: 'unknown', 0, 63);
$last_heartbeat = 0;

$known_loaders = [
    ['key' => 'binance_candles', 'exchange' => 'binance', 'script' => 'bnc_candles_dl.php', 'period' => 3600, 'timeout' => 3900],
    ['key' => 'binance_ticks', 'exchange' => 'binance', 'script' => 'bnc_ticks_dl.php', 'period' => 3600, 'timeout' => 3900, 'enabled' => 0],
    ['key' => 'bitmex_candles', 'exchange' => 'bitmex', 'script' => 'bmx_candles_dl.php', 'period' => 3600, 'timeout' => 3900],
    ['key' => 'bitmex_ticks', 'exchange' => 'bitmex', 'script' => 'bmx_ticks_dl.php', 'period' => 3600, 'timeout' => 3900, 'enabled' => 0],
    ['key' => 'bitfinex_candles', 'exchange' => 'bitfinex', 'script' => 'bfx_candles_dl.php', 'period' => 3600, 'timeout' => 3900],
    ['key' => 'bitfinex_ticks', 'exchange' => 'bitfinex', 'script' => 'bfx_ticks_dl.php', 'period' => 3600, 'timeout' => 3900, 'enabled' => 0],
    ['key' => 'bybit_candles', 'exchange' => 'bybit', 'script' => 'bbt_candles_dl.php', 'period' => 3600, 'timeout' => 3900],
    ['key' => 'coinmarketcap_update', 'exchange' => 'meta', 'script' => 'cm_update.php', 'period' => 300, 'timeout' => 240, 'enabled' => 0],
];

if (function_exists('pcntl_async_signals')) {
    pcntl_async_signals(true);
}
if (function_exists('pcntl_signal')) {
    pcntl_signal(SIGTERM, function () use (&$stop_requested) {
        $stop_requested = true;
    });
    pcntl_signal(SIGINT, function () use (&$stop_requested) {
        $stop_requested = true;
    });
}

@mkdir(dirname($log_path), 0775, true);

function mgr_log(string $fmt, ...$args): void {
    global $log_path;
    $prefix = gmdate('Y-m-d H:i:s') . ' [datafeed] ';
    $line = $args ? vsprintf($fmt, $args) : $fmt;
    file_put_contents($log_path, $prefix . $line . PHP_EOL, FILE_APPEND);
    echo $prefix . $line . PHP_EOL;
}

function sqli_df_mgr(): mysqli_ex {
    global $manager_db_name;
    static $conn = null;
    if ($conn instanceof mysqli_ex) {
        return $conn;
    }
    $target_db = $manager_db_name;
    $conn = init_remote_db($target_db);
    if (!$conn instanceof mysqli_ex && 'datafeed' === $target_db) {
        $fallback = init_remote_db('trading');
        if ($fallback instanceof mysqli_ex) {
            $manager_db_name = 'trading';
            $conn = $fallback;
            mgr_log('warn: no grant for datafeed DB, fallback to trading DB for manager tables');
        }
    }
    if (!$conn instanceof mysqli_ex) {
        throw new RuntimeException('Cannot connect to manager database');
    }
    $conn->try_query("SET time_zone = '+0:00'");
    return $conn;
}

function known_exchanges(): array {
    global $known_loaders;
    $set = ['datafeed' => true];
    foreach ($known_loaders as $loader) {
        $exchange = strtolower(trim(strval($loader['exchange'] ?? '')));
        if ('' === $exchange || 'meta' === $exchange) {
            continue;
        }
        $set[$exchange] = true;
    }
    return array_keys($set);
}

function apply_sql_file_if_present(mysqli_ex $db, string $sql_path, string $db_name): bool {
    if (!is_file($sql_path)) {
        return false;
    }

    $sql = file_get_contents($sql_path);
    if (!is_string($sql) || '' === trim($sql)) {
        return false;
    }

    if (!$db->multi_query($sql)) {
        throw new RuntimeException(sprintf('failed SQL bootstrap for %s from %s: %s', $db_name, $sql_path, $db->error));
    }

    do {
        $res = $db->store_result();
        if ($res instanceof mysqli_result) {
            $res->free();
        }
    } while ($db->more_results() && $db->next_result());

    if ($db->errno) {
        throw new RuntimeException(sprintf('SQL bootstrap error for %s from %s: %s', $db_name, $sql_path, $db->error));
    }

    mgr_log('sql bootstrap applied: db=%s file=%s', $db_name, $sql_path);
    return true;
}

function apply_seed_rows_from_proto(mysqli_ex $db, string $db_name, string $sql_path, array $tables = ['ticker_map', 'data_config']): void {
    if (!is_file($sql_path)) {
        return;
    }

    $sql = file_get_contents($sql_path);
    if (!is_string($sql) || '' === trim($sql)) {
        return;
    }

    $db_name_q = $db->real_escape_string($db_name);
    foreach ($tables as $table) {
        $table_q = $db->real_escape_string($table);
        $exists = intval($db->query(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$db_name_q' AND table_name = '$table_q'"
        )->fetch_column(0));
        if ($exists <= 0) {
            continue;
        }

        $pattern = '/INSERT\\s+INTO\\s+`' . preg_quote($table, '/') . '`\\s*\\([^)]*\\)\\s*VALUES\\s*.*?;/is';
        if (!preg_match_all($pattern, $sql, $matches) || empty($matches[0])) {
            continue;
        }

        $db->try_query("USE `$db_name_q`");
        foreach ($matches[0] as $statement) {
            $seed_query = preg_replace('/^INSERT\\s+INTO\\s+/i', 'INSERT IGNORE INTO ', trim($statement), 1);
            if (!is_string($seed_query) || '' === $seed_query) {
                continue;
            }
            $db->try_query($seed_query);
        }
        mgr_log('seed reconcile applied: db=%s table=%s file=%s', $db_name, $table, $sql_path);
    }
}

function ensure_exchange_dbs(): void {
    global $manager_sql_dir;

    $db = sqli_df_mgr();
    $exchanges = known_exchanges();
    $proto_files = [
        'datafeed' => [
            'cm_tables.sql',
            'src_activity.sql',
        ],
        'binance' => ['db_binance_proto.sql'],
        'bitmex' => ['db_bitmex_proto.sql'],
        'bitfinex' => ['db_bitfinex_proto.sql'],
        'bybit' => ['db_bybit_proto.sql'],
    ];

    foreach ($exchanges as $db_name) {
        $db_name_q = $db->real_escape_string($db_name);
        $db->try_query("CREATE DATABASE IF NOT EXISTS `$db_name_q` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");

        $table_count = intval($db->query("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$db_name_q'")->fetch_column(0));
        $db_proto_files = $proto_files[$db_name] ?? [];
        if ($table_count <= 0) {
            foreach ($db_proto_files as $proto_name) {
                $proto_path = $manager_sql_dir . DIRECTORY_SEPARATOR . $proto_name;
                apply_sql_file_if_present($db, $proto_path, $db_name);
            }
        }

        foreach ($db_proto_files as $proto_name) {
            $proto_path = $manager_sql_dir . DIRECTORY_SEPARATOR . $proto_name;
            apply_seed_rows_from_proto($db, $db_name, $proto_path);
        }
    }
}

function ensure_loader_runtime_dirs(): void {
    foreach (['/tmp/bnc', '/tmp/bmx', '/tmp/bfx', '/tmp/bbt', '/tmp/cm'] as $tmp_dir) {
        @mkdir($tmp_dir, 0775, true);
    }
}

function ensure_schema(): void {
    global $known_loaders, $manager_db_name;
    $db = sqli_df_mgr();
    if ('datafeed' === $manager_db_name) {
        $db->try_query('CREATE DATABASE IF NOT EXISTS `datafeed`');
    }
    $db->try_query(sprintf('USE `%s`', $manager_db_name));

    $db->try_query(
        "CREATE TABLE IF NOT EXISTS `" . CONTROL_TABLE . "` (
            `loader_key` VARCHAR(64) NOT NULL,
            `exchange` VARCHAR(32) NOT NULL,
            `script_name` VARCHAR(96) NOT NULL,
            `enabled` TINYINT(1) NOT NULL DEFAULT 1,
            `period_seconds` INT UNSIGNED NOT NULL DEFAULT 3600,
            `timeout_seconds` INT UNSIGNED NOT NULL DEFAULT 3900,
            `last_started_at` DATETIME NULL,
            `last_finished_at` DATETIME NULL,
            `last_exit_code` INT NULL,
            `last_pid` INT NULL,
            `last_error` VARCHAR(255) NOT NULL DEFAULT '',
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`loader_key`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    $db->try_query(
        "CREATE TABLE IF NOT EXISTS `" . ACTIVITY_TABLE . "` (
            `name` VARCHAR(64) NOT NULL,
            `host` VARCHAR(64) NOT NULL,
            `pid` INT NOT NULL,
            `state` VARCHAR(24) NOT NULL,
            `active_count` INT UNSIGNED NOT NULL DEFAULT 0,
            `note` VARCHAR(255) NOT NULL DEFAULT '',
            `ts_alive` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`name`, `host`, `pid`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    foreach ($known_loaders as $loader) {
        $key = $db->real_escape_string($loader['key']);
        $exchange = $db->real_escape_string($loader['exchange']);
        $script = $db->real_escape_string($loader['script']);
        $period = intval($loader['period']);
        $timeout = intval($loader['timeout']);
        $enabled = array_key_exists('enabled', $loader) ? intval($loader['enabled']) : 1;

        $db->try_query(
            "INSERT INTO `" . CONTROL_TABLE . "` (`loader_key`, `exchange`, `script_name`, `enabled`, `period_seconds`, `timeout_seconds`) VALUES
            ('$key', '$exchange', '$script', $enabled, $period, $timeout)
            ON DUPLICATE KEY UPDATE
              `exchange` = VALUES(`exchange`),
              `script_name` = VALUES(`script_name`),
              `period_seconds` = VALUES(`period_seconds`),
              `timeout_seconds` = VALUES(`timeout_seconds`)"
        );
    }
}

function heartbeat(string $state, int $active_count, string $note = ''): void {
    global $manager_host, $manager_id, $last_heartbeat, $heartbeat_seconds;
    $now = time();
    if ('running' === $state && ($now - $last_heartbeat) < $heartbeat_seconds) {
        return;
    }

    $db = sqli_df_mgr();
    $name = 'datafeed';
    $name_q = $db->real_escape_string($name);
    $host_q = $db->real_escape_string($manager_host);
    $state_q = $db->real_escape_string($state);
    $note_q = $db->real_escape_string(substr($note, 0, 255));

    $db->try_query(
        "INSERT INTO `" . ACTIVITY_TABLE . "` (`name`, `host`, `pid`, `state`, `active_count`, `note`) VALUES
         ('$name_q', '$host_q', $manager_id, '$state_q', $active_count, '$note_q')
         ON DUPLICATE KEY UPDATE
           `state` = VALUES(`state`),
           `active_count` = VALUES(`active_count`),
           `note` = VALUES(`note`),
           `ts_alive` = CURRENT_TIMESTAMP"
    );
    $last_heartbeat = $now;
}

function fetch_control_rows(): array {
    $db = sqli_df_mgr();
    $rows = $db->select_rows(
        '`loader_key`,`exchange`,`script_name`,`enabled`,`period_seconds`,`timeout_seconds`,`last_started_at`,`last_finished_at`',
        CONTROL_TABLE,
        'ORDER BY `loader_key`',
        MYSQLI_ASSOC
    );
    return is_array($rows) ? $rows : [];
}

function ts_or_zero(?string $ts): int {
    if (!$ts) {
        return 0;
    }
    $parsed = strtotime($ts);
    return is_numeric($parsed) ? intval($parsed) : 0;
}

function mark_loader_start(string $loader_key, int $pid): void {
    $db = sqli_df_mgr();
    $key = $db->real_escape_string($loader_key);
    $db->try_query(
        "UPDATE `" . CONTROL_TABLE . "`
         SET `last_started_at` = UTC_TIMESTAMP(), `last_pid` = $pid, `last_error` = ''
         WHERE `loader_key` = '$key'"
    );
}

function mark_loader_finish(string $loader_key, int $exit_code, string $error = ''): void {
    $db = sqli_df_mgr();
    $key = $db->real_escape_string($loader_key);
    $error_q = $db->real_escape_string(substr($error, 0, 255));
    $db->try_query(
        "UPDATE `" . CONTROL_TABLE . "`
         SET `last_finished_at` = UTC_TIMESTAMP(), `last_exit_code` = $exit_code, `last_error` = '$error_q'
         WHERE `loader_key` = '$key'"
    );
}

function start_loader(array $row): ?array {
    global $loaders_dir;

    $loader_key = strval($row['loader_key']);
    $script = basename(strval($row['script_name']));
    $script_path = rtrim($loaders_dir, '/\\') . DIRECTORY_SEPARATOR . $script;
    if (!file_exists($script_path)) {
        mark_loader_finish($loader_key, 127, "script not found: $script_path");
        mgr_log('skip %s: script not found: %s', $loader_key, $script_path);
        return null;
    }

    $log_file = '/app/var/log/datafeed/' . $loader_key . '.log';
    $php_opts = '-d include_path=.:/app:/app/datafeed/src:/app/datafeed:/usr/share/php:/usr/local/lib/php';
    $cmd = sprintf('php %s %s >> %s 2>&1', $php_opts, escapeshellarg($script_path), escapeshellarg($log_file));
    $descriptors = [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ];

    $proc = proc_open(['/bin/sh', '-lc', $cmd], $descriptors, $pipes, dirname($script_path));
    if (!is_resource($proc)) {
        mark_loader_finish($loader_key, 126, 'proc_open failed');
        mgr_log('failed to start %s: proc_open failed', $loader_key);
        return null;
    }

    foreach ($pipes as $pipe) {
        fclose($pipe);
    }

    $status = proc_get_status($proc);
    $pid = intval($status['pid'] ?? 0);
    mark_loader_start($loader_key, $pid);
    mgr_log('started %s (%s), pid=%d', $loader_key, $script, $pid);

    return [
        'proc' => $proc,
        'started' => time(),
        'timeout' => max(120, intval($row['timeout_seconds'] ?? 3900)),
        'script' => $script,
        'pid' => $pid,
    ];
}

function is_due(array $row): bool {
    $period = max(60, intval($row['period_seconds'] ?? 3600));
    $last_start = ts_or_zero($row['last_started_at'] ?? null);
    $last_finish = ts_or_zero($row['last_finished_at'] ?? null);
    $base = max($last_start, $last_finish);

    if ($base <= 0) {
        return true;
    }
    return (time() - $base) >= $period;
}

mgr_log('boot: loaders_dir=%s loop=%ds heartbeat=%ds sql_dir=%s', $loaders_dir, $loop_seconds, $heartbeat_seconds, $manager_sql_dir);

ensure_exchange_dbs();
ensure_loader_runtime_dirs();
ensure_schema();
heartbeat('running', 0, 'bootstrapped');

while (!$stop_requested) {
    $rows = fetch_control_rows();

    foreach ($rows as $row) {
        $loader_key = strval($row['loader_key']);
        $enabled = intval($row['enabled'] ?? 0) === 1;
        if (!$enabled) {
            continue;
        }

        if (isset($running[$loader_key])) {
            continue;
        }

        if (!is_due($row)) {
            continue;
        }

        $job = start_loader($row);
        if ($job) {
            $running[$loader_key] = $job;
        }
    }

    foreach (array_keys($running) as $loader_key) {
        $job = $running[$loader_key];
        $proc = $job['proc'];
        $status = proc_get_status($proc);
        $exit_code = -1;
        $timed_out = false;

        if (!$status['running']) {
            $exit_code = intval($status['exitcode']);
        } elseif ((time() - intval($job['started'])) > intval($job['timeout'])) {
            $timed_out = true;
            proc_terminate($proc);
            usleep(300000);
            $status = proc_get_status($proc);
            if ($status['running']) {
                proc_terminate($proc, 9);
            }
            $exit_code = 124;
        }

        if (!$status['running'] || $timed_out) {
            $error = $timed_out ? 'timeout' : '';
            mark_loader_finish($loader_key, $exit_code, $error);
            proc_close($proc);
            unset($running[$loader_key]);
            mgr_log('finished %s exit=%d%s', $loader_key, $exit_code, $timed_out ? ' timeout' : '');
        }
    }

    heartbeat('running', count($running));
    sleep($loop_seconds);
}

mgr_log('stop requested, active jobs=%d', count($running));
foreach ($running as $loader_key => $job) {
    $proc = $job['proc'];
    proc_terminate($proc);
    usleep(300000);
    $status = proc_get_status($proc);
    if ($status['running']) {
        proc_terminate($proc, 9);
    }
    mark_loader_finish($loader_key, 143, 'manager stop');
    proc_close($proc);
}

heartbeat('stopped', 0, 'manager stopped');
mgr_log('stopped');

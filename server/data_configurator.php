<?php
require_once 'lib/common.php';
require_once 'lib/esctext.php';
require_once 'lib/db_tools.php';
require_once 'lib/db_config.php';

ob_implicit_flush();
error_reporting(E_ERROR | E_WARNING | E_PARSE);
mysqli_report(MYSQLI_REPORT_ERROR);

if (!function_exists('h')) {
  function h(string $v): string {
    return htmlspecialchars($v, ENT_QUOTES | ENT_SUBSTITUTE, 'UTF-8');
  }
}

$allowed_exchanges = ['binance', 'bybit', 'bitfinex', 'bitmex'];
$exchange = strtolower((string)rqs_param('exchange', 'bitmex'));
if (!in_array($exchange, $allowed_exchanges, true)) {
    $exchange = 'bitmex';
}

$mysqli = init_remote_db($exchange);
if (!$mysqli instanceof mysqli_ex) {
    error_exit('FATAL: failed DB connection for exchange %s', $exchange);
}

if (!$mysqli->table_exists('ticker_map') || !$mysqli->table_exists('data_config')) {
    error_exit('FATAL: expected tables ticker_map/data_config do not exist in DB %s', $exchange);
}

$message = '';
$message_type = 'ok';

function q_int_map(string $name): array {
    $source = $_POST[$name] ?? [];
    if (!is_array($source)) {
        return [];
    }

    $result = [];
    foreach ($source as $id => $val) {
        $iid = intval($id);
        if ($iid > 0) {
            $result[$iid] = 1;
        }
    }
    return $result;
}

function apply_minimal_defaults(mysqli_ex $mysqli): int {
    $affected = 0;
    $mysqli->try_query('UPDATE data_config SET load_candles = 0, load_depth = 0, load_ticks = 0');
    $affected += max(0, intval($mysqli->affected_rows));

    $rows = $mysqli->select_rows('id', 'ticker_map', 'WHERE pair_id IN (1,3)', MYSQLI_ASSOC);
    if (is_array($rows)) {
        foreach ($rows as $row) {
            $id = intval($row['id'] ?? 0);
            if ($id <= 0) {
                continue;
            }
            $mysqli->try_query(
                "INSERT INTO data_config (id_ticker, load_candles, load_depth, load_ticks) VALUES ($id, 1, 0, 0) " .
                "ON DUPLICATE KEY UPDATE load_candles = 1, load_depth = 0, load_ticks = 0"
            );
            $affected += max(0, intval($mysqli->affected_rows));
        }
    }

    return $affected;
}

if ('POST' === strtoupper($_SERVER['REQUEST_METHOD'] ?? 'GET')) {
    $action = strval($_POST['action'] ?? '');
    $mysqli->try_query('START TRANSACTION');

    try {
        if ('preset_minimal' === $action) {
            $affected = apply_minimal_defaults($mysqli);
            $message = sprintf('Minimal preset applied for %s. Changed rows: %d', $exchange, $affected);
        } elseif ('save' === $action) {
            $candles = q_int_map('candles');
            $depth = q_int_map('depth');
            $ticks = q_int_map('ticks');

            $rows = $mysqli->select_rows('id', 'ticker_map', 'ORDER BY id', MYSQLI_ASSOC);
            $changed = 0;
            if (is_array($rows)) {
                foreach ($rows as $row) {
                    $id = intval($row['id'] ?? 0);
                    if ($id <= 0) {
                        continue;
                    }

                    $c = isset($candles[$id]) ? 1 : 0;
                    $d = isset($depth[$id]) ? 1 : 0;
                    $t = isset($ticks[$id]) ? 1 : 0;

                    $mysqli->try_query(
                        "INSERT INTO data_config (id_ticker, load_candles, load_depth, load_ticks) VALUES ($id, $c, $d, $t) " .
                        "ON DUPLICATE KEY UPDATE load_candles = $c, load_depth = $d, load_ticks = $t"
                    );
                    $changed += max(0, intval($mysqli->affected_rows));
                }
            }

            $message = sprintf('Config saved for %s. Changed rows: %d', $exchange, $changed);
        } else {
            throw new RuntimeException('Unknown action');
        }

        $mysqli->try_query('COMMIT');
    } catch (Throwable $e) {
        $mysqli->try_query('ROLLBACK');
        $message_type = 'error';
        $message = 'Save failed: ' . h($e->getMessage());
    }
}

$tickers = $mysqli->select_rows(
    'TM.id, TM.ticker, TM.symbol, TM.pair_id, COALESCE(DC.load_candles, 0) AS load_candles, COALESCE(DC.load_depth, 0) AS load_depth, COALESCE(DC.load_ticks, 0) AS load_ticks',
    'ticker_map AS TM LEFT JOIN data_config AS DC ON DC.id_ticker = TM.id',
    'ORDER BY TM.pair_id, TM.ticker',
    MYSQLI_ASSOC
);
if (!is_array($tickers)) {
    $tickers = [];
}

function checked(mixed $v): string {
    return intval($v) > 0 ? ' checked' : '';
}
?>
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Datafeed Configurator</title>
  <style>
    body { font-family: Segoe UI, Tahoma, sans-serif; margin: 16px; color: #121212; }
    h1 { margin-bottom: 8px; }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 12px; }
    .msg-ok { background: #e8f7e8; border: 1px solid #90c490; padding: 8px; margin-bottom: 12px; }
    .msg-error { background: #fdeaea; border: 1px solid #d08a8a; padding: 8px; margin-bottom: 12px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #d6d6d6; padding: 6px 8px; text-align: left; }
    th { background: #f5f6f8; position: sticky; top: 0; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .pair-main { background: #eef6ff; }
    .muted { color: #666; font-size: 12px; margin-bottom: 10px; }
    .actions { margin-top: 10px; }
  </style>
</head>
<body>
  <h1>Datafeed Configurator</h1>
  <div class="muted">Manage data_config for exchange DBs. Recommended default: candles only for pair_id 1 and 3, ticks disabled.</div>

  <?php if ('' !== $message): ?>
    <div class="<?php echo 'error' === $message_type ? 'msg-error' : 'msg-ok'; ?>"><?php echo $message; ?></div>
  <?php endif; ?>

  <form method="get" class="toolbar">
    <label for="exchange">Exchange:</label>
    <select name="exchange" id="exchange" onchange="this.form.submit()">
      <?php foreach ($allowed_exchanges as $ex): ?>
        <option value="<?php echo h($ex); ?>"<?php echo $exchange === $ex ? ' selected' : ''; ?>><?php echo h($ex); ?></option>
      <?php endforeach; ?>
    </select>
    <noscript><button type="submit">Open</button></noscript>
  </form>

  <form method="post" action="?exchange=<?php echo h($exchange); ?>">
    <div class="toolbar">
      <button type="submit" name="action" value="preset_minimal">Apply Minimal Preset (BTC/ETH candles only)</button>
      <button type="submit" name="action" value="save">Save Current Checkboxes</button>
      <a href="datafeed_report.php?exchange=<?php echo h($exchange); ?>&data=candles">Open Datafeed Report</a>
    </div>

    <table>
      <thead>
        <tr>
          <th class="num">id_ticker</th>
          <th>ticker</th>
          <th>symbol</th>
          <th class="num">pair_id</th>
          <th>candles</th>
          <th>depth</th>
          <th>ticks</th>
        </tr>
      </thead>
      <tbody>
        <?php foreach ($tickers as $row): ?>
          <?php $main = in_array(intval($row['pair_id'] ?? 0), [1,3], true) ? ' pair-main' : ''; ?>
          <tr class="<?php echo trim($main); ?>">
            <td class="num"><?php echo intval($row['id'] ?? 0); ?></td>
            <td><?php echo h(strval($row['ticker'] ?? '')); ?></td>
            <td><?php echo h(strval($row['symbol'] ?? '')); ?></td>
            <td class="num"><?php echo intval($row['pair_id'] ?? 0); ?></td>
            <td><input type="checkbox" name="candles[<?php echo intval($row['id'] ?? 0); ?>]" value="1"<?php echo checked($row['load_candles'] ?? 0); ?>></td>
            <td><input type="checkbox" name="depth[<?php echo intval($row['id'] ?? 0); ?>]" value="1"<?php echo checked($row['load_depth'] ?? 0); ?>></td>
            <td><input type="checkbox" name="ticks[<?php echo intval($row['id'] ?? 0); ?>]" value="1"<?php echo checked($row['load_ticks'] ?? 0); ?>></td>
          </tr>
        <?php endforeach; ?>
      </tbody>
    </table>

    <div class="actions">
      <button type="submit" name="action" value="save">Save Current Checkboxes</button>
    </div>
  </form>
</body>
</html>

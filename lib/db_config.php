  <?php
    $db_configs = [];
    $db_configs['trading'] = ['loader', '********']; 
    $db_configs['datafeed'] = $db_configs['trading'];
    foreach (['binance', 'bitmex', 'bitfinex', 'deribit'] as $db_name)
        $db_configs[$db_name] = $db_configs['datafeed'];

    // for simplicity hosts can be used in /etc/hosts with real IP-addrs
    $db_servers = ['db-local.lan'];
    $db_alt_server = 'db-remote.vpn';
    
    const CLICKHOUSE_HOST = 'db-local.lan';
    const CLICKHOUSE_USER = 'loader';
    const CLICKHOUSE_PASS = '**********';

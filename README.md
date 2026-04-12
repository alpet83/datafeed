# datafeed
Trading data accumulation and distribution scripts for several crypto exchanges. The runtime is PHP CLI centric and uses MariaDB for candle storage and metadata. ClickHouse is used for high-volume tick storage and analytics, but not every loader requires it to start.

Supported exchanges in the current codebase: Binance, Bitfinex, BitMEX, Bybit.

### What you need to install and configure
Apache 2.4+ is optional for the server scripts.

Runtime requirements:
- PHP 8.4+ including CLI
- php-mysqli
- php-mbstring
- php-json
- php-bz2
- Composer
- MariaDB 10.6+
- ClickHouse Server 25.3+ only if tick loaders or ClickHouse-backed analytics are used
 
External libraries must be placed in the same directory or in the PHP include path. In practice the repository expects `vendor/autoload.php` to be reachable during CLI execution.

Required external libraries:
   *  alpet-libs-php
   *  arthurkushman/php-wss
   *  smi2/phpClickHouse          
 
 WARNING: For debugging purposes need access to /tmp and /cache folder (create it!), with free at least 10G space inside. Better using ZRAM LZ4/ZSTD disks for both, with commands like: zramctl -f && zramctl -a lz4 -s 16G /dev/zram1 && mkfs.ext4 /dev/zram1 && mount /dev/zram1 /cache
     
Operational notes:
- Candle loaders store minute candles in `candles__<ticker>` and also rely on daily tables `candles__<ticker>__1D` for volume/control checks.
- Tick loaders can skip their run if `CLICKHOUSE_HOST` is not configured. This is expected runtime behavior, not a fatal startup error.
- History range can be limited with environment variables `DATAFEED_HISTORY_LIMIT_DAYS`, `DATAFEED_HISTORY_MIN_TS`, and exchange-specific variants such as `DATAFEED_HISTORY_LIMIT_DAYS_BNC`.
- Runtime logs are written under `src/logs/` and can grow quickly during repeated loader failures.
 
 Any questions and feedback please send to project chat https://t.me/svcpool_chat
   
# Run in Docker
1. Prepare MariaDB for the exchange databases and the shared `datafeed` database.
2. Prepare ClickHouse only if tick loaders or ClickHouse synchronization are needed.
3. Configure `lib/db_config.php` or mount a project-specific equivalent.
4. Build the container: `docker build --network=host -t datafeed .`
5. Run the container: `screen -qdmS DFSC ./docker-run.sh`
6. Start loaders explicitly, for example: `docker exec -it dfsc php /datafeed/src/bnc_candles_dl.php`

Before enabling a candle loader in cron, verify that:
- `ticker_map` and `data_config` are filled for the target exchange
- the target MariaDB exchange schema exists
- the corresponding daily tables `candles__<ticker>__1D` are present and writable

If ClickHouse is not configured, tick loaders will log a warning and exit without processing. Candle loaders still require MariaDB and their daily helper tables.
 
 

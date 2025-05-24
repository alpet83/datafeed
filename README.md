# datafeed
 Trading data accumulation and distribution scripts for several cryptoexchanges. Most of the scripts are written in PHP, interacting simultaneously with MySQL database and ClickHouse (allows for much faster access).
 
 ### What you need to install and configure:
 Apache 2.4+, PHP 8.1+ including CLI and libapache2-mod, php-mysqli, php-mbstring, php-json, php-bzip, php-composer
 MariaDB 10.6+, ClickHouse Server 25.3+
 
 External libraries must placed in same directory or /usr/share/php or /usr/local/php (means subdirectory vendor with autoload.php must in PHP include path): 
   *  alpet-libs-php
   *  arthurkushman/php-wss
   *  smi2/phpClickHouse          
 
 WARNING: For debugging purposes need access to /tmp and /cache folder (create it!), with free at least 10G space inside. Better using ZRAM LZ4/ZSTD disks for both, with commands like: zramctl -f && zramctl -a lz4 -s 16G /dev/zram1 && mkfs.ext4 /dev/zram1 && mount /dev/zram1 /cache
     
 NOTE: This is a draft version of the file, there will be additions 
 
 Any questions and feedback please send to project chat https://t.me/svcpool_chat
   
 # Run in Docker 
 1. After downloading and configuring docker, install and configure containers with MariaDB Server, ClicksHouse Server.  
 2. Configure user loader in both DB servers, create databases datafeed (better from source cm_tables.sql) and for each used exchange:
   MariaDB:
     `CREATE DATABASE IF NOT EXISTS binance DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;` 
     `GRANT ALL PRIVILEGES ON binance.* TO 'loader'.'%';`
   ClickHouse:
     `CREATE DATABASE IF NOT EXISTS binance Engine = Atomic`  
     `GRANT ALL PRIVILEGES ON binance.* TO 'loader';`
 3. Change directory to datafeed after cloning this repository.
    Run `deploy.sh` for download JPGraph library and edit `db_config.php` - specify password for MariaDB and ClickHouse connectors.
 4. Build cointaier: `docker build --network=host -t datafeed .`
 5. Run container with binding via screen: `screen -qdmS DFSC ./docker-run.sh`
 6. After changing exchange configuration in tables `ticker_map`, `data_config` downloaded can be start interactive: 
    `docker exec -it dfsc php /datafeed/src/bfx_candles_dl.php`
    Script will work only hour, so same command can be added into crontab  
 
 

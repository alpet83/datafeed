# datafeed
 Trading data accumulation and distribution scripts for several cryptoexchanges. Most of the scripts are written in PHP, interacting simultaneously with MySQL database and ClickHouse (allows for much faster access).
 
 ### What you need to install and configure:
 Apache 2.4+, PHP 8.1+ including CLI and libapache2-mod, php-mysqli, php-mbstring, php-json, php-bzip, php-composer
 MariaDB 10.6+, ClickHouse Server 25.3+
 
 External libraries must placed in same directory or /usr/share/php (means subdirectory vendor with autoload.php must in PHP include path): 
   *  alpet-libs-php
   *  arthurkushman/php-wss
   *  smi2/phpClickHouse          
 
 WARNING: For debugging purposes need access to /tmp and /cache folder (create it!), with free at least 10G space inside. Better using ZRAM LZ4/ZSTD disks for both, with commands like: zramctl -f && zramctl -a lz4 -s 16G /dev/zram1 && mkfs.ext4 /dev/zram1 && mount /dev/zram1 /cache
     
 NOTE: This is a draft version of the file, there will be additions 
 
 Any questions and feedback please send to project chat https://t.me/svcpool_chat
   
 
 

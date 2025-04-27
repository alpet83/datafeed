CREATE USER 'loader'@'%' IDENTIFIED VIA mysql_native_password USING '***';
GRANT PROCESS, RELOAD, SHOW DATABASES, SUPER, REPLICATION SLAVE ON *.* TO 'loader'@'%' REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0;

GRANT ALL PRIVILEGES ON  `datafeed`.* TO 'loader'@'%'; 
-- next depends from available databases
GRANT ALL PRIVILEGES ON  `binance`.* TO 'loader'@'%'; 
GRANT ALL PRIVILEGES ON  `bitfinex`.* TO 'loader'@'%'; 
GRANT ALL PRIVILEGES ON  `bitmex`.* TO 'loader'@'%';   
GRANT ALL PRIVILEGES ON  `deribit`.* TO 'loader'@'%';
CREATE USER `loader` IDENTIFIED WITH double_sha1_password BY '******' HOST ANY;

GRANT ALL ON datafeed.* TO `loader`;
GRANT ALL ON binance.* TO `loader`;
GRANT ALL ON bitfinex.* TO `loader`;
GRANT ALL ON bitmex.* TO `loader`;


GRANT SELECT ON system.query_log TO loader;
GRANT SELECT ON INFORMATION_SCHEMA.TABLES TO loader;
GRANT SELECT ON system.parts to loader;


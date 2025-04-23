CREATE TABLE IF NOT EXISTS `ext__@TABLENAME` (
        `ts` DateTime('UTC') NOT NULL,
        `open` Float32 NOT NULL,
        `close` Float32 NOT NULL,
        `high` Float32 NOT NULL,
        `low` Float32 NOT NULL,
        `volume` Float32 NOT NULL )
  ENGINE = MySQL('db-local.lan:3306', 'datafeed', 'bitfinex__candles__adausd', 'trader', 'v0SBkC8SfJmzkM0r')
  SETTINGS connection_pool_size=16, connection_wait_timeout=5, connection_auto_close=true;
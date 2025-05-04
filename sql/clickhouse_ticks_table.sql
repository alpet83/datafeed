CREATE TABLE IF NOT EXISTS `@TABLENAME` (         
        `ts` DateTime64(6, 'UTC') NOT NULL,
        `price` Float32 NOT NULL,
        `amount` Float32 NOT NULL,
        `buy` Bool NOT NULL,
        `trade_no` String NOT NULL,
        INDEX ts ts TYPE minmax GRANULARITY 16 
ENGINE = ReplacingMergeTree(ts)
ORDER BY (trade_no)
PARTITION BY toStartOfMonth(ts)
SETTINGS  replicated_deduplication_window=100;

CREATE TABLE IF NOT EXISTS `@TABLENAME` (         
        `ts` DateTime64(3, 'UTC') NOT NULL,
        `price` Float32 NOT NULL,
        `amount` Float32 NOT NULL,
        `buy` Bool NOT NULL,
        `trade_no` String NOT NULL)
ENGINE = ReplacingMergeTree(ts) -- если попадет тик с округленным временем, ему жопа после оптимизации
ORDER BY (trade_no)
PARTITION BY toStartOfMonth(ts)
SETTINGS  replicated_deduplication_window=100;

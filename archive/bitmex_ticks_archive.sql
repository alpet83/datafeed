CREATE TABLE bitmex.ticks__archive ( `ts` DateTime64(3, 'UTC'), `symbol` String, `price` Float32, `amount` Float32, `buy` Bool, `trade_no` String ) ENGINE = ReplacingMergeTree(ts) PARTITION BY toStartOfMonth(ts) ORDER BY (symbol, trade_no, ts) SETTINGS index_granularity = 8192;



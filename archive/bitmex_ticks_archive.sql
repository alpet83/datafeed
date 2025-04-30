CREATE TABLE bitmex.ticks__archive ( `ts` DateTime64(6, 'UTC'), `symbol` LowCardinality(String), `price` Float32, `amount` Float32, `buy` Bool, `trade_no` String, INDEX symbol symbol TYPE set(0) GRANULARITY 16) ENGINE = ReplacingMergeTree PARTITION BY toStartOfMonth(ts) ORDER BY (ts, trade_no, symbol) SETTINGS index_granularity = 8192;



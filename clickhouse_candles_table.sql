CREATE TABLE IF NOT EXISTS @TABLENAME (
        ts DateTime('UTC') NOT NULL,
        `open` Float32 NOT NULL CODEC(Delta, ZSTD),
        `close` Float32 NOT NULL CODEC(Delta, ZSTD),
        `high` Float32 NOT NULL CODEC(Delta, ZSTD),
        `low` Float32 NOT NULL CODEC(Delta, ZSTD),
        `volume` Float32 NOT NULL,
        `flags` UInt32 DEFAULT 0 CODEC(Delta, ZSTD),
        `orders` Int16 DEFAULT 0 CODEC(Delta, ZSTD))         
  ENGINE = ReplacingMergeTree() 
  ORDER BY ts
  PARTITION BY toYYYYMM(ts)
  SETTINGS  replicated_deduplication_window=100;

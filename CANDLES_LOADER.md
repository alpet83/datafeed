### DRAFT DOCUMENT

If the set of tickers is large enough, you will need gigabytes of disk to store the database.

Current candle-loader implementations exist for Binance, Bitfinex, BitMEX, and Bybit.

Operational behavior to keep in mind:
- Candle loaders primarily write minute candles into MariaDB exchange schemas.
- They also read and update helper daily tables named `candles__<ticker>__1D`.
- These daily tables are part of the validation and block-repair flow, not optional metadata.
- If a daily table is missing, the loader can repeatedly fail during daily import or block scan even when the main minute table exists.

The loader scripts (`*_candles_dl.php`) are typically started from cron once per hour after database initialization.

ClickHouse is not a hard runtime prerequisite for all candle loader scenarios, but MariaDB and the daily helper tables are.
  
  
How to fast download ticks for BitMEX?
1. Use any bittorrent client and download archive for interested year
2. In Linux shell run bmx_import_csv.php '[year]/*.csv.gz' and wait for complete (may need hours!). Also u can specify mask with full month like '2024/202412*.csv.gz'
3. Be ready for increase disk usage by ClickHouse.

## HARDCORE PROCESS => HARD CODE
Due trades in archive have random trdMatchID (looks as generateUUIDv4 result in ClickHouse), storing it in tables with ReplacingMergeTree (ORDER BY trade_no) produces totaly userialized trash on disk. Any sequential access will delayed due sorting "by timestamp". For solve this problem in script was realized alternate trade_no generation with keeping the matching idx & id in the dedicated table. 
This all UGLY, and solution have big prices: very long time import, too much memory usage (for some days over 2GB!!!). Matching table eats additional gigabytes in DB, but it needs only for repair. For a little speedup while importing group of archives (full year/month) script using fork self for 2 "workers", why processing different files in parallel. 

## Hardware Requirements 
For full and relatively fast import whole ticks archive, I recommend at least 4 core CPU + 8GB RAM + 100GB free space for ClickHouse. It's a real bigdata, about 1.3 billion rows. Be ready spent hours for total import. But if you try using for same result only REST API, probably it will cost for months!
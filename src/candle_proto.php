<?php
    include_once('lib/common.php');
    include_once('lib/esctext.php');
    include_once('lib/db_tools.php');

    require_once("loader_proto.php");
    require_once("blocks_loader.php");

    # NOTE: all timestamps are in seconds, rounded
    const HISTORY_MIN_TS = '2013-01-01 00:00:00'; // minimal history start

    const CANDLE_FLAG_RTMS      = 0x0400;  // realtime sync
    const CANDLE_FLAG_DUMMY     = 0x1000;
    

    class  CandlesCache extends DataBlock  {

        public $interval = 60; // in seconds

        public $mark_flags = 0;


        public  function AddRow(int $t, float $open, float $close, float $high, float $low, float $volume, int $flags = 0) {            
            $rec = [$open, $close, $high, $low, $volume]; 
            if ($this->interval < SECONDS_PER_DAY)
                $rec []= $flags | $this->mark_flags;

            return $this->SetRow($t, $rec);            
        }

        public function Export(float $filter = 0): array {
            $result = [];
            foreach ($this->cache_map as $key => $rec) {
                if ($rec[CANDLE_VOLUME] < $filter) continue;
                $result[$key] = $rec;
            }
            
            return $result; // clone
        }

        public function FormatRow (int $key){
            global $mysqli_df;
            $row = $this->cache_map[$key];
            if (!is_array($row)) return "#ERROR: wrong offset #$key";
            $ts = gmdate('Y-m-d H:i:00', $key);
            $ts = str_replace(' ', 'T', $ts);            
            array_unshift($row, $ts); 
            $cols = array_keys($row);
            $row = $mysqli_df->pack_values($cols, $row, "'");
            return "($row)"; // for INSER INTO
        }

        protected function AddDummy(int $t) {
            $best = $t;
            if (isset($this->cache_map[$t])) return; // already exists
            $best_dist = SECONDS_PER_DAY;
            foreach ($this->cache_map as $tk => $rec) {                                
                $dist = $tk - $best;
                if (abs($dist) < $best_dist) {
                    $best = $tk;
                    $best_dist = abs($dist);
                }
            }            
            $flags = CANDLE_FLAG_DUMMY;
            if ($this->lbound == $t) 
                $flags |= CANDLE_OPEN + 1;  

            if (SECONDS_PER_DAY - 60 == $t) 
                $flags |= CANDLE_CLOSE + 1;

            $rec = [0, 0, 0, 0, $flags];
            if (isset($this->cache_map[$best])) 
                $rec = $this->cache_map[$best]['price'];                        
            $this->cache_map[$t] = $rec;
        }    

        public function IsFullFilled(): bool {             
            if ($this->target_volume > 0)
                return  $this->target_volume == $this->CalcSummary(CANDLE_VOLUME);
            return parent::IsFullFilled();
        }


        public function SetFlags(int $key, int $flags, bool $set = true) {
            if (isset($this->cache_map[$key])) {
                $row = $this->cache_map[$key];
                if (!isset($row[CANDLE_FLAGS]))
                    $row[CANDLE_FLAGS] = 0;

                if ($set) 
                    $row[CANDLE_FLAGS] |= $flags;
                else
                    $row[CANDLE_FLAGS] &= ~$flags;
                $this->cache_map[$key] = $row;
            }
            return 0;
        } 

        public function SetRow(mixed $key, array $row): int {                   
            verify_timestamp($key, 'CandlesCache->SetRow');
            $key = floor($key / $this->interval) * $this->interval; 
            
            if (isset($this->cache_map[$key])) $this->duplicates ++;   
            if ($this->interval < SECONDS_PER_DAY && count($row) < 6)
                $row []= $this->mark_flags; // flags
            
            $this->cache_map[$key] = $row;            
            $this->set_filled($key);
            return count($this->cache_map);            
        }
    
        public function Store(CandlesCache $target) {
            $min = $target->lbound;
            $max = $target->rbound;
            foreach ($this->cache_map as $t => $rec) {                
                if ($t >= $min && $t < $max)                     
                    $target->SetRow($t, $rec);                
            }
            $target->OnUpdate();
        }
        public function OnUpdate() {
            $this->min_fills = 60;
            parent::OnUpdate();
        }

    };


    function sqli_df(): ?mysqli_ex {
        global $mysqli_df;
        return $mysqli_df;
    }

    abstract class CandleDownloader
        extends BlockDataDownloader {       

        public      $current_interval = 60; // используется при обработке импорта
        
        public      $last_import = '';

        protected   $count_db = 0;
        protected   $exports = 0;
        protected   $is_funding = false;

        protected  $daily_map = [];


        
        private    int $lazy_rcv = 0;        


        public   function __construct(DownloadManager $mngr, stdClass $ti) {
            $this->import_method = 'ImportCandles';
            $this->load_method = 'LoadCandles';
            $this->block_class = 'CandlesCache';
            $this->data_name = 'candles';
            $this->table_proto = 'mysql_candles_table.sql';            
            parent::__construct($mngr, $ti);     
            $this->cache = new CandlesCache($this);            
            $this->time_precision = 1;               
        }

        public   function  get_manager(): ?CandleDownloadManager {
            return $this->manager instanceof CandleDownloadManager ? $this->manager : null;
        }

        public function CorrectTable (){
            global $chdb;
            $mysqli = sqli();
            if (!$mysqli->table_exists($this->table_name)) {
                log_cmsg("~C31#WARN:~C00 table %s not exists, trying to create", $this->table_name);
                $this->CreateTables();
            }

            parent::CorrectTable();           
            if (strlen($this->table_create_code) < 10)
                throw new Exception("~C91#ERROR:~C00 table {$this->table_name} code not retrieved: ".var_export($this->table_create_code, true));
            
            $query = "ALTER TABLE {$this->table_name} ADD `flags` INT UNSIGNED NOT NULL DEFAULT '0' AFTER `volume`";                                    
            if ( !str_in($this->table_create_code, 'flags')) {  // в таблице нет 7-ой колонки?
                log_cmsg("~C93#TABLE_UPGRAGE(MySQL):~C00 current code: %s", $this->table_create_code);
                if ($mysqli->try_query($query))
                    log_cmsg("~C92#TABLE_UPGRADE(MySQL):~C00 added column `flags` to %s", $this->table_name);            
            }

            $query = "ALTER TABLE {$this->table_name} ADD COLUMN `flags` UInt32 DEFAULT 0 AFTER `volume`";
            if ($chdb && strlen($this->table_create_code_ch) > 10 
                      && !str_in($this->table_create_code_ch, 'flags') && $stmt = $chdb->write($query)) {
                log_cmsg("~C93#TABLE_UPGRAGE(ClickHouse):~C00 current code: %s", $this->table_create_code_ch);
                $msg = $stmt->isError () ? format_color("~C91#TABLE_UPGRADE_ERROR:~C00 %s", $stmt->dump()) 
                                         : format_color("~C92#TABLE_UPGRADE(ClickHouse):~C00 added column `flags` to %s", $this->table_name);            
                log_cmsg($msg);
            }
        }    

  
        public function FlushCache() {
            $this->SaveToDB($this->cache, true);                        
        }

        public function CleanPoor() {
            $mysqli = sqli();      
            $rows = $mysqli->select_rows('MIN(ts), COUNT(close)', $this->table_name, "GROUP BY YEARWEEK(ts) ORDER BY `ts`");
            $clean_from =  0;
            $year_max = strtotime('2022-01-01 00:00:00');
            if (is_array($rows)) 
                foreach ($rows as $row) {
                    $ts = strtotime($row[0]);
                    if ($row[1] < 10 && $ts < $year_max) 
                        $clean_from = max($clean_from, $ts);
                }
            if ($clean_from > 0) {
                $clean_from += SECONDS_PER_DAY * 7; // add week offset         
                $ts_from = gmdate(SQL_TIMESTAMP, $clean_from);
                if ($mysqli->try_query("DELETE FROM {$this->table_name} WHERE `ts` < '$ts_from';")) 
                    log_cmsg("~C91#WARN:~C00 performed table %s cleanup due inconsistent data before %s, affected rows = %d ", $this->table_name, $ts_from, $mysqli->affected_rows);
            }
        }

        /**
         * CacheClean удаляет из кэша старые данные, оставляя только последние $limit
         */

        protected function CacheClean(array $keys) {
            foreach ($keys as $key)
                unset($this->cache_map[$key]);
        }
        

        

        public function HistoryFirst(): bool|int {
            $def_first = false;
            $mgr = $this->get_manager();      
            $cache = &$mgr->start_cache;
            $block = $this->first_block;
            $block->index = -2;

            if ($block->min_avail > $block->lbound)
                return $block->min_avail;

            $fname = "{$mgr->tmp_dir}/start_cache.json";            
            if (file_exists($fname) && (!is_array($cache) || 0 == count($cache))) {
                $cache = file_load_json($fname, null, true);
                $mgr->start_cache = is_array($cache) ? $cache : [];
                $cache = &$mgr->start_cache;
            }    

            if (isset($cache[$this->ticker]))  
                return $cache[$this->ticker];            

            $start = format_ts(EXCHANGE_START_SEC);
            log_cmsg("~C97#HISTORY_FIRST:~C00 exchange start day %s", $start);            
            

            if (is_object($this->cache) && count($this->cache) > 0)
                $this->SaveToDB($this->cache); // flush 

            $cache = null;    
            $head = $this->LoadCandles($block, $start, false, 1000); // для пропуска флуда как у Битмекс... надо больше свечей набрать                        
            if (is_array($head) && count($head) >= 1) 
                $cache = $this->ImportCandles($head, $this->symbol, false);
            
            if (is_object($cache)) {                
                $this->history_first = $cache->firstKey();  // first candle in seconds
                $block->lbound = floor_to_day($this->history_first);
                $block->rbound = $block->lbound  + SECONDS_PER_DAY - 1; // end of first day
                $block->min_avail = $this->history_first;                
                $block->max_avail = $block->rbound;
                $cache[$this->ticker] = $this->history_first;                                                        
                file_put_contents($fname, json_encode($cache));    
                log_cmsg("~C93#DBG:~C00 first block detected as %s", strval($block));
                $this->SaveToDB($this->cache); // flush 
                return $this->history_first;
            }

            $info = '';
            if (is_array($head) && count($head) > 0)
                $info = substr(var_export(array_pop($head), true), 0, 1000); // last record
            else
                $info = substr(var_export($head, true), 0, 1000);            

            
            log_cmsg("~C91#WARN(HistoryFirst):~C00 no data for %s?: %s",    
                                $this->symbol, $info);

            return 0;            
        }

        abstract public   function    ImportCandles(array $data, string $source, bool $direct_sync = true): ?CandlesCache;
        public function ImportWS(mixed $data, string $context): int {
            if (is_string($data))
                $data = json_decode($data);
            if (is_array($data)) {
                $list = $this->ImportCandles($data, "WebSocket$context");
                return is_object($list) || is_array($list) ? count($list) : 0;
            }
            return 0;
        }
        protected function InitBlocks(int $start, int $end) {            
            if ($this->data_flags & DL_FLAG_HISTORY == 0 || $this->loaded_full() || $this->nothing_to_download) 
                return;

            if ($end < strtotime(HISTORY_MIN_TS)) {
                log_cmsg("~C91#ERROR:~C00 invalid end timestamp %d: %s", $end, gmdate(SQL_TIMESTAMP, $end));
                $end = time(); 
            }

            if ($start < strtotime(HISTORY_MIN_TS) && !$this->initialized) {
                log_cmsg("~C31#WARN_INIT_BLOCKS:~C00 start timestamp %d: %s, will be increased", $start, gmdate(SQL_TIMESTAMP, $start));
                $start = strtotime(HISTORY_MIN_TS); 
            }              
            
            $this->blocks = [];
            $this->blocks_map = [];

            $mysqli_df = sqli_df();

            $block = $this->last_block;
            $day = date('Y-m-d'); // only today
            $col = 'UNIX_TIMESTAMP(ts)';
            $strict = "WHERE date(ts) = '$day'";
            $block->min_avail = $mysqli_df->select_value($col, $this->table_name, "$strict ORDER BY ts") ?? $block->min_avail;
            $block->max_avail = $mysqli_df->select_value($col, $this->table_name, "$strict ORDER BY ts DESC") ?? $block->max_avail;
            $block->min_avail = max($block->lbound, $block->min_avail);

            $voids = $this->ScanIncomplete($end);            
            log_cmsg("~C97#INIT_BLOCKS:~C00 for symbol %s will used range %s .. %s, expected download %d blocks, detected voids = %d, last block %s", $this->symbol,
                        gmdate(SQL_TIMESTAMP, $start), gmdate(SQL_TIMESTAMP, $end), 
                        count($this->blocks), $voids, strval($block));

            $this->initialized = true;
            
        }


        public function Elapsed () { 
            $newest = max($this->newest_ms, EXCHANGE_START);
            return max(0, pr_time() - $newest / 1000); 
        }        

        /**
         * Summary of LoadCandles загрузка минутных свечей через RestAPI
         * @param DataBlock $block
         * @param string $ts_from
         * @param bool $backward_scan - сканирование взад или вперед 
         * @param int $limit - ограничение на количество загружаемых свечей
         * @return void
         */
        abstract public function LoadCandles(DataBlock $block, string $ts_from, bool $backward_scan = true, int $limit = 5 * 24 * 60): ?array; 
        /**
         * загрузка всех дневных свечей подряд, из БД или API. Они будут использоваться для контроля объемов
        */
        public function LoadDailyCandles(int $per_once = 1000, bool $from_DB = true): ?array {
            global $mysqli_df, $chdb;
            $prev_name = "{$this->table_name}_1D";
            $table_name = "{$this->table_name}__1D";

            $rename = "RENAME TABLE IF EXISTS $prev_name TO $table_name";
            if ($mysqli_df->table_exists($prev_name)) {                
                if (!$mysqli_df->try_query($rename))
                    $mysqli_df->try_query("DROP TABLE IF EXISTS $prev_name"); // can't rename, due exists with target name, so not need
            }
            if ($chdb)
                $chdb->write($rename);
            
            if ($from_DB) {                
                if (!$mysqli_df->table_exists($table_name)) {
                    $mysqli_df->try_query("CREATE TABLE IF NOT EXISTS $table_name LIKE {$this->table_name}"); // void create                    
                    $mysqli_df->try_query("ALTER TABLE $table_name REMOVE PARTITIONING");
                    $mysqli_df->try_query("ALTER TABLE $table_name DROP `flags`"); // not need flags column
                    return null;
                }
                
                log_cmsg("~C93#DAILY_CACHE:~C00 trying load from %s", $table_name);
                $params = $mysqli_df->is_clickhouse() ? 'FINAL' : '';
                $map = $mysqli_df->select_map('UNIX_TIMESTAMP(ts),open,close,high,low,volume', $table_name, $params, MYSQLI_NUM);
                if (0 == count($map))
                   log_cmsg("~C91#WARN:~C00 no data in %s", $table_name);                    
                else
                   ksort($map);
                return $map;                
            }
            return null;
        }

        public function OnBlockComplete(DataBlock $block) {
            if ($block->code == $block->reported) return;            
            $this->loaded_blocks ++;                       
            $block->reported = $block->code;
            $filled = '~C43~C30['.$block->format_filled(60).']~C00';                    
            $lag_left = $block->min_avail - $block->lbound;
            $index = $block->index;
            if ($index >= 0 && $lag_left > 60) {
                $block->FillDummy();
                $block->Store($this->cache); // save dummies
            }
            $this->last_error = '';
            $prefix = '~C93#BLOCK_COMPLETE';
            if ($block->covers($this->history_first)) {
                $this->head_loaded = true;
                $prefix = '~C04~C93#HISTORY_COMPLETE';
            }
            $symbol = $this->symbol;
            $block->OnUpdate();
            $volume = $block->CalcSummary(CANDLE_VOLUME);
            
            $ftk = $block->firstKey();
            $ltk = $block->lastKey();            
            if (count($block) > 0) {            
                $block->SetFlags($ftk, CANDLE_OPEN + 1); 
                if ($block->index >= 0)  // маркировать последние свечи можно лишь в завершенных блоках         
                    $block->SetFlags($ltk, CANDLE_CLOSE + 1);  
                // TODO: add high & low detection for marking
            }

            $vdiff = $block->target_volume - $volume;
            // пороговое значение 0.1%, т.к. текстовые данные с преобразованием в float могут накопить погрешность
            $msg = '';
            $tmp_dir = $this->manager->tmp_dir.'/blocks'; 
            $loaded = count($block);
            check_mkdir($tmp_dir);

            if ( abs($vdiff) < $block->target_volume * 0.001 || -1 == $block->index && $loaded <= 1440) 
                $msg = format_color("$prefix($symbol/$index):~C00 %s, lag left %d, summary volume = target %s, filled in session %d: %s ", 
                            strval($block), $lag_left, $block->info, format_qty($volume), 
                            $block->fills, $filled);       
            else  {
                $day = $this->daily_map[$block->lbound] ?? [];
                $day_close = $day[CANDLE_CLOSE] ?? 0;
                
                $last = $block->last() ?? [];
                
                $msg = format_color("~C103~C31 #BLOCK_COMPLETE_WARN($symbol/$index): ~C00 %s have target volume %s vs calculated %s (diff %s), daily close = %.5f and last candle close = %s:%.5f, loaded %d", 
                                strval($block),
                                format_qty($block->target_volume),  format_qty($volume), format_qty($vdiff),
                                $day_close,
                                color_ts($ltk), $last[CANDLE_CLOSE] ?? 0, $loaded);                                         
                $dump = [];
                foreach ($block->Export(0.000001) as $tk => $rec) 
                    $dump [] = format_ts($tk).' = '.json_encode($rec);
                $dump []= 'VOLUME ROW: '.json_encode($block->saldo_source);
                $dump []= "VOLUME DIFF: expected {$block->target_volume}, achieved $volume, diff = ".format_qty($vdiff);
                $dump []= '';
                if ($block->key <= '2016-02-15' && str_in($tmp_dir, 'bmx')) {
                    if (sqli()->try_query("UPDATE {$this->table_name}__1D SET volume = $volume WHERE DATE(ts) = '{$block->key}' ")) { // BitMEX invalid data patch
                        log_cmsg("~C31 #BITMEX_PATCH:~C00 affected %d rows", sqli()->affected_rows);                    
                        $this->daily_map[$block->key]->volume = $volume;                        
                    }
                }

                file_put_contents("$tmp_dir/bad-{$this->ticker}-{$block->key}.txt", implode("\n", $dump));
            }
            $prefix = $block->recovery ? 'recovery' : 'completed';            
            file_add_contents("$tmp_dir/$prefix-{$this->ticker}.log", tss()."$msg\n");                
            log_cmsg($msg);            
            $block->Reset();                        
        }   

        protected function OnCacheUpdate(DataBlock $default, DataBlock $cache) {
            $covered = [];
            $size = count($cache);
            if (0 == $size) return;
            $miss = [];
            $days = [];
            $block = null;
            $full_map = []; // full map of blocks
            foreach ($this->blocks as $block) {
                $cursor = $block->lbound;
                while ($cursor <= $block->rbound) {
                    $full_map [$cursor] = $block;
                    $cursor += SECONDS_PER_DAY;
                }
            }

            $keys = $cache->keys;
            sort($keys);

            foreach ($keys as $tk) { 
                $rec = $cache[$tk];
                $this->oldest_ms = min($this->oldest_ms, $tk * 1000); // in ms
                $this->newest_ms = max($this->newest_ms, $tk * 1000); // in ms
                $day = date('Y-m-d', $tk);
                $days [$day] = ($days[$day] ?? 0) + 1;                
                $block = $full_map[$tk] ?? $default;            
                if ($block && $block->covers($tk) && BLOCK_CODE::FULL != $block->code)  {
                    $covered [$day] = 1;
                    $block->SetRow($tk, $rec);
                }
                elseif ($block)
                    $miss [$day] = "$tk ? ".strval($block); 
            }   

            if (0 == count($this->blocks_map)) return;

            $days = array_flip($days);
            $keys = array_keys($this->blocks_map);
            $bc = count($keys);
            $keys = array_slice($keys, 0, 10);
            $info = ', no day matched with '.json_encode($days). " vs $bc:".json_encode($keys);
            if ($block)
                $info = ',last matched '.strval($block);

            log_cmsg("~C93#DBG_CACHE_UPDATED:~C00 with %d candles, %d duplicates, %d blocks covered$info, miss %s, full map size %d", 
                       $size, $cache->duplicates, count($covered), json_encode($miss), count($full_map));         
        }

        public function CheckCandle(mixed $tk, array $row, array &$errs = null): array {
            [$o, $c, $h, $l] = $row;
            $errs = [];
            $max = max($o, $c, $l);
            if ($h < $max)  
              foreach ($row as $i => $v)
                if ($v > $h && $i <= 3) { 
                    $errs []= "~C96#$i ~C95 $v~C00 >~C91 HIGH~C95 $h~C00";          
                    swap_values($row[$i], $h);                
                }    

            $min = min($o, $c, $h);           

            if ($l > $min) 
               foreach ($row as $i => $v)
                if ($v < $l && $i <= 3) { 
                    $errs []= "~C96#$i ~C95 $v~C00 <~C94 LOW~C95 $l~C00";          
                    swap_values($row[$i], $h);                
                }    

            $row[2] = $h;
            $row[3] = $l;
            if (is_string($tk))
                $tk = strtotime($tk);
            if ($tk < EXCHANGE_START_SEC)
                throw new ErrorException("#ERROR: invalid timestamp $tk < EXCHANGE_START_SEC ".EXCHANGE_START);                        
            return $row;
        }

        /**
         * Summary of SaveToDB
        * @param array $rows
        * @param array $last
        * @return int
        */
        
        protected function SaveToDB(CandlesCache $cache, bool $reset = true): int {
            // log_cmsg (" #PERF: formated %d rows, trying insert in table %s", $cntr, $this->table);
            // multi-line insert
            global $chdb;
            // recursion possible, due SaveToDB called from InitBlocks for daily candles...
            $mysqli = sqli();

            $values = [];            
            foreach ($cache->keys as $tk)                                               
                $values []= $cache->FormatRow($tk);
            
            $cntr = count($values);      
            if (0 == $cntr) return 0;             

            $addf = '';
            if ($this->current_interval < SECONDS_PER_DAY)
                $addf .= ', `flags`';

            // DEFAULT FORMAT OCHLV!!!
            $query = "INSERT IGNORE INTO {$this->table_name} (`ts`, `open`, `close`, `high`, `low`, `volume` $addf)\n";
            $query .= 'VALUES  '.implode(",\n", $values)."\n";
            $query .= "ON DUPLICATE KEY UPDATE high = VALUES(high), low = VALUES(low), close = VALUES(close), volume = VALUES(volume);\n";            
            
            if (!$mysqli->try_query($query)) 
                throw new ErrorException("~C91#ERROR:~C00 query failed on MySQL server: {$mysqli->error}" );
                       
            $res = $mysqli->affected_rows;
            if ($reset) {
                $mysqli->try_query('COMMIT');
                log_cmsg(" ~C04~C97#CACHE_FLUSHED:~C00 for %s; updated/inserted %d / %d rows in(to) %s", strval($cache), $res, $cntr, $this->table_name);
                $cache->Reset();
            }

            if ($cntr > 1 && 0 == $res) {                
                if ($cntr > 1000) {
                    file_put_contents($this->manager->tmp_dir."/{$this->ticker}-ignored.sql", $query);
                    log_cmsg("~C91#WARN~C00: ignored insert request, rows = %d - dumped", $cntr);
                }  
            }    

            if ($chdb) {
                //  ReplacingMergeTree not supports IGNORE CLAUSE 
                $query = "INSERT INTO {$this->table_name} (`ts`, `open`, `close`, `high`, `low`, `volume` $addf)\n";
                $query .= 'VALUES  '.implode(",\n", $values).";";    
                $stmt = $chdb->write($query); 
                if (!is_object($stmt) || $stmt->isError()) 
                    log_cmsg("~C91#WARN:~C00 query failed on ClickHouse server: %s", $stmt ? $stmt->dump() : '???');                  
            }            

            return $res;
        }
        protected function ScanIncomplete(int $end) {        
            global $chdb;
            $mysqli_df = sqli_df();            
            // TODO: add table with known voids, due exchange maintenance or other reasons
            $tmp = $this->manager->tmp_dir;
            $voids = [];                        
            $daily_map = $this->daily_map;
            if (0 == count($this->daily_map))
                $daily_map = $this->LoadDailyCandles($this->default_limit, true);            

            $count_map = [];
            
            if (0 == count($daily_map)) {
                log_cmsg("~C91#ERROR:~C00 no daily candles loaded for %s, possible there a  problem!", $this->symbol);
                return 0;
            }

            $total_count = $mysqli_df->select_value('COUNT(*)', $this->table_name);
            $total_days = 0;
            // только учтенные данные в БД, которые могли загрузиться не полностью
            if ($total_count > 0) {                                  
                $mysqli_df->raw_rows = [];
                $count_map = $mysqli_df->select_map('DATE(ts) as date, COUNT(*) as count, SUM(volume) as volume', 
                                                $this->table_name, "GROUP BY DATE(ts)", MYSQLI_OBJECT); // select full history map, possible 10 years = 3650+ entries
                $total_days = count($count_map);
                if (0 == $total_days)
                    throw new ErrorException("~C91#ERROR:~C00 void count map loaded, but exists $total_count candles in {$this->table_name}:\n {$mysqli_df->last_query} \n ".
                                                        var_export($mysqli_df->raw_rows, true));  
                $mysqli_df->raw_rows = null;
            }

            $dump = [];
            foreach ($count_map as $date => $row) 
                $dump[$date] = strval($row);

            file_put_contents("$tmp/candles_{$this->ticker}_data.map", print_r($dump, true));
            
            $alloc = 0;
            $block = null;      

            $result = 0;            
            if (!is_array($daily_map) || 0 == count($daily_map)) {
                log_cmsg("~101C97 #FATAL: ~C00 no daily candles for %s, need bug fix!", $this->symbol);
                return 0;
            }
            
            $prev_count = count($this->blocks_map);
            $removed = [];
            foreach ($this->blocks_map as $day => $block) {
                $t = strtotime($day);
                if (!isset($daily_map[$t])) {
                    $removed []= $t;
                    unset($this->blocks_map[$day]);
                }
            }

            log_cmsg("~C96#PERF_SCAN:~C00 scaning for voids in %s, exists %d candles, count map size = %d, daily candles %d",
                        $this->table_name,  $total_count, $total_days, count($this->daily_map));
            if (count($removed) > 0)
                log_cmsg("~C31#WARN:~C00 for %s due no daily candles removed %d / %d blocks", 
                            $this->symbol, count($removed), $prev_count);
            $today = floor_to_day(time());
            $verbose = 0;
            if (0 == count ($count_map))
                $verbose = 3;
            
            $scan_map = $this->daily_map;
            $fill_map = [];
            krsort($scan_map); // from oldest to newest
            foreach ($scan_map as $cursor => $row) {                
                if ($today <= $cursor) continue;   // skip today due is for last block               

                // 0:open, 1:close, 2:high, 3:low, 4:volume
                if (count( $row) > 5)  // is row full and include timestamp -> cutting??
                    log_cmsg("~C31#WARN:~C00 excess column in daily candle %s: %s?", $this->symbol, json_encode($row));                                    
                $ts = date('Y-m-d', $cursor);
                if (!isset($row[CANDLE_OPEN]) || !isset($row[CANDLE_VOLUME])) 
                    throw new ErrorException("~C91#FATAL:~C00 wrong candle ".var_export($row, true ));
                $day_vol = $row[CANDLE_VOLUME]; 
                $day_close = $row[CANDLE_CLOSE];
    
                $volume = 0;
                $count = 0;
                
                $eod = "$ts 23:59:59";
                $exists = false;
                if (isset($count_map[$ts]))  {
                    $exists = true;
                    $count  = $count_map[$ts]->count;
                    $volume = $count_map[$ts]->volume;
                }
                $close = 0;                
                $fill_map [$cursor] = $count;
                
                
                if ($count > 0) 
                    $close = $mysqli_df->select_value('close', $this->table_name, "WHERE ts <= '$eod' ORDER BY `ts` DESC"); 

                $diff = $day_vol - $volume;
                if ($verbose > 1)
                    log_cmsg("~C94#CHECK_DBG:~C00 day %s candle: %s", $ts, json_encode($row));

                if (abs($diff) > $day_vol * 0.001 || $close != $day_close) {  // only fake candles or incomplete                    
                    if ($cursor < EXCHANGE_START_SEC) {
                        log_cmsg("~C91#WARN_OUTBOUND:~C00 have fake data in DB for %s %s?", $this->symbol, color_ts($cursor));
                        continue;
                    }
                    $result ++;                   

                    if ($close != $day_close && $exists)
                        $msg = format_color ("~C33#WRONG_DATA:~C00 for %s have close %f but day candle %s have different. Volume diff %s",
                                             $ts, $close, json_encode($row), format_qty($diff));                                    
                    elseif ($volume > $day_vol * 1.001) {                                              
                        $msg = format_color ("~C31#EXCESS_DATA:~C00 may be wrong instrument was mixed in table. Volume %s > %s (diff = %s) for %s", 
                                             format_qty($volume), format_qty($day_vol), format_qty($diff), $ts);                        
                    }
                    else
                        $msg = format_color ("~C04~C97#INCOMPLETE_CHECK({$this->ticker}):~C00  for %s have volume %s in %d candles instead %s (diff %s).",
                                             $ts, format_qty($volume),
                                             $count, format_qty($day_vol), format_qty($diff));                                           
                    
                    log_cmsg($msg);
                    file_add_contents("$tmp/recovery-{$this->ticker}.log", tss()." $msg\n");
                    if ($block && $block->covers($cursor)) {
                        $block->target_volume += $day_vol; 
                        continue;                    
                    }
                    if ($this->BlocksCount() >= 200) break;

                    $cursor = floor_to_day($cursor); 
                    $rb = $cursor + $this->days_per_block * SECONDS_PER_DAY - 1;                                        
                    $block = $this->CreateBlock( $cursor, $rb);                    
                    $block->target_volume = $day_vol;                     
                    $block->recovery = true;                    
                    
                    $voids []= sprintf('%d %s => %s, TV = %f', $alloc, color_ts( $block->lbound), color_ts( $block->rbound), $day_vol);
                    $alloc ++;
                    $info = strval($block);                
                    if (isset($this->block_load_history[$info])) {
                        log_cmsg("~C91#WARN_SKIP:~C00 block %s %s was loaded in this session, preventing again.", $this->ticker, $info);                        
                        unset($this->blocks_map[$block->key]);
                        $this->blocks = array_values($this->blocks_map);
                        continue;
                    }                    
                    
                    if ($exists && $this->BlocksCount() < $this->max_blocks) {                    
                        // remove for reliable overwrite
                        $query = "DELETE FROM {$this->table_name} WHERE DATE(`ts`) = '$ts'";                        
                        $msg = '';                   
                        $res = $mysqli_df ->try_query($query); // remove excess data                    
                        $data_info = $res ? format_color("Removed %d candles from MySQL", $mysqli_df->affected_rows) : "~C91Failed remove exist data: ~C00{$mysqli_df->error}";                        
                    
                        if ($chdb)
                            try {
                                $stmt = $chdb->write($query);
                                if (!is_object($stmt) ||$stmt->isError())
                                    log_cmsg("~C91#ERROR:~C00 query failed on ClickHouse: %s", $stmt->dump());
                                elseif (is_object($stmt))
                                    $data_info .= ", from ClickHouse some rows removed";
                            } 
                            catch (Exception $e) {
                                log_cmsg("~C91#ERROR:~C00 query failed on ClickHouse: %s", $e->getMessage());
                            }
                        $data_info .= ', planned for reload';
                        log_cmsg("~C93#BLOCK_CLEAN:~C00 $data_info ");
                    }                    
                    
                }                 
                elseif (isset($this->blocks_map[$ts])) {
                    log_cmsg("~C92 #FILLED_GOOD:~C00 block at %s not need for reload, volume = %s, close = %f", $ts, format_qty($day_vol), $close);
                    unset($this->blocks_map[$ts]);
                }                
                elseif ($verbose > 2) 
                    log_cmsg("~C31#SKIP:~C00 ????");
            }

            ksort($fill_map);
            $fill_str = '~C43~C30[';
            foreach ($fill_map as $t => $count)  {
                if ($count >= 1440)
                    $fill_str .= '█';
                elseif ($count >= 1000)
                    $fill_str .= '▓';
                elseif ($count >= 500)
                    $fill_str .= '▒';                    
                elseif ($count > 0)
                    $fill_str .= '░';
                else
                    $fill_str .= '◦';
            }
            $fill_str .= ']~C40';

            log_cmsg("~C97#BLOCKS_CHECK: total incomplete %d, %s", count($this->blocks), $fill_str);
            $vcnt = count($voids);

            if ($vcnt > 0) {
                file_put_contents("$tmp/{$this->ticker}-voids.txt", print_r($voids, true));                
                $voids = array_slice($voids, 0, 20);
                log_cmsg("~C93#INCOMPLETE_DUMP({$this->ticker}):~C00 %d: %s", $vcnt, implode("\t", $voids));
            }
            $this->blocks = array_values($this->blocks_map);          
            $this->blocks = array_slice($this->blocks, -$this->max_blocks);  // limit block count
            if (0 == count($this->blocks) && 0 == $total_count)
                throw new Exception("~C91#ERROR:~C00 no planned blocks for {$this->symbol} with empty history");  

            return  $result;
        }

    } // class CandleDataFeed


?>
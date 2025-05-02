<?php
    include_once('lib/common.php');
    include_once('lib/esctext.php');
    include_once('lib/db_tools.php');

    require_once("loader_proto.php");
    require_once("blocks_loader.php");

    # NOTE: all timestamps are in seconds, rounded    
    const CANDLE_FLAG_RTMS      = 0x0400;  // realtime sync
    const CANDLE_FLAG_DUMMY     = 0x1000;
    

    class  CandlesCache extends DataBlock  {

        public $interval = 60; // in seconds

        public $mark_flags = 0;

        public $stat_rec = null;


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

            $rec = [0, 0, 0, 0, 0, 0];
            if (isset($this->cache_map[$best])) 
                $rec = $this->cache_map[$best];                                    

            $rec[CANDLE_FLAGS] = $flags;
            $this->cache_map[$t] = $rec;
        }    

        public function IsFullFilled(): bool {             
            if ($this->target_volume > 0) {
                $saldo_vol = $this->SaldoVolume();
                return  $this->target_volume <= $saldo_vol;
            }
            return parent::IsFullFilled();
        }

        public function SaldoVolume(): float {
            return $this->CalcSummary(CANDLE_VOLUME);
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
            if ($key < $this->lbound || $key > $this->rbound)
                throw new ErrorException("FATAL: invalid timestamp $key, outbound of range {$this->lbound}..{$this->rbound}");

            $key = floor($key / $this->interval) * $this->interval; 

            if ($this->interval < SECONDS_PER_DAY)
                $row [CANDLE_FLAGS] = $this->mark_flags; // flags

            if (isset($this->cache_map[$key])) 
                $this->duplicates ++;   
            else
                $this->set_filled($key);    

            $this->cache_map[$key] = $row;            
            
            return count($this->cache_map);            
        }
    
        public function Store(CandlesCache $target): int  {
            $min = $target->lbound;
            $max = $target->rbound;
            $stored = 0;
            foreach ($this->cache_map as $t => $rec) {                
                if ($t >= $min && $t < $max) {                      
                    $target->SetRow($t, $rec);                
                    $stored ++;
                }
            }
            $target->OnUpdate();
            return 0;
        }

        public function newest_ms(): int {
            if (0 == count($this)) 
                return $this->lbound_ms;
            return ($this->lastKey() + 59) * 1000;     
        }
        public function oldest_ms(): int {
            if (0 == count($this)) 
                return $this->rbound_ms;
            return $this->firstKey() * 1000;     
        }

        public function OnUpdate() {
            $this->min_fills = 60;
            parent::OnUpdate();
        }
        public function UnfilledBefore(): int {
            return $this->oldest_ms() + 1000;
        }
    };


    function sqli_df(): ?mysqli_ex {
        global $mysqli_df;
        return $mysqli_df;
    }

    abstract class CandleDownloader
        extends BlockDataDownloader {       

        
        public      $last_import = '';

        protected   $count_db = 0;
        protected   $exports = 0;
        protected   $is_funding = false;

        protected  $daily_map = [];

        protected  $sync_map = []; // ClickHouse sync checked

        protected  $ch_count_map = []; // ClickHouse table stats 

        protected  $stats_table = '';

        
        
        private    int $lazy_rcv = 0;        


        public   function __construct(DownloadManager $mngr, stdClass $ti) {
            $this->current_interval = 60;
            $this->import_method = 'ImportCandles';
            $this->load_method = 'LoadCandles';
            $this->block_class = 'CandlesCache';
            $this->data_name = 'candles';
            $this->table_proto = 'mysql_candles_table.sql';                        
            parent::__construct($mngr, $ti);     
            $this->cache = new CandlesCache($this);            
            $this->cache->key = 'main';
            $this->stats_table = $mngr->TableName("stats__{$this->ticker}");
            $this->time_precision = 1;               
        }

        public   function  get_manager(): ?CandleDownloadManager {
            return $this->manager instanceof CandleDownloadManager ? $this->manager : null;
        }

        public function CorrectTable (){
            global $chdb;
            $mysqli = sqli();
            $table_name = $this->table_name;
            if (!$mysqli->table_exists($table_name)) {
                log_cmsg("~C31#WARN:~C00 table %s not exists, trying to create", $this->table_name);
                $this->CreateTables();
            }

            parent::CorrectTable();           
            $query = "DELETE FROM $table_name WHERE (open = 0) AND (volume = 4096)";
            if ($mysqli->try_query($query) && $chdb)  // remove invalid rows
                $chdb->write($query);             
                
            $query = "DELETE FROM $table_name WHERE volume = 0 AND MINUTE(ts) > 0";
            if ($mysqli->try_query($query) && $chdb)
                $chdb->write($query);             

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

        public function CreateTables(): bool {
            global $chdb;
            $template = 'mysql_stats_table.sql';            
            $mysqli_df = sqli_df();
            if ($mysqli_df->table_exists($this->stats_table)) {
                $code = $mysqli_df->show_create_table($this->stats_table);
                if (str_in($code, 'volume_days'))
                    $mysqli_df->query("ALTER TABLE {$this->stats_table} CHANGE `volume_days` `volume_day` FLOAT NULL DEFAULT NULL");
            }
            else
                $this->CreateTable($mysqli_df, $template, $this->stats_table); 

            $res = parent::CreateTables();

            $daily_table = "{$this->table_name}__1D";                
            $exists = $mysqli_df->table_exists($daily_table);
            $query = "CREATE TABLE IF NOT EXISTS $daily_table LIKE {$this->table_name}";
            $res &= $mysqli_df->try_query($query); // void create                    

            if (is_object($chdb)) {                 
                // $this->table_proto_ch = str_replace('mysql', 'clickhouse', $this->table_proto);
                $res &= $this->CreateTable($chdb, $this->table_proto_ch, $daily_table);
            }
            
            if (!$exists) {                         
                $code = $mysqli_df->show_create_table($this->table_name);
                if (str_in($code, 'PARTITION BY'))
                    $mysqli_df->query("ALTER TABLE {$this->table_name} REMOVE PARTITIONING");
                $query = "ALTER TABLE $daily_table DROP IF EXISTS `flags`";
                $mysqli_df->try_query($query); // not need flags column                       
                try {
                    if ($chdb) $chdb->write($query); 
                } catch (Throwable $e) {
                }
            }
            return $res;
        }
  
        public function FlushCache() {
            $this->SaveToDB($this->cache, true);                        
            $this->cache->min_avail = $this->cache->max_avail = 0;
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
            $this->init_count ++;

            $mysqli_df = sqli_df();
            $id_ticker = $this->ticker_info->id_ticker;
            if (null !== $id_ticker)
                $this->data_flags = $mysqli_df->select_value('load_candles', $this->tables['data_config'], "WHERE id_ticker = $id_ticker"); // update

            $block = $this->last_block;
            $day = date('Y-m-d'); // only today
            $col = 'UNIX_TIMESTAMP(ts)';
            $strict = "WHERE date(ts) = '$day'";
            $block->min_avail = $mysqli_df->select_value($col, $this->table_name, "$strict ORDER BY ts") ?? $block->min_avail;
            $block->max_avail = $mysqli_df->select_value($col, $this->table_name, "$strict ORDER BY ts DESC") ?? $block->max_avail;
            $block->min_avail = max($block->lbound, $block->min_avail);

            if ($this->data_flags & DL_FLAG_HISTORY) {
                $voids = $this->ScanIncomplete($end);            
                log_cmsg("~C97#INIT_BLOCKS:~C00 for symbol %s will used range %s .. %s, expected download %d blocks, detected voids = %d, last block %s", $this->symbol,
                            gmdate(SQL_TIMESTAMP, $start), gmdate(SQL_TIMESTAMP, $end), 
                            count($this->blocks), $voids, strval($block));
            }

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

        public function LoadClickHouseStats() {
            global $chdb;
            if (!is_object($chdb)) return;
            $func = __FUNCTION__;
            $table_name = $this->table_name;
            $query = "SELECT DATE(ts) as date, COUNT(*) as count, SUM(volume) as volume FROM $table_name FINAL\n";
            $query .= " GROUP BY DATE(ts)";
            $stmt = $chdb->select($query);            
            $result = [];
            try {
                if (is_object($stmt) && !$stmt->isError()) {
                    $rows = $stmt->rows();
                    file_put_contents("{$this->manager->tmp_dir}/{$this->ticker}-chdb-stats.json", json_encode($rows, JSON_PRETTY_PRINT));
                    foreach ($rows as $row)  {
                        if (!is_array($row) || !isset($row['date'])) break;
                        $date = $row['date'];           
                        $result[$date] = new mysqli_row($row);
                    }
                }
            }
            catch (Throwable $e) {
                log_cmsg("~C31#ERROR($func):~C00 %s", $e->getMessage());
            }            
            return $result;
        }

        public function LoadDailyCandles(int $per_once = 1000, bool $from_DB = true): ?array {
            global $mysqli_df, $chdb;
            $table_name = "{$this->table_name}__1D";
            
            if ($from_DB) {                                
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
            $volume = $block->SaldoVolume();            
            $ftk = $block->firstKey();
            $ltk = $block->lastKey();            
            if (count($block) > 0) {            
                $block->SetFlags($ftk, CANDLE_OPEN + 1); 
                if ($block->index >= 0)  // маркировать последние свечи можно лишь в завершенных блоках         
                    $block->SetFlags($ltk, CANDLE_CLOSE + 1);  
                // TODO: add high & low detection for marking
            }

            $vdiff = $block->target_volume - $volume;
            $vdiff_pp = 100 * $vdiff / max($block->target_volume, $volume, 0.01);
            // пороговое значение 0.1%, т.к. текстовые данные с преобразованием в float могут накопить погрешность
            $msg = '';
            $date = $block->key;
            $tmp_dir = $this->manager->tmp_dir.'/blocks'; 
            $loaded = count($block);
            check_mkdir($tmp_dir);
            $close_info = '';            
            $last_day = floor_to_day($block->rbound);            
            $last = $block->last() ?? [0, 0, 0, 0, 0];
            if (isset($this->daily_map[$last_day])) {
                $day = $this->daily_map[$last_day];
                $last_close = $last[CANDLE_CLOSE] ?? 0;
                $day_close = $day[CANDLE_CLOSE] ?? 0;
                if ($day_close != $last_close) 
                    $close_info = format_color(", daily close %.5f != candle close %s:%.5f", $day_close, $ltk, $last_close);
            }            
            else
                log_cmsg("~C31#WARN:~C00 no info in daily_map [%s] for block %s", color_ts($last_day), strval($block));


            $volume_info = '';    
            if (abs($vdiff_pp) > $this->volume_tolerance)
                $volume_info = format_color(", target volume %s vs calculated %s (diff_pp %.3f) vs avail was %s",
                             format_qty($block->target_volume ?? 0),  format_qty($volume), $vdiff_pp, format_qty($block->avail_volume ?? 0));

            if ('' == $volume_info  && '' == $close_info || -1 == $block->index && $loaded <= 1440) 
                $msg = format_color("$prefix($symbol/$index):~C00 %s, lag left %4d, saldo volume %8s, CR: %s, filled in session %d: %s ", 
                            strval($block), $lag_left, format_qty($volume), $block->info, $block->fills, $filled);       
            else  {                
                $repairs = 0;
                if (is_object($block->stat_rec)) {                    
                    $repairs = $block->stat_rec->repairs + 1;                    
                    $query = sprintf("UPDATE %s SET repairs = $repairs, count_minutes = %d, volume_minutes = %.5f\n", 
                                        $this->stats_table, count($block), $volume);                                        
                    $query .= "WHERE date = '$date'";
                    sqli()->try_query($query);
                }                

                $msg = format_color("~C103~C31 #BLOCK_COMPLETE_WARN($symbol/$index): ~C00 %s have $volume_info $close_info, loaded %d, repairs %d, CR: %s", 
                                strval($block), $loaded, $repairs, $block->info);                                         
                $dump = []; 
                foreach ($block->Export(0.000001) as $tk => $rec) 
                    $dump [] = format_ts($tk).' = '.json_encode($rec);
                $dump []= 'VOLUME ROW: '.json_encode($block->saldo_source);
                $dump []= "VOLUME DIFF: expected {$block->target_volume}, achieved $volume, diff = ".format_qty($vdiff);
                $dump []= "$close_info\n";
                $chg_diff = $volume - $block->avail_volume;

                // патч применяется для старых данных BitMEX, где не все понятно 
                if ( abs($chg_diff) < $volume * 0.001 && 1 == $this->days_per_block && str_in($date, '201')) {                    
                    if (sqli()->try_query("UPDATE {$this->table_name}__1D SET volume = $volume WHERE DATE(ts) = '$date' ")) { // BitMEX invalid data patch
                        log_cmsg("~C31 #DAILY_PATCH:~C00 affected %d rows", sqli()->affected_rows);                                            
                    }
                    $this->daily_map[$last_day][CANDLE_VOLUME] = $volume;
                }                

                file_put_contents("$tmp_dir/bad-{$this->ticker}-$date.txt", implode("\n", $dump));
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
                    $covered [$day] = $block;
                    $block->SetRow($tk, $rec);
                }
                elseif ($block)
                    $miss [$day] = "$tk ? ".strval($block); 
            }   

            if (0 == count($this->blocks_map)) return;
            $ftk = $cache->firstKey();
            $ltk = $cache->lastKey();

            foreach ($covered as $day => $block) {
                if ($block->Covered_by($ftk, $ltk + 59) && $block->IsFullFilled()) {
                    $block->code = BLOCK_CODE::FULL;
                    $block->info = "covered by cache {$cache->key}";
                }
                elseif (BLOCK_CODE::NOT_LOADED == $block->code && count($block) > 0)
                    $block->code = BLOCK_CODE::PARTIAL;                
            }

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
            foreach ($cache->keys as $tk) {                                               
                if ($cache[$tk][CANDLE_VOLUME] > 0 || 0 == $tk % 3600)
                    $values []= $cache->FormatRow($tk);
            }
            
            $cntr = count($values);      
            if (0 == $cntr) return 0;             

            $addf = '';
            if ($this->current_interval < SECONDS_PER_DAY)
                $addf .= ', `flags`';

            // DEFAULT FORMAT OCHLV!!!
            $query = "INSERT INTO {$this->table_name} (`ts`, `open`, `close`, `high`, `low`, `volume` $addf)\n";
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
            $mgr = $this->get_manager();
            // TODO: add table with known voids, due exchange maintenance or other reasons
            $tmp = $mgr->tmp_dir;
            $voids = [];                        
            $daily_map = $this->daily_map;

            if (0 == count($this->daily_map)) 
                $this->daily_map = $daily_map = $this->LoadDailyCandles($this->default_limit, true);
           

            $count_map = [];
            
            if (0 == count($daily_map)) {
                log_cmsg("~C91#ERROR:~C00 no daily candles loaded for %s, possible there a  problem!", $this->symbol);
                return 0;
            }

            $total_count = $mysqli_df->select_value('COUNT(*)', $this->table_name);
            $total_days = 0;
            // только учтенные данные в БД, которые могли загрузиться не полностью
            if ($total_count > 0) {  // для оптимизации, этот запрос можно выполнять раз в день, остальные - из статистики                                 
                $mysqli_df->raw_rows = [];
                $count_map = $mysqli_df->select_map('DATE(ts) as date,MIN(ts) as day_start, MAX(ts) as day_end, COUNT(*) as count, SUM(volume) as volume', 
                                                $this->table_name, "GROUP BY DATE(ts)", MYSQLI_OBJECT); // select full history map, possible 10 years = 3650+ entries
                $total_days = count($count_map);
                if (0 == $total_days)
                    throw new ErrorException("~C91#ERROR:~C00 void count map loaded, but exists $total_count candles in {$this->table_name}:\n {$mysqli_df->last_query} \n ".
                                                        var_export($mysqli_df->raw_rows, true));  
                $mysqli_df->raw_rows = null;
            }

            
            $stats = $mysqli_df->select_map('date, day_start, day_end, count_minutes as count, volume_minutes as volume, volume_day, repairs', $this->stats_table, '', MYSQLI_OBJECT);

            $this->ch_count_map = $this->LoadClickHouseStats(); // same stats as count_map

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
                        $this->table_name,  $total_count, $total_days, count($daily_map));
            if (count($removed) > 0)
                log_cmsg("~C31#WARN:~C00 for %s due no daily candles removed %d / %d blocks", 
                            $this->symbol, count($removed), $prev_count);
            $today_t = floor_to_day(time());
            $verbose = 0;
            if (0 == count ($count_map))
                $verbose = 3;
            
            $scan_start = time();    
            $scan_map = $this->daily_map;
            $fill_map = [];
            $total_volume = 0;
            $total_volume_ch = 0;

            krsort($scan_map); // from oldest to newest
            foreach ($scan_map as $cursor => $row) {                
                if ($today_t <= $cursor) continue;   // skip today due is for last block        
                $elps = time() - $scan_start;
                if (!$mgr->active || $elps >= 60) break;  // stop signal maybe                  
                // 0:open, 1:close, 2:high, 3:low, 4:volume
                if (count( $row) > 5)  // is row full and include timestamp -> cutting??
                    log_cmsg("~C31#WARN:~C00 excess column in daily candle %s: %s?", $this->symbol, json_encode($row));                                    
                $day = date('Y-m-d', $cursor);
                if (!isset($row[CANDLE_OPEN]) || !isset($row[CANDLE_VOLUME])) 
                    throw new ErrorException("~C91#FATAL:~C00 wrong candle ".var_export($row, true ));
                $day_vol = $row[CANDLE_VOLUME]; 
                $day_close = $row[CANDLE_CLOSE];
    
                $volume = 0;
                $count = 0;
                
                $eod = "$day 23:59:59";
                $exists = false;
                $srec = null;
                if (isset($stats[$day]))
                    $srec = $stats[$day];                  

                if (isset($count_map[$day]))  {
                    $exists = true;
                    $drec = $count_map[$day];                    
                    $drec->repairs = 0;
                    $count  = $drec->count;
                    $volume = $drec->volume;
                    $total_volume += $volume;

                    if (null === $srec) {                        
                        $stats[$day] = $srec = $drec;
                        $row = sprintf("('%s', '%s', '%s', %d, %s, %s)", 
                                        $day, $srec->day_start, $srec->day_end, $srec->count, $srec->volume, $day_vol);
                        $mysqli_df->insert_into($this->stats_table, "date, day_start, day_end, count_minutes, volume_minutes, volume_day", $row);
                    }
                } elseif (null === $srec) {
                    $srec = new stdClass();
                    $srec->volume = 0;
                    $srec->count = 0;
                    $srec->volume_day = $day_vol;
                    $srec->repairs = 0;
                }

                $close = 0;                
                $fill_map [$cursor] = $count;               
                
                if ($count > 0) 
                    $close = $mysqli_df->select_value('close', $this->table_name, "WHERE ts <= '$eod' ORDER BY `ts` DESC"); 

                $diff = $day_vol - $volume;
                $diff_pp = 100 * $diff / max($day_vol, $volume, 0.01);

                $close_diff = $day_close - $close;
                $close_diff_pp = 100 * $close_diff / max($day_close, $close, 0.01);
                
                if ($verbose > 1)
                    log_cmsg("~C94#CHECK_DBG:~C00 day %s candle: %s, repairs %d", $day, json_encode($row), $srec->repairs);

                if ($srec->repairs > 1) continue; // данные биржи не совпадают основовательно для разных ТФ, перезагружать смысла нет 

                // валидация с загрублениями, т.к. биржи отдают иногда не полные данные, усложняющие вертикальную валидацию
                if (abs($diff_pp) > $this->volume_tolerance || abs($close_diff_pp) > 0.15) {  // only fake candles or incomplete                    
                    if ($cursor < EXCHANGE_START_SEC) {
                        log_cmsg("~C91#WARN_OUTBOUND:~C00 have fake data in DB for %s %s?", $this->symbol, color_ts($cursor));
                        continue;
                    }
                    $result ++;                   
                    $need_clean = true;
                    if ($close != $day_close && $exists) 
                        $msg = format_color ("~C33#WRONG_DATA:~C00 for %s have close %f but day candle %s have different. Volume diff %s",
                                             $day, $close, json_encode($row), format_qty($diff));                                    
                    elseif ($volume > $day_vol * 1.001) {                                              
                        $msg = format_color ("~C31#EXCESS_DATA:~C00 may be wrong instrument was mixed in table. Volume %s > %s (diff = %s) for %s", 
                                             format_qty($volume), format_qty($day_vol), format_qty($diff), $day);                        
                    }
                    else {
                        $msg = format_color ("~C04~C97#INCOMPLETE_CHECK({$this->ticker}):~C00  for %s have volume %s in %d candles instead %s (diff %s %.2f%%).",
                                             $day, format_qty($volume),
                                             $count, format_qty($day_vol), format_qty($diff), $diff_pp);                                           
                        // $need_clean = false;                                             
                    }
                    
                    log_cmsg($msg);
                    file_add_contents("$tmp/recovery-{$this->ticker}.log", tss()." $msg\n");           

                    /* Этот вариант оптимизации использовать нельзя, если возможны несоответствия данных и ремонт, т.к. статистическая привязка - 1 день! 
                    $next_day = $cursor + SECONDS_PER_DAY;                                               
                    if ($block && $block->lbound == $next_day) {
                        // сканирование производится в прошлое, поэтому блоки расширять можно лишь влево.
                        // проверка на расширение блока, до максимального числа дней. 
                        $days = round(($block->rbound - $block->lbound) / SECONDS_PER_DAY);                        
                        if ($days < $this->days_per_block) {                             
                            $block->lbound = $cursor;
                            $block->target_volume += $day_vol; 
                            $block->avail_volume += $volume;
                            $block->key = $day;
                            $block->db_need_clean |= $need_clean;
                            log_cmsg("~C94#DBG:~C00 block %s extended left to %d days and now start from %s, target volume rised to %s", 
                                        $this->ticker, $days + 1, $day, format_qty($block->target_volume));
                            continue;                    
                        }
                    } // */

                    if ($this->BlocksCount() >= 200) break;
                    // по умолчанию создается блок в один день
                    $cursor = floor_to_day($cursor);                     
                    $eod = $cursor + SECONDS_PER_DAY - 1; // end of                                     
                    $block = $this->CreateBlock( $cursor, $eod);                    
                    $block->target_volume = $day_vol; 
                    $block->avail_volume = $volume;                    
                    $block->recovery = true;                    
                    $block->stat_rec = $srec;
                    
                    $voids []= sprintf('%d %s => %s, TV = %f', $alloc, color_ts( $block->lbound), color_ts( $block->rbound), $day_vol);
                    $alloc ++;
                    $info = strval($block);                
                    if (isset($this->block_load_history[$info])) {
                        log_cmsg("~C91#WARN_SKIP:~C00 block %s %s was loaded in this session, preventing again.", $this->ticker, $info);                        
                        unset($this->blocks_map[$block->key]);
                        $this->blocks = array_values($this->blocks_map);
                        continue;
                    }                    
                    $block->db_need_clean = $need_clean;
                }                 
                elseif (isset($count_map[$day])) {
                    if ($verbose > 2)
                        log_cmsg("~C92 #FILLED_GOOD:~C00 block at %s not need for reload, volume = %s, close = %f", $day, format_qty($day_vol), $close);                    
                    if ($elps < 20)
                        $total_volume_ch += $this->SyncClickHouse($day, $count_map[$day]);
                    unset($this->blocks_map[$day]);
                }                
                elseif ($verbose > 2) 
                    log_cmsg("~C31#SKIP:~C00 ????");
            }

            ksort($fill_map);
            $fill_str = '~C43~C30[';
            foreach ($fill_map as $t => $count)  {
                if (1 == date('d', $t))  // format months as rows
                    $fill_str .= "~C49~C33+ \n\t~C43~C30";

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
            $fill_str .= ']~C40;';

            log_cmsg("~C97#BLOCKS_CHECK_SUMMARY:~C00 total incomplete %d, total volume %s (ClickHouse %s), F:\n\t%s",
                        count($this->blocks), format_qty($total_volume), format_qty($total_volume_ch), $fill_str);
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


        protected function SyncClickHouse(string $day, object $target): float {
            global $chdb;            
            if (!is_object($chdb) || isset($this->sync_map[$day])) return 0;            
            $this->sync_map[$day] = 1;
            $table_name = $this->table_name;
            $table_qfn = DB_NAME.".$table_name";            
            $avail_volume = 0;
            if (isset($this->ch_count_map[$day])) {
                $rec = $this->ch_count_map[$day];
                $avail_volume = $rec->volume;
                $vdiff = $avail_volume - $target->volume;
                $vdiff_pp = 100 * $vdiff / max($avail_volume, $target->volume, 0.1);
                if ($rec->count == $target->count && abs($vdiff_pp) < 0.01) 
                    return $rec->volume;                
                else
                    log_cmsg("~C31#SYNC_NEED:~C00 for %s ClickHouse:$table_qfn, count %d vs MySQL %d  volume diff = %5.2f%% relative %s",
                                    $day, $rec->count, $target->count, $vdiff_pp, format_qty($target->volume), );
            }
RESYNC:      
            $query = '-';
            $mgr = $this->get_manager();
            $tmp_dir = $mgr->tmp_dir;
            try {
                if ($avail_volume > $target->volume)  {
                    log_cmsg("~C31#PERF_WARN:~C00 cleanup excess data, removing all for day %s from ClickHouse table", $day);                    
                    $chdb->write("DELETE FROM $table_name WHERE DATE(ts) = '$day'"); // remove old data
                }

                $mysqli_df = sqli_df();
                $cols = 'ts, open, close, high, low, volume, flags';
                $source = $mysqli_df->select_rows($cols, $table_name, "WHERE DATE(ts) = '$day'", MYSQLI_ASSOC);
                if (!is_array($source) || 0 == count($source)) {
                    log_cmsg("~C31#WARN_EMPTY_RES:~C00 no data for %s in MySQL.$table_name: %s", $day, $mysqli_df->error);
                    return 0;
                }

                $cols = str_replace(' ', '', $cols);
                $cl = explode(',', $cols);
                $rows = [];
                $volume = 0;
                foreach ($source as $row) {
                    $sr = $mysqli_df->pack_values($cl, $row, "'");
                    $volume += $row['volume'];
                    $rows []= "($sr)";
                }
                $query = "INSERT INTO $table_name ($cols) VALUES\n ";
                $query .= implode(",\n", $rows)."\n";
                $stmt = $chdb->write($query);                              
                if (is_object($stmt) && !$stmt->isError()) {
                    $stat = LoadQueryStats($chdb, $table_qfn, 1);
                    log_cmsg("~C93#SYNC_COMPLETE:~C00 for %s ClickHouse:.$table_qfn, stats: %s ", $day, json_encode($stat));
                    return $volume;
                }
                else
                    log_cmsg("~C31#SYNC_FAIL:~C00 for %s ClickHouse.$table_name", $day);
            } 
            catch (Exception $E) {
                $fname = "$tmp_dir/failed_sync-{$this->ticker}.sql";
                file_put_contents($fname, $query);
                log_cmsg("~C91#EXCEPTION(SyncClickHouse):~C00 first row %s (full query in %s), message %s from: %s", $rows[0], $fname,
                            $E->getMessage(), $E->getTraceAsString());
            }  
            return 0;         
        } // SyncClickHouse

    } // class CandleDataFeed


?>
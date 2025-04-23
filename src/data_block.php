<?php

    class DataBlock implements Countable, ArrayAccess {

        public  int $index = -1;
        public  $key = '';  // in map
        public  int $lbound = 0;
        public  int $rbound = 0;

        public  int $min_avail = 0; // min available timestamp in block
        public  int $max_avail = 0; // max available timestamp in block 
        public  BLOCK_CODE $code;   // 0 - not loaded, 1 - void, 2 - partial, 3 - full

        public BLOCK_CODE $reported;

        public  $info = '';

        public  int $attempts_bwd = 0;
        public  int $attempts_fwd = 0;

        public  int $last_bwd = 0;
        public  int $last_fwd = 0;

        public  int $duplicates = 0;
        public  int $fills = 0;

        protected int $min_fills = 5000; // минимальное количество заполнений часа, чтобы быть уверенным в его заполнении 

        protected $process_stats = false; 

        public  int $next_retry = 0; // timestamp allowing repeat download

        public  $recovery = false; // загрузка нужна полная, для восстановления

        public  $filled = []; // заполнение данными, на каждый час (статистика для отладки)

        public  $last_api_request = ''; // last API request
        public  $last_load = 0; // last load was (timestamp in seconds)

        public  $target_volume = 0; // целевой объем, для контроля 

                
        protected $created_from = '';

        /** array @cache_map - records with string|int key and array values */
        protected array $cache_map = []; /* records with string|int key and array values */

        public   $saldo_source = [];

        protected $owner = null;

        protected $upd_loop = 0; // number of update loop inside LoadBlock

        public function __construct(BlockDataDownloader|null $loader, int $start = 0, int $end = 0) {
            $this->owner = $loader;
            $this->process_stats = $start > EXCHANGE_START_SEC;
            // по умолчанию диапазон блока накроет всю историю данных (глобальный кэш)
            if (0 == $start) $start = EXCHANGE_START_SEC;
            if (0 == $end)   $end = END_OF_TODAY;
            
            $this->max_avail = $this->lbound = verify_timestamp($start, ' from '.get_class($this).' constructor for $start');            
            $this->min_avail = $this->rbound = verify_timestamp($end, ' from '.get_class($this).' constructor for $end');
            $this->key = date('Y-m-d', $start); 
            $this->code = BLOCK_CODE::NOT_LOADED;
            $this->reported = $this->code;
            for ($hour = 0; $hour < 24; $hour++)
                 $this->filled[$hour] = 0;

            if ($end > time() * 1.5)
                throw new ErrorException("~C91#ERROR(DataBlock):~C00 block end time is too far in future!"); // 1.5 - safety margin
            if ($start < EXCHANGE_START_SEC)
                throw new ErrorException("~C91#ERROR(DataBlock):~C00 block start $start before EXCHANGE_START_SEC: ".strval($this));

            $this->created_from = format_backtrace();    
        }

        public function __get(string $name) {
            if (property_exists($this, $name)) 
                return $this->$name;            
            $getter = 'get_'.$name;
            if (method_exists($this, $getter)) 
                return $this->$getter(); // get

            $s_prop = str_replace('_ms', '', $name);       

            if (property_exists($this, $s_prop))  {
                $ms = verify_timestamp($this->$s_prop, " from __get('$name')") * 1000;
                return $ms + str_in($s_prop, 'max_') ? 999 : 0; // get in ms                
            }
            
            return null;
        }

        public function __set ($key, $value) {            
            $setter = 'set_'.$key;
            if (method_exists($this, $setter)) {
                $this->$setter($value); // set                           
                return $value;
            } 

            $s_prop = str_replace('_ms', '', $key);   
            if (property_exists($this, $s_prop)) {
                $this->$s_prop = floor($value / 1000);
                return $value;
            }
            return $value;
        }

        public function __toString() {
            $avail = 'EMPTY';
            if ($this->fills > 0 && $this->min_avail <= $this->max_avail)
                 $avail = sprintf('AVAIL[%s..%s] F:%d', color_ts($this->min_avail), color_ts($this->max_avail), $this->fills);
            return sprintf("IDX:%d, BOUNDS[%s..%s], %s, C:%s", 
                            $this->index, color_ts($this->lbound), color_ts($this->rbound),
                            $avail, $this->code->name);                
        }

        public function CalcSummary(int $column): float {            
            $col = [];
            foreach ($this->cache_map as $rec) {
                $val = floatval($rec[$column]);
                if (0 != $val) 
                    $col []= $val;
            }
            // от перемены мест слагаемых сумма... меняется, потому что float и многие мелкие значения могут поглощаться как погрешность крупными            
            sort($col); 
            $this->saldo_source = $col;
            return array_sum($col);
        }

        public function Count(): int {
            return count($this->cache_map);
        }

        public function Covered_by(int $oldest, int $newest): bool {
            return $oldest <= $this->lbound && $this->rbound <= $newest; // block inside cache range
        }
        public function Covered_by_ms(int $oldest, int $newest): bool {
            $oldest = ceil($oldest / 1000);
            $newest = floor($newest / 1000);
            return $oldest <= $this->lbound && $this->rbound <= $newest; // block inside cache range
        }

        public function Covers(int $t) {
            return $this->lbound <= $t && $t < $this->rbound; // rbound can be next day
        }

        public function Covers_ms(float $tms) {
            $tss = floor($tms / 1000);            
            return $this->lbound <= $tss && $tss < $this->rbound; // rbound can be next day
        }

        public function Export(float $filter = 0): array {
            return $this->cache_map; // clone
        }


        public function FillDummy() { // Move to base class?
            $ts = false;                            
            if (0 == $this->fills) {
                log_cmsg("~C31#WARN:~C00 block %d:%s have no fills in session, no dummies added", $this->index, $this->key);                                                    
                return;
            }                
            $fake = [];
            if ($this->min_avail - $this->lbound > 180) {  // если gap больше 3 минут                
                $this->AddDummy($this->get_lbound_ms()); // первый фейковый тик позволит избежать повторной загрузки                
                $fake = [0];
            }            

            $hour = 0;
            $cursor = round($this->lbound / 3600) * 3600; // округляем до часа
            for ($hour = 1; $hour < $this->days_per_block * 24; $hour ++) {                                                            
                $cursor = $this->lbound + $hour * 3600;                   
                if ($cursor >= $this->rbound) break;                    
                if (!isset($block->filled[$hour]) || 0 == $this->filled[$hour])  {
                    $ts = format_ts($cursor);
                    $this->AddDummy($ts);  // в этом часе не было тиков, добавить фейк
                    $fake []= $hour;
                }  
            }                
            if ($ts)
                log_cmsg("~C94#FILL_DUMMY:~C00 block %d:%s, filled up to %s, %d ticks in cache, added fake hours [%s], last dummy ts %s", 
                            $this->index, $this->key, format_ts($this->max_avail), count($this->cache_map),
                            implode(',', $fake), $ts);
        }

        public function Finished() {
            return BLOCK_CODE::FULL == $this->code || BLOCK_CODE::VOID == $this->code;
        }

        public function format_filled(int $avg_full = 1000): string {
            $res = '';
            for ($hour = 0; $hour < 24; $hour ++)  {
                $raw = $this->filled[$hour] ?? 0;                
                $fill = 100 * $raw / $avg_full;      
                // ▒▓█□○◦ 
                if (0 == $raw) 
                    $res .= ' '; 
                elseif (0 == $fill)
                    $res .= '□';
                elseif ($fill < 50)
                    $res .= '░';
                elseif ($fill < 75)
                    $res .= '▒';
                elseif ($fill < 100)
                    $res .= '▓';
                else
                    $res .= '█';
            }
            return $res;
        }

        public function format_ts(string $key, bool $color = true) {
            $t = $this->$key;
            try {
                $res = '';
                $class = get_class($this);
                if (str_in($key, '_ms')) {
                    $t = verify_timestamp_ms($t, "from $class::format_ts");
                    $res = format_tms($t);
                } else {
                    $t = verify_timestamp($t, "from $class::format_ts");
                    $res = format_ts($t);
                }                
                return $color ? color_ts($res) : $res;
            }
            catch (Exception $E) {
                log_cmsg("~C91#ERROR(format_ts):~00 for block %s, requested from %s", $this->key, format_backtrace());
                $res = "<invalid $t>";
            }
            return $res;

        }


        protected function  get_lbound_ms() { return $this->lbound * 1000;  }
        protected function  get_rbound_ms() { return $this->rbound * 1000 + 999;  }        

   

        public function first() {
            return $this->cache_map[$this->firstKey()] ?? null;
        }
        public function firstKey(): mixed {
            return array_key_first($this->cache_map);                           
        }           

        public function get_keys(): array {
            return array_keys($this->cache_map);
        }

        public function IsFullFilled(): bool {
            $required = $this->min_fills;
            $last_hour = round(($this->rbound - $this->lbound) / 3600 - 1);
            if ($this->filled[0] >= $required && ($this->filled[$last_hour] ?? 0) >= $required) {
                $this->info = sprintf ("bound hours %d & %d have over %d records", 0, $last_hour, $this->min_fills); // block is full
                return true;
            }
            return false;
        }

        public function last() {
            return $this->cache_map[$this->lastKey()] ?? null;
        }
        public function lastKey(): mixed {
            return array_key_last($this->cache_map);                           
        }

        public function newest_ms() {
            verify_timestamp($this->max_avail, 'from block->newest_ms');
            return $this->max_avail * 1000;
        }

        public function offsetExists(mixed $offset): bool {        
            return isset($this->cache_map[$offset]);
        }

        public function offsetGet(mixed $offset): mixed {
            return $this->cache_map[$offset] ?? null;
        }
        public function offsetSet(mixed $offset, mixed $value): void {
            if (is_array($value) && isset($value[0])) 
                $this->SetRow($offset, $value);            
        }

        public function offsetUnset(mixed $offset): void {
            unset($this->cache_map[$offset]);
        }       

        public function OnUpdate() {
            ksort($this->cache_map, SORT_STRING); 
            $this->min_avail = max($this->lbound, $this->min_avail);
            $this->max_avail = min($this->rbound, $this->max_avail);
        }

        public function oldest_ms() {
            verify_timestamp($this->min_avail, 'from datablock->oldest_ms');
            return $this->min_avail * 1000;        
        }

        public function Reset() {
            $this->fills = 0;
            $this->cache_map = [];
            $this->max_avail = $this->lbound;
            $this->min_avail = $this->rbound;
        }

        public function SaldoVolume(): float {
            return 0;
        }

        public function SetRow(mixed $key, array $row): int { // no stats update, but must reimplemented in child class
            $class = get_class($this);
            log_cmsg("~C94 #DBG_SET_ROW($class):~C00 [%s] = %s, created from %s", $key, json_encode($row), $this->created_from);
            $this->cache_map[$key] = $row;
            if (is_int($key))
                $this->set_filled($key);
            return $this->Count();
        }

            /**
         * Summary of set_filled - обновление статистики заполнения на момент времени
         * @param int $t - timestamp in seconds
         */
        public function set_filled(int $t, int $records = 1) {            
            $t = verify_timestamp($t, 'from datablock->set_filled');
            $this->max_avail = max($this->max_avail, $t);
            $this->min_avail = min($this->min_avail, $t);
            $this->fills += $records;            
            if (!$this->process_stats) return;
            $hour = floor (($t - $this->lbound) / 3600);
            if ($hour < 1000)
                while (count($this->filled) <= $hour)
                    $this->filled[] = 0; // fill empty hours
            $this->filled[$hour] += $records; // чувствительно к повторным заполнениям!            
            if ($this->IsFullfilled() && $this->code != BLOCK_CODE::FULL) {
                $this->code = BLOCK_CODE::FULL;                                
                $this->info = 'full fill detected';
            }
        }

        public function set_filled_ms(float $tms, int $records = 1) {
            $t = $tms / 1000.0;            
            $tss = floor($t);
            $tse = ceil($t);
            $this->set_filled($tss, $records);
            if ($tse > $tss)  
                $this->set_filled( $tse, 0);
        }
        
        public function set_lbound(int $t){
            $this->lbound = verify_timestamp($t, 'from datablock->set_lbound');
        }
        public function set_rbound(int $t){
            $this->rbound = verify_timestamp($t, 'from datablock->set_rbound');
        }

        protected function  set_lbound_ms($value) { 
            $this->set_lbound(floor($value / 1000)); 
        }
        protected function  set_rbound_ms($value) {   
            $this->set_rbound( floor($value / 1000)); 
        }     

        public function ts_start() {
            return gmdate(SQL_TIMESTAMP, $this->lbound);
        }
        public function ts_end() {
            return gmdate(SQL_TIMESTAMP, $this->rbound);
        }    
        /** UnfilledAfter - возвращает время в мс, после которого данных ещё нет
         * @return int
         */
        public function UnfilledAfter(): int { 
            return $this->newest_ms();
        }
        /** nfilledBefore - возвращает время в мс, перед которым данных ещё нет
         * @return int
         */
        public function UnfilledBefore(): int {
            return $this->oldest_ms();
        }

        public function VoidLeft(string $unit = 's'): float {
            return seconds2u(max(0, $this->min_load - $this->lbound), $unit);
        }
        public function VoidRight(string $unit = 's'): float {
            return seconds2u(max(0,  $this->rbound - $this->max_load), $unit);
        }

    }

    function seconds2u(int $t, string $unit): float {
        if ('s' == $unit) return $t;
        elseif ('ms' == $unit) 
            return $t * 1000;
        elseif ('m' == $unit)
        return $t / 60;
        elseif ('h' == $unit)
            return $t / 3600;
        elseif ('d' == $unit)
            return $t / 86400;
        return NAN;
    }
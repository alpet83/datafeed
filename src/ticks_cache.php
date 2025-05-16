<?php
    require_once 'data_block.php';
    
    const TICK_TMS = 0;
    const TICK_BUY = 1;
    const TICK_PRICE = 2;    
    const TICK_AMOUNT = 3;
    const TICK_TRADE_NO = 4;

    class  TicksCache extends DataBlock  {
        /**
         * candle_map - карта минутных свечей, для контроля заполнения по объему и времени. Ключом является время в МИНУТАХ от начала блока
         * @var array
         */
        public     array $candle_map = [];  

        public     $daily_candle = null;  // для хранения дневной свечи
        public     $minutes_volume = 0; // for reconstruction need check
        public     $last_aggregated = 0; // last aggregated count from cache source into 1s candles

        public     array $refilled_vol = [];
        public     array $unfilled_vol = [];
        
        public function __construct(BlockDataDownloader|null $loader, int $start = 0, int $end = 0) {
            parent::__construct($loader, $start, $end);
            $this->min_fills = 10000000; // prevent false positive in IsFullFilled
        }


        public function __toString() {
            $res = parent::__toString();
            $res .= ', CM: '.count($this->candle_map);
            $res .= ', UF: '.count($this->unfilled_vol); 
            return $res;
        }

        public function newest_ms(): int {
            if (0 == count($this->cache_map)) return parent::newest_ms();
            $last = $this->last();
            return is_array($last) ? $last[TICK_TMS] : 0;            
        }

        public function oldest_ms(): int {
            if (0 == count($this->cache_map)) return parent::oldest_ms();
            $first = $this->first();
            return is_array($first) ? $first[TICK_TMS] : 0;            
        }        

        public  function AddRow(int $tms, bool $buy, float $price, float $amount, string $trade_no, string $key = null) {
            $rec = [$tms];
            $rec[TICK_BUY] = $buy;
            $rec[TICK_PRICE] = $price;
            $rec[TICK_AMOUNT] = $amount;
            $rec[TICK_TRADE_NO] = $trade_no; // standard row includes timestamp, due non-unique            
            if (null == $key)
                $key = $trade_no;          
            $this->SetRow($key, $rec);
        }
       
        /** @param float $t - timestamp in seconds         
         *  return minute from start of block
         */
        private function candle_key(float $t): int {
            return floor(($t - $this->lbound) / 60);
        }

        private function candle_tms(int $mk): int {
            return ($this->lbound + $mk * 60) * 1000;
        }

        protected function CheckFillMinute(int $m, float $amount, bool $is_dup) {        
            if (!isset($this->unfilled_vol[$m]))
                return;

            $cm = $this->candle_map;
            if (!isset($cm[$m])) 
                log_cmsg("~C91#WARN:~C00 CheckFillMinute no candle record for minute %d", $m);                           

            $this->refilled_vol[$m] = ($this->refilled_vol[$m] ?? 0) + $amount; // учитывается заполнение в т.ч. дублирующее
            if ($is_dup) return;            
            $this->unfilled_vol[$m] = ($this->unfilled_vol[$m] ?? 0) - $amount;                         
            if ($this->unfilled_vol[$m] <= 0) 
                unset($this->unfilled_vol[$m]);
        }
        public function Count(): int {
            return count($this->cache_map);
        }

        public function FormatProgress(): string {
            $res = parent::FormatProgress();
            if (count($this->unfilled_vol) < 10) {
                $vals = [];
                // may display unfilled seconds count in unfilled minutes?
                foreach ($this->unfilled_vol as $m => $v)
                    $vals []= sprintf("%d:%.3f", $m, $v);
                return '~C95'.$res.'~C00 unfilled: ~C93'.implode(', ', $vals);
            }            
            $ufcount = array_pad([], 24, 0); // count unfilled per hour
            foreach ($this->unfilled_vol as $m => $v) {
                $h = floor($m / 60);                
                $ufcount[$h] ++;
            }
            $res .= '~C00 unfilled map:~C43~C34 ';

            foreach ($ufcount as $count) {
                // ▒▓█□○◦ 
                if (0 == $count) 
                    $res .= ' '; 
                elseif ($count == 1)
                    $res .= '□';
                elseif ($count < 20)
                    $res .= '░';
                elseif ($count < 30)
                    $res .= '▒';
                elseif ($count < 60)
                    $res .= '▓';
                else
                    $res .= '█';                
            }
            return $res;
        }

        public function IsFullFilled(): bool {            
            if (0 == count($this->unfilled_vol)) return true;
            $sum = 0;
            foreach ($this->unfilled_vol as $v) 
                if ($v > 0) $sum += $v;

            return $sum < $this->target_volume * 0.0001;
        }

        public function LeftToDownload(): float {
            if (count ($this->unfilled_vol) > 0)
                return array_sum($this->unfilled_vol);
            return parent::LeftToDownload();
        }

        /** загрузка минутных свечей для контроля консистентности */
        public function LoadCandles() {
            if ($this->index <= -1) return;

            $mysqli = sqli();
            $mysqli_df = sqli_df();
            $loader = $this->owner;
            $table = $loader->table_name;
            $c_table = str_replace('ticks', 'candles', $table);
            if (!$mysqli_df->table_exists($c_table)) return;                                                

            $this->history_bwd = [];
            $this->history_fwd = [];
            
            $inaccuracy = 0.999; // 0.1% погрешность по объему, чтобы не зацикливаться на заполнении минуток 

            $days = round( ($this->rbound - $this->lbound) / SECONDS_PER_DAY);
            if ($days > 1) 
                throw new ErrorException("LoadCandles: too long range for ticks block, days = $days");

            $saldo_cv = 0;
            $bounds = "Date(ts) = '{$this->key}'";
            $m_candles = $mysqli->select_map('ts,open,close,high,low,volume', $c_table, "WHERE $bounds", MYSQLI_NUM); // load from MySQL
            $this->daily_candle = $daily =  $mysqli->select_rows('ts,open,close,high,low,volume', "{$c_table}__1D", "WHERE $bounds", MYSQLI_OBJECT); // load from MySQL

            if ($this->minutes_volume < $this->target_volume) {
                $this->minutes_volume = 0;
                foreach ($m_candles as $ts => $candle)
                    $this->minutes_volume += $candle[CANDLE_VOLUME]; // calc saldo
            }
            
            if (is_object($daily) && $daily->volume > $this->target_volume)                 
                $this->target_volume = $daily->volume;                
            
            //  один из типовых сценариев проблемных данных, когда минуток по объему не хватает до дневок. В этом случае надо дополнить фейковые минутки, хоть это и замедлит потенциально загрузку            
            $vdiff = $this->target_volume - $this->minutes_volume;
            $vdiff_pp = 100 * $vdiff / $this->target_volume;
            $full_scan = $vdiff_pp > 0.01; 
            $full_unfilled = [];

            if (!$m_candles) {
                log_cmsg("~C91 #WARN(LoadCandles):~C00 no 1m data for %s [%s] in MySQL", $c_table, $this->key);
                return;
            }            

            foreach ($m_candles as $ts => $candle) {
                $t = strtotime($ts);
                if ($t < EXCHANGE_START_SEC) {
                    log_cmsg("~C91 #ERROR:~C00 loaded candle with outbound timestamp %s", format_ts($t));
                    continue;
                }
                $key = $this->candle_key($t);
                $this->candle_map[$key] = $candle;
                $cv = $candle[CANDLE_VOLUME];
                $saldo_cv += $cv;
                $full_unfilled[$key] = $cv * $inaccuracy; 
                $this->refilled_vol[$key] = 0; // учитывается заполнение в т.ч. дублирующее
            }
            if ($full_scan) {    
                $fake = 0;            
                for ($m = 0; $m < 1440; $m ++) {
                    if (isset($full_unfilled[$m])) continue;
                    $full_unfilled[$m] = 0.00001;
                    $this->candle_map[$m] = [0, 0, 0, 0, 0.0]; // fake candle
                    $this->refilled_vol[$m] = 0;
                    $fake ++;
                }
                if ($fake > 0)
                    log_cmsg("~C33 #FULL_DAY_SCAN:~C00 due insuffcient minutes volume = %.2f%% in %d candles, added %d fake candles for %s", $vdiff_pp, count($m_candles), $fake, $this->key);
            }

            $this->unfilled_vol = $full_unfilled;

            if ($this->db_need_clean) return; // no optimize, need full refill
            $vol_map = $mysqli_df->select_map("(toHour(ts) * 60 + toMinute(ts)) as minute, SUM(amount) as volume", $table, 
                                               "FINAL WHERE $bounds GROUP BY minute ORDER BY minute");
            $abnormal = 0;
            $overhead = 0;
            $saldo = 0;
            $suspicous = [];
            if (0 == count($vol_map)) return; // no ticks preloaded            
            
            foreach ($vol_map as $m => $v) {
                $saldo += $v;
                if (!isset($this->unfilled_vol[$m])) continue;      
                $uv = $this->unfilled_vol[$m] / $inaccuracy;                           
                $cm = $this->candle_map[$m] ?? [0, 0, 0, 0, $uv];                
                $cv = $cm[CANDLE_VOLUME];                                
                if ($v > $cv * 1.005)  {                    
                    $abnormal ++;
                    $overhead += $v - $cv;
                    $this->unfilled_vol[$m] = $cv;   // try full reload
                    $ts = format_ts($m * 60 + $this->lbound);
                    $suspicous [$ts]= [$v, $cv, $uv]; 
                }
                else 
                    $this->unfilled_vol[$m] -= $v;    // для этой минуты тики загружены хотя-бы частично                
                if (!$this->TestUnfilled($m)) 
                    unset($this->unfilled_vol[$m]); // сокращение работы
            }
            
            if (0 == $saldo_cv) return;
            $this->db_need_clean |= $saldo > $saldo_cv; // force reload            
            $can_replace = $loader->data_flags & DL_FLAG_REPLACE;
            $this->db_need_clean  &= $can_replace;
            $pp = 100 * $overhead / $saldo_cv;
            if ($abnormal > 0 && $this->db_need_clean) { // TODO: use volume_tolerance
                $mgr = $this->owner->get_manager();
                $ticker = $this->owner->ticker;
                $fname = "{$mgr->tmp_dir}/overhead_{$ticker}_{$this->key}.txt";
                $info = json_encode($suspicous, JSON_PRETTY_PRINT)."\n";                               
                file_put_contents($fname, $info);
                $this->db_need_clean = true;
                $this->unfilled_vol = $full_unfilled; 
                log_cmsg("~C91#WARN:~C00 %3d abnormal minutes for %s, where volume of ticks > volume of candle, and total overhead %.2f%%. Saldo candle vol %s vs saldo ticks vol %s. Block reload planned",    
                                $abnormal, $bounds, $pp, format_qty($saldo_cv), format_qty($saldo)); // TODO: patch candles table with reset volume?  */                

            }
        }


        public function FindRow(int $t, bool $nearest = true): int {                        
            $start = 0;
            $end = count($this->cache_map) - 1;
            $mid = 0;
            $keys = array_keys($this->cache_map);
            while ($start <= $end) {
                $mid = floor(($start + $end) / 2);
                $key = $keys[$mid];
                $row = $this->cache_map[$key];

                if ($row[TICK_TMS] == $t) return $key;
                if ($row[TICK_TMS] < $t) 
                    $start = $mid + 1;
                else 
                    $end = $mid - 1;                    
            }
            $start = min($start, $end); // prevent outbound
            if ($nearest && isset($keys[$start])) 
                return $keys[$start];            
            return -1; // not found
        }

        public function FormatRow (string $key){
            global $mysqli_df;
            $row = $this->cache_map[$key];
            if (!is_array($row)) return "#ERROR: wrong offset #$key";
            $tms = format_tms($row[TICK_TMS]);
            $tms = str_replace(' ', 'T', $tms);
            $row[TICK_TMS] = $tms;
            $cols = array_keys($row);
            $row = $mysqli_df->pack_values($cols, $row, "'");
            return "($row)"; // for INSER INTO
        }
        

        public function SaldoVolume(): float {
            return $this->CalcSummary(TICK_AMOUNT);
        }

        public function SetRow(mixed $key, array $row): int {            
            $is_dup = isset($this->cache_map[$key]);  
            if ($row[TICK_PRICE] <= 0) 
                throw new ErrorException("Invalid tick record, price field must > 0 ".json_encode($row));

            $this->cache_map[$key] = $row;                    
            $t = floor($row[TICK_TMS] / 1000);                            
            if ($is_dup)
                $this->duplicates ++;
            else 
                $this->set_filled($t);            

            if ($this->index >= -1) {  // в общем кэше нет смысла фиксировать минуты
                $m = $this->candle_key($t);
                $this->CheckFillMinute($m, $row[TICK_AMOUNT], $is_dup);                
            }               
                      
            return $this->Count();
        }
    
        public function Store(TicksCache $target): int {
            $min = $target->lbound_ms;
            $max = $target->rbound_ms;
            $stored = 0;
            foreach ($this->cache_map as $key => $rec) {
                $mts      = $rec[TICK_TMS];                
                if ($mts >= $min && $mts <= $max) {                    
                    $target[$key] = $rec;
                    $stored ++;
                }
            }
            $target->OnUpdate();
            return $stored;
        }

        protected function TestUnfilled($m) {
            $cm = $this->candle_map[$m] ?? [0, 0, 0, 0, 0];
            $mv = $cm[CANDLE_VOLUME];
            if ($mv > 0) {
                $rv = $this->refilled_vol[$m] ?? 0;            
                return ($this->unfilled_vol[$m]  > 0) && ($rv < $mv * 1.5);  // чрезмерное покрытие дупликатами, тоже означает заполнение
            }
            else
                return $this->unfilled_vol[$m]  > 0; // это странный вариант, но оставить пока
        } 

        public function UnfilledAfter(): int {
            $newest = $this->newest_ms();
            $newest = floor($newest / MIN_STEP_MS) * MIN_STEP_MS;
            if ($this->LoadedForward($newest, 100))
                $newest = 0; // not available as repeat

            foreach ($this->unfilled_vol as $key => $v)
                if ($this->TestUnfilled($key)) {
                    $tms = $this->candle_tms($key);
                    if ($this->LoadedForward ($tms, 100)) {
                        $elps = $newest - $tms;
                        if ($elps > 0 && $elps < 60000 )
                            return $newest; // intra scan
                        continue; // предотвращение повторных результатов
                    }                    
                    $back = $tms - 60000;
                    $eom  = $tms + 60000;
                    if ($tms > $newest && $newest > $back && $newest < $eom) 
                        $tms = $newest;

                    // if ($tms <= $this->prev_after) $tms = $this->prev_after + 100;  // последний костыль надежды

                    if ($tms >= $this->lbound_ms) return $this->prev_after = $tms;                    
                    log_cmsg("~C91#ERROR(UnfilledAfter):~C00 for minute key %d produced timestamp %s ", $key, format_tms($tms));
                    break;
                }

            return $this->get_lbound_ms();  // full reload suggest
        }

        public function UnfilledBefore(): int {         
            $rv = array_reverse($this->unfilled_vol, true); // need maximal key
            $oldest = $this->oldest_ms();
            foreach ($rv as $key => $v) {                
                if ($this->TestUnfilled($key)) {
                    $tms = $this->candle_tms($key + 1); // download need from next minute
                    if ($this->LoadedBackward($tms, 100)) {
                        $elps = $oldest - $tms; 
                        if ($elps > 0 && $elps < 60000)  // минута ещё не просканирована?
                            return $oldest; // intra scan
                        continue; // предотвращение повторных результатов
                    }
                    return $this->prev_before = $tms;
                }
            }
            return $this->get_rbound_ms(); // full reload suggest
        }
        public function VoidLeft(string $unit = 's'): float {
            if (0 == count($this->candle_map)) return parent::VoidLeft();
            $first = array_key_first($this->candle_map);
            $void = max(0, $this->min_avail - $first * 60); 
            return seconds2u($void, $unit);
        }
        public function VoidRight(string $unit = 's'): float {
            if (0 == count($this->candle_map)) return parent::VoidLeft();
            $last = array_key_last($this->candle_map);
            $void = max(0,  $last * 60 - $this->max_avail); 
            return seconds2u($void, $unit);
        }        

    }
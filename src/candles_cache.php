<?php
    require_once 'data_block.php';

    const CANDLE_FLAG_RTMS      = 0x0400;  // realtime sync
    const CANDLE_FLAG_DUMMY     = 0x1000;   

    class  CandlesCache extends DataBlock implements Countable  {

        public $interval = 60; // in seconds

        public $mark_flags = 0;

        public $stat_rec = null;


        public  function AddRow(int $t, float $open, float $close, float $high, float $low, float $volume, int $extra = 0) {            
            $rec = [$open, $close, $high, $low, $volume];
            // total row now always 6 columns: some exchanges provide trades count for daily candles 
            $rec[] = ($this->interval < SECONDS_PER_DAY) ? $extra | $this->mark_flags : $extra; 
            return $this->SetRow($t, $rec);            
        }

        public function Combine(int $t, array $rec) {
            if (isset($this->cache_map[$t])) {
                $row = $this->cache_map[$t];                
                $row[CANDLE_CLOSE] = $rec[CANDLE_CLOSE];
                $row[CANDLE_HIGH] = max($row[CANDLE_HIGH], $rec[CANDLE_HIGH]);
                $row[CANDLE_LOW]  = min($row[CANDLE_LOW], $rec[CANDLE_LOW]);
                $row[CANDLE_VOLUME] += $rec[CANDLE_VOLUME];
            }
            else
                $this->SetROw($t, $rec);
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

            if (!isset($row[CANDLE_FLAGS]))
                $row[CANDLE_FLAGS] = 0; // trades count for daily candles

            if ($this->interval < SECONDS_PER_DAY)
                $row [CANDLE_FLAGS] |= $this->mark_flags; // flags
            

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
            return ($this->lastKey() + $this->interval - 1) * 1000;     
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

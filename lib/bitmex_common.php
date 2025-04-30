<?php
    const DB_NAME = 'bitmex'; // same as exchange

    const HISTORY_MIN_TS = '2023-10-01 00:00:00'; // первый месяц, когда уже номера сделок имеют сортируемую последовательность 

    define('EXCHANGE_START', strtotime_ms('2014-01-01 00:00')); // указывать лучше день начала работы биржи, до минут
    define('EXCHANGE_START_SEC', floor(EXCHANGE_START / 1000));
    

    class TradesMapper {

        protected $mysqli_df = null;
        protected $day = '';

        public $added = [];
        protected $exists = [];
        

        public function __construct(mysqli_ex $mysqli_df, string $day) {
            $this->mysqli_df = $mysqli_df;
            $this->day = $day;
            $query = 'CREATE TABLE IF NOT EXISTS match_id_map ';
            $query .= '(idx UInt64 NOT NULL, id String NOT NULL, dt Date, ';
            $query .= 'INDEX idx idx TYPE minmax GRANULARITY 16, ';
            $query .= 'INDEX dt dt TYPE set(0) GRANULARITY 32) ';
            $query .= 'ENGINE = ReplacingMergeTree ORDER BY id PARTITION BY toStartOfMonth(dt);';
            $mysqli_df->try_query($query); 
            $this->exists = $mysqli_df->select_map('id,idx', 'match_id_map', "FINAL WHERE dt = '$day'"); // load map part, if available                  
        }

        public function flush() {
            $rmap = [];
            foreach ($this->added as $match_id => $idx)
                $rmap[]= "('$idx', '$match_id', '{$this->day}')"; // instead auto-increment index

            if (count($rmap) > 0) {
                $query = "INSERT INTO `match_id_map` (idx, id, dt) VALUES ";
                $query .= implode(",\n", $rmap);
                if($this->mysqli_df->try_query($query))
                    log_cmsg("~C93 #OK:~C00 inserted %d match_id rows", count($rmap));            
            }            
        }


        public  function map(string $match_id, int $tms, bool $scan_new = false): string {
            // в истории тиков до 2023-09-28 были рандомные match_id, что очень хаотизирует индексацию/хранение в ClickHouse и снижает производительность в разы :-O
            if (str_contains($match_id, '00000000-0'))
                return $match_id; // уже сериализованные пошли   

            $idx = 0;      

            if (isset($this->exists[$match_id]))
                $idx = $this->exists[$match_id]; // already in DB 
            else {
                $idx = ($scan_new && isset($this->added[$match_id])) ? $this->added[$match_id] : count($this->added);
                $this->added [$match_id] = $idx;                                     
            }            
                
            // instead BitMEX 16 byte UUID using daily unique index 
            return sprintf('00000000-%010x-%08x', $tms, $idx);
        }

    }

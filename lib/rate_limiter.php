<?php

    class RateLimiter {
   
        protected $c_time = 0; 
        protected $rqs_list = array();
         
        public    $max_rate = 60; // per minute
        
        public    function     __construct($max_r = 60) {
            $this->max_rate = $max_r;
            $this->c_time = pr_time();
        }
         
         # how allowed requests without delay
        public    function     Avail(): int {   
            $mback = pr_time() - 60;
            while (count($this->rqs_list) > 0 && $this->rqs_list[0] < $mback)
            array_shift($this->rqs_list); // remove old request timestamps

            return $this->max_rate - count($this->rqs_list);
        }
        public    function     Dump($relative = true) {
            $ref = 0;
            if ($relative)
               $ref = time();
            $res = [];
            foreach ($this->rqs_list as $rqt) 
               $res []= sprintf('%.1f', $rqt - $ref);
               
            return count($res).':('.implode(', ', $res).')';       
        }
         
        public    function     Wait($pause = 100000, $sleep_func = 'usleep') {
            global $rest_allowed_ts;            
            $t = pr_time();
            $uptime = $t - $this->c_time;
            $this->rqs_list []= $t;
            sort($this->rqs_list);
            if ($uptime < 180)
                call_user_func($sleep_func, $pause);  // warmup delay for correction 

            while (1) {             
               call_user_func($sleep_func, $pause); 
               if ($this->Avail () > 3) break; // request count average
            }    

            if ($this->Avail() <= 5)
               return $this->rqs_list[0] + 60;  // best schedule time; 
            return pr_time();    
        }    

        public   function    SpreadFill() {
            $intv = 60 / $this->max_rate;
            $rqt = time();
            for ($i = 0; $i < $this->max_rate; $i ++)  {    
               $rqt -= $intv;
               $this->rqs_list[]= $rqt;       
            }
        }
   }

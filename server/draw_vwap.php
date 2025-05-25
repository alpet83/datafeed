<?php
    set_include_path(get_include_path() . PATH_SEPARATOR . '..');
    ob_start();

    require_once 'lib/common.php';
    require_once 'lib/esctext.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/clickhouse.php';
    require_once 'lib/db_config.php';

    require_once 'jpgraph/jpgraph.php';
    require_once 'jpgraph/jpgraph_stock.php';
    require_once 'jpgraph/jpgraph_line.php';
    require_once 'jpgraph/jpgraph_date.php';  
    ob_clean();

    const TICK_TMS = 0;
    const TICK_BUY = 1;
    const TICK_PRICE = 2;    
    const TICK_AMOUNT = 3;
    const TICK_TRADE_NO = 4;

    error_reporting(E_ERROR | E_WARNING | E_PARSE);    
    mysqli_report(MYSQLI_REPORT_ERROR);          
    
    $color_scheme = 'cli';
    $inj_flt = 'PHP:SQL';

    $exch = rqs_param('exchange', 'bitfinex', $inj_flt, '/(\w+)/');
    $ticker = rqs_param('ticker', 'btcusd', $inj_flt, '/(\w+)/');
    $period = rqs_param('period', 1) * 1; // in minutes or volume
    $is_vol = rqs_param('volume', 0) * 1;
    $back   = rqs_param('back', 60 * 24) * 1; // in minutes!
    $width  = rqs_param('w', 1900) * 1;
    $height = rqs_param('h', 1000) * 1;

    $exch = strtolower($exch);

    $hour_back = gmdate('Y-m-d H:i:00', time() - $back * 60);
    $from_ts = rqs_param('from', $hour_back, $inj_flt, REGEX_TIMESTAMP_FILTER);
    $limit  = rqs_param('limit', 10000000) * 1000;    
    $db_time = 0;

    class TicksSource implements Countable, ArrayAccess {

        public $ticks = []; // [time] = array(OLHCV)

        public function count(): int {
            return count($this->ticks);
        }

        public function offsetExists(mixed $offset): bool {
            return isset($this->ticks[$offset]);
        }

        public function offsetGet(mixed $offset): mixed  {
            return $this->ticks[$offset];
        }

        public function offsetSet(mixed  $offset, mixed  $value): void  {
            $this->ticks[$offset] = $value;
        }

        public function offsetUnset(mixed $offset): void {
            unset($this->ticks[$offset]);
        }
        public function keys() {
            return array_keys($this->ticks);
        }


        public function ImportDB(string $table, string $start, int $limit = 10000) {
            global $mysqli_df, $db_time;
            $strict = "FINAL WHERE (ts >= '$start') LIMIT $limit";
            $t = pr_time();
            $res = $mysqli_df->select_map('trade_no, ts, buy, price, amount', $table, $strict);
            if (!is_array($res)) {
                return false;
            }            
            $db_time += pr_time() - $t;
            $this->ImportRaw($res);
            return count($res);
        }

        public function ImportRaw(array $data) {
            $this->ticks = $data;
        }

        public function toString($format = 'json') {
            if ('json' == $format)
                return json_encode($this->ticks);
            return print_r($this->ticks, true);
        }
    }


    abstract class TicksAggregator {
        abstract public function  Process(object $source);
    }


    class TicksVolumeAggregator
        extends TicksAggregator {        

        private $period = 60;

        public function __construct(int $period = 60) {
            $this->period = $period;
        }

        public function Process(object $source) {   // result [timestamp] = [buys_cost, buys_volume, sell_cost, sells_volume, close]
            $result = [];
            $data = &$source->ticks;
            $period = $this->period;

            foreach ($data as $tno => $tick) {
                [$ts, $buy, $price, $amount] = $tick;
                $t = strtotime($ts); // ignore ms
                $t = floor($t / $period) * $period;
                $ts = date('Y-m-d H:i', $t);
                if (!isset($result[$ts])) {
                    $result[$ts] = [0, 0, 0, 0, 0];
                }
                $cost = $price * $amount;
                $offset = $buy ? 0 : 2;                
                $result[$ts][$offset] += $cost;
                $result[$ts][$offset + 1] += $amount;                
                $result[$ts][4] = $price;
            }         
            return $result;
        }
    } // class TicksVolumeAggregator

    $buy_vwap = [];
    $sell_vwap = [];
    $close_vals = [];
    $timeline = [];
    $log_file = fopen('logs/draw_ticks.log', 'w');

    date_default_timezone_set('UTC');    
    $from_ts = str_replace('T', ' ', $from_ts);
    $from_ts = trim($from_ts, 'Z');

    /*
    $fmt = strlen($from_ts) > 16 ? 'Y-m-d H:i:s.v' : 'Y-m-d H:i';
    $from_t = date_create_from_format($fmt, $from_ts);
    $from_t = $from_t->getTimestamp(); // 2021-12-08T20:40:00.000Z 
    //*/

    // strtotime($from_ts);
    $tstart = pr_time();

    // $vlines = curl_http_request("http://localhost:8090/volume_ticks?from=$from_t&treshold=$volume&limit=$limit&print_data=1&exchange=$exch&ticker=$ticker");
    $tload = pr_time() - $tstart;
    
    $db_name = strtolower($exch);
    $mysqli_df  = ClickHouseConnectMySQL(null, null, null, $db_name);       
    if (!$mysqli_df) 
        error_exit("~C91 #FATAL:~C00 Failed to connect to DB: $db_name");

    $table = sprintf('ticks__%s',  $ticker);

    $source = new TicksSource();
    if (!$source->ImportDB($table, $from_ts, $limit)) {
        // echo $source->toString()."\n";
        error_exit("~C91#FAILED:~C00 ImportDB!\n");
    }

    $raw_count = $source->count();
    $tload = pr_time() - $tstart;

    $ts = date('Ymd-his');
    
    $cd_text = '';

    $data = &$source->ticks;

    $aggr = new TicksVolumeAggregator(900);        
    $data = $aggr->Process($source);    
    ksort($data);
    

    $w_small = rqs_param('weight_small', 4) * 1;
    $w_big = rqs_param('weight_big', 20) * 1;

    $bv_accum = [];
    $sv_accum = [];

    $buys_ema = [];
    $sells_ema = [];

    function calc_vema_coef(array &$accum, float $vol) {
        global $w_small, $w_big;
        $accum []= $vol;        
        if (count($accum) > $w_big) array_shift($accum);
        $vbase = array_sum($accum);
        $tail = array_slice($accum, -$w_small);
        $vtail = array_sum($tail);
        if ($vbase > 0) {
            $coef = $vtail / $vbase;
            return $coef;
        }
        return 0;
    }

    function calc_ema(array &$result, float $coef, float $price) {
        if (count($result) > 0) {
            $last = end($result);
            $ema = $last * (1 - $coef) + $price * $coef;
            $result []= $ema;
            return $ema;
        }
        return $result []= $price;       
    }


    foreach ($data as $ts => $row) {        
        $idx = count($close_vals);
        [$buys_cost, $buys_vol, $sells_cost, $sells_vol] = $row;
        
        $b_coef = calc_vema_coef($bv_accum, $buys_vol);
        $s_coef = calc_vema_coef($bv_accum, $buys_vol);

        $timeline []= strtotime($ts);            

        $buy_vwap []= $bwap = $buys_vol > 0 ? $buys_cost / $buys_vol : null;        
        $sell_vwap []= $swap = $sells_vol > 0 ? $sells_cost / $sells_vol : null;     
        $close_vals []= $row[4];
        if (null != $bwap)
            calc_ema($buys_ema, $b_coef, $bwap);        
        else
            $buys_ema []= null;

        if (null != $swap)
            calc_ema($sells_ema, $s_coef, $swap);
        else
            $sells_ema []= null;
    }

    file_put_contents('data/vwap_raw.json', print_r($data, true));

    $graph = new Graph($width, $height);


    $count = count($timeline);
    $cinfo = $count;


    if ($raw_count > $count)
      $cinfo = sprintf("%d -> %d", $raw_count, $count);


    $elps = pr_time() - $tstart;

    $info = sprintf("count = %s, timings = %.3f, %.3f, %.2f s", $cinfo, $db_time, $tload, $elps);
    log_msg($info);    
    try {
        $bw = max(5, $width / $count - 4);
        $graph->SetScale("datlin");
        $graph->SetShadow();

        $graph->img->SetMargin(70, 30, 20, 40);                        
        $graph->xaxis->scale->SetTimeAlign(DAYADJ_1);
        $graph->xaxis->scale->SetDateFormat('d M H');         
        $graph->xaxis->scale->SetAutoTicks();        
        if ($count >= 50)
            $graph->xaxis->SetTextLabelInterval(floor($count / 10));

        $b_line = new LinePlot($buy_vwap, $timeline);
        $c_line = new LinePlot($close_vals, $timeline);
        $s_line = new LinePlot($sell_vwap, $timeline);

        $eb_line = new LinePlot($buys_ema, $timeline);
        $es_line = new LinePlot($sells_ema, $timeline);

        $graph->Add($b_line);
        $graph->Add($c_line);
        $graph->Add($s_line);        
        $graph->Add($eb_line);
        $graph->Add($es_line);

        $b_line->SetColor('green');        
        $b_line->SetWeight(1);

        $s_line->SetColor('red');
        $s_line->SetWeight(1);

        $c_line->SetColor('blue');
        $c_line->SetWeight(1);

        $eb_line->SetColor('#004000');
        $eb_line->SetStyle('dashed');

        $es_line->SetColor('#400000');
        $es_line->SetStyle('dashed');


        // $graph->xaxis->SetTickLabels($timeline);

        $graph->title->Set("VWAP curves chart: $ticker $info");
        $graph->xaxis->title->Set("Time");
        $graph->yaxis->title->Set("Price");

        $graph->title->SetFont(FF_FONT1,FS_BOLD);
        $graph->yaxis->SetTitleMargin(50);
        $graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
        $graph->legend->Pos(0.05, 0.1);
        $graph->legend->SetColumns(1);
        $graph->Stroke();
        log_cmsg("~C97#DONE:~C00 %s", $graph->img->imgsrc);
    }
    catch (Throwable $E) {
        log_cmsg("~C91 #EXCEPTION:~C00 %s from: %s", $E->getMessage(), $E->getTraceAsString());
        echo "<pre>#EXCEPTION: {$E->getMessage()}\n";
        print_r($buy_vwap);
    }


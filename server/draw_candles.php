<?php    
    set_include_path(get_include_path() . PATH_SEPARATOR . '..');
    ob_start();

    require_once 'lib/common.php';
    require_once 'lib/db_tools.php';
    require_once 'lib/db_config.php';
    require_once 'jpgraph/jpgraph.php';
    require_once 'jpgraph/jpgraph_stock.php';
    ob_clean();
    // indexes
    const BAR_TS = 0;
    const BAR_OPEN = 1;
    const BAR_CLOSE = 2;
    const BAR_LOW = 3;
    const BAR_HIGH = 4;
    const BAR_VOLUME = 5;
    const BAR_CNTR = 6;


    $inj_flt = 'PHP:SQL';
    $exch = rqs_param('exchange', 'bitfinex', $inj_flt, '/(\w+)/');
    $ticker = rqs_param('ticker', 'btcusd', $inj_flt, '/(\w+)/');
    $period = rqs_param('period', 1) * 1; // in minutes or volume
    $is_vol = rqs_param('volume', 0) * 1;
    $back   = rqs_param('back', 60 * 24) * 1; // in minutes!
    $width  = rqs_param('w', 1900) * 1;
    $height = rqs_param('h', 800) * 1;

    $exch = strtolower($exch);

    $from_back = gmdate('Y-m-d H:i', time() - $back * 60);
    $from_ts = rqs_param('from', $from_back, $inj_flt, REGEX_TIMESTAMP_FILTER);
    $limit  = rqs_param('limit', 10000) * 1;
    $volume = rqs_param('volume', 0) * 1;
    $db_time = 0;

    class CandleSource implements Countable, ArrayAccess {

        public $candles = []; // [time] = array(OLHCV)

        function count(): int {
            return count($this->candles);
        }

        public function offsetExists(mixed $offset): bool {
            return isset($this->candles[$offset]);
        }

        public function offsetGet(mixed $offset): mixed  {
            return $this->candles[$offset];
        }

        public function offsetSet(mixed  $offset, mixed  $value): void  {
            $this->candles[$offset] = $value;
        }

        public function offsetUnset(mixed $offset): void {
            unset($this->candles[$offset]);
        }
        public function keys() {
            return array_keys($this->candles);
        }


        public function ImportDB(string $table, string $start, int $limit = 10000) {
            global $mysqli, $db_time;
            $strict = "WHERE (ts >= '$start') ORDER BY `ts` LIMIT $limit";
            $t = pr_time();
            $res = $mysqli->select_from('UNIX_TIMESTAMP(ts),open,close,low,high,volume', $table, $strict);
            if (!is_object($res)) {
                return false;
            }
            $rows = $res->fetch_all(MYSQLI_NUM);
            $db_time += pr_time() - $t;
            $this->ImportRaw($rows);
            return count($rows);
        }

        public function ImportRaw(array $data) {
            $this->candles = $data;
        }

        public function toString($format = 'json') {
            if ('json' == $format)
                return json_encode($this->candles);
            return print_r($this->candles, true);
        }
    }


    abstract class CandleAggregator {
        abstract public function  Process(object $source);
    }

    // TODO: implement this class
    abstract class CandleTimeAggregator 
        extends CandleAggregator {
        abstract public function Process(object $source);
    }

    class CandleVolumeAggregator
        extends CandleAggregator {
        private float $bar_volume = 0;
        public bool   $carry_volume = false;

        public function __construct($volume) {
            $this->bar_volume = $volume;
        }


        public function Process(object $source) {

            $result = [];
            $data = &$source->candles;

            $result[] = $data[0];
            $bar = &$result[0];
            $bar[BAR_CNTR] = 0;


            $lim = $this->bar_volume;
            $carry_mul = $this->carry_volume ? 1 : 0;
            $carry = 0;
            $count = count($data);
            $rcnt = 1;
            $i = 0;

            
            $vindex = array(0);
            $vstore = array();
            $vaccum = 0;


            for ($i = 1; $i < $count; $i ++) {
                $src_bar = &$data[$i];
                $vaccum += $src_bar[BAR_VOLUME];
                if ($vaccum >= $lim) {
                    $vindex []=  $i;
                    $vstore [] = $vaccum;
                    $vaccum -= $lim;
                }
            }

            $vindex []= $count;   // finish
            $vstore [] = $vaccum;
            $carry = 0;
            $vaccum = 0;

            $last = count($vindex) - 1;
            for ($j = 0; $j < $last; $j++) {  // optimized algo
                $start = $vindex[$j];
                $src_bar = &$data[$start]; // init-copy
                $result[] = $src_bar;
                $bar = &$result[$rcnt];    // reference for upgrading
                $bar[BAR_CNTR] = 0; // source counter
                $bar[BAR_VOLUME] = $vstore[$j] + $carry * $carry_mul;  // from previous loop
                $rcnt ++;

                $bar = $data[$start];
                $bar[BAR_CNTR] = 0; // source counter
                $low = &$bar[BAR_LOW];
                $high = &$bar[BAR_HIGH];
                // TODO: unroll in several threads
                for ($i = $start; $i < $vindex[$j + 1]; $i ++) {
                    $src_bar = &$data[$i];
                    $low  = min($low,  $src_bar[BAR_LOW]); // update low
                    $high = max($high, $src_bar[BAR_HIGH]); // update high
                }

                $bar[BAR_CLOSE] = $src_bar[BAR_CLOSE]; // update close
                $bar[BAR_CNTR] = $vindex[$j + 1] - $start + 1;
                $carry = $bar[BAR_VOLUME] - $lim;
                $bar[BAR_VOLUME] -= $carry * $carry_mul;
            }
           

            return $result;
        }
    } // class CandleVolumeAggregator

    $cdata = array();
    $timeline = array();
    $log_file = fopen('data/candles.log', 'w');

    date_default_timezone_set('UTC');
    $from_ts = str_replace('T', ' ', $from_ts);
    $from_ts = trim($from_ts, 'Z');

    $fmt = strlen($from_ts) > 16 ? 'Y-m-d H:i:s.v' : 'Y-m-d H:i';
    $from_t = date_create_from_format($fmt, $from_ts);
    $from_t = $from_t->getTimestamp(); // 2021-12-08T20:40:00.000Z

    // strtotime($from_ts);
    $tstart = pr_time();

    $vlines = curl_http_request("http://localhost:8090/volume_candles?from=$from_t&treshold=$volume&limit=$limit&print_data=1&exchange=$exch&ticker=$ticker");
    $tload = pr_time() - $tstart;

    if (str_in($vlines, '#ERROR')) {
        log_msg("#DBG: no data from candle server, result: $vlines");
        $mysqli = init_remote_db($exch);
        if (!$mysqli)
            die("#FATAL: DB inaccessible!\n");

        $mysqli->try_query("SET time_zone = '+0:00'");

        $table = sprintf('%s.candles__%s', $exch, $ticker);


        $source = new CandleSource();
        if (!$source->ImportDB($table, $from_ts, $limit)) {
            // echo $source->toString()."\n";
            die("#FAILED: ImportDB!\n");
        }

        $raw_count = $source->count();

        $tload = pr_time() - $tstart;


        $ts = date('Ymd-his');
        $timepts = array_keys($source->candles);


        $test = array();
        $cd_text = '';

        $data = &$source->candles;
        if ($volume > 0) {
            $aggr = new CandleVolumeAggregator($volume);
            $aggr->carry_volume = rqs_param('carry_volume', 0);
            $data = $aggr->Process($source);
        }

        foreach ($data as $row) {
            $ts = date('y-m-d H:i', $row[BAR_TS]);
            $timeline []= $ts;
            $cdata []= $row[BAR_OPEN];
            $cdata []= $row[BAR_CLOSE];
            $cdata []= $row[BAR_LOW];
            $cdata []= $row[BAR_HIGH];
            $test[$ts] = array_shift($row);
        }


        file_put_contents('data/candles.json', print_r($test, true));
    } 
    else {
        $vlines = explode("\n", $vlines);
        $hdr = array_shift($vlines);
        log_msg(sprintf(" candle server returns %d lines, header:\n %s ", count($vlines), $hdr));

        while (strpos($vlines[0], '#') !== false) {
            $line = array_shift($vlines); // comments and meta
            log_msg("\t $line\n");
        }

        $raw_count = 0;

        foreach ($vlines as $line) {
            $vals = sscanf($line, '%d,%f,%f,%f,%f,%f');
            if (!$vals) continue;
            list($t, $o, $c, $l, $h, $v) = $vals;
            $ts = gmdate('y-m-d H:i', $t);
            $timeline []= $ts;
            $cdata []= $o;
            $cdata []= $c;
            $cdata []= $l;
            $cdata []= $h;
        }

    }

    $graph = new Graph($width, $height);


    $count = count($timeline);
    $cinfo = $count;


    if ($raw_count > $count)
      $cinfo = sprintf("%d -> %d", $raw_count, $count);


    $elps = pr_time() - $tstart;

    $info = sprintf("count = %s, timings = %.3f, %.3f, %.2f s", $cinfo, $db_time, $tload, $elps);
    if ($volume > 0)
      $info .= ", vol = $volume";
    log_msg($info);

    // file_put_contents(, $info);
    // file_put_contents('candles.data', $cd_text);


    $bw = max(5, $width / $count - 4);

    $plot = new StockPlot($cdata);
    $plot->HideEndLines(true);
    $plot->SetWidth($bw);

    $graph->SetScale("textlin");
    $graph->SetShadow();
    $graph->img->SetMargin(70,30,20,40);
    $graph->Add($plot);

    $graph->xaxis->SetTextTickInterval(1);
    if ($count >= 50)
      $graph->xaxis->SetTextLabelInterval(floor($count / 10));
    $graph->xaxis->SetTickLabels($timeline);

    $graph->title->Set("Candles chart: $ticker $info");
    $graph->xaxis->title->Set("Time");
    $graph->yaxis->title->Set("Price");

    $graph->title->SetFont(FF_FONT1,FS_BOLD);
    $graph->yaxis->SetTitleMargin(50);
    $graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
    $graph->legend->Pos(0.05, 0.1);
    $graph->legend->SetColumns(1);
    $graph->Stroke();


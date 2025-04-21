package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
)

// ...
const CLICKHOUSE_USER string = "username"
const CLICKHOUSE_PASS string = "passdword"

var db *sql.DB = nil

func OpenDB() {
	var err error
	var url string
	url = "clickhouse://" + CLICKHOUSE_USER + ":" + CLICKHOUSE_PASS + "@127.0.0.1:9000/trading?dial_timeout=1s&compress=true"
	db, err = sql.Open("clickhouse", url)
	if err != nil {
		panic(err)
	}
	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
}

func headers(w http.ResponseWriter, req *http.Request) {
	for name, headers := range req.Header {
		for _, h := range headers {
			fmt.Fprintf(w, "#HEADER: %v = %v\n", name, h)
		}
	}
} // headers

type CandleData struct {
	ts                             int
	open, close, low, high, volume float32
}

type CandleCache struct {
	ticker string
	data   []CandleData
	//	finish		func()
}

/*
	func		Count() int
	func		Find(ts int) int
*/

var cache_map map[string]*CandleCache // key exch:ticker

func (cache CandleCache) Count() int { return len(cache.data) }
func (cache CandleCache) Find(ts int) int {

	right := cache.Count() - 1
	left := 0

	for left = 0; left+1 < right; {
		mid := (left + right) / 2
		rec := cache.data[mid]
		if rec.ts > ts { // need scan left
			right = mid
		} else {
			left = mid
		}
	}
	// end scan
	if cache.data[left].ts >= ts {
		return left
	}
	return right
}

func LoadCache(exch string, ticker string) *CandleCache {
	key := exch + ":" + ticker
	cache := new(CandleCache)
	cache.ticker = ticker
	cache_map[key] = cache
	fmt.Printf("Loading cache %s ", key)
	start := time.Now()
	// UNIX_TIMESTAMP
	table := exch + "__candles__" + ticker
	fields := "toUnixTimestamp(ts), open, close, low, high, volume"
	tsplit := "now() - 600" // toStartOfHour(now(), 'UTC')
	query := fmt.Sprintf("SELECT %s FROM trading.%s WHERE ts < %s\n", fields, table, tsplit)
	query = query + fmt.Sprintf("UNION ALL SELECT %s FROM mysql_datafeed.%s WHERE ts >= %s\n", fields, table, tsplit)
	query = query + " ORDER by `ts`"

	results, err := db.Query(query)
	if err != nil {
		fmt.Printf("#ERROR: Query result %s \n", err.Error()) // proper error handling instead of panic in your app
		return cache
	}
	loadt := time.Since(start)

	for results.Next() {
		var cd CandleData
		results.Scan(&cd.ts, &cd.open, &cd.close, &cd.low, &cd.high, &cd.volume)
		cache.data = append(cache.data, cd)
	}

	fmt.Print(", count = ", cache.Count())
	elps := time.Since(start)
	fmt.Printf(" request time = %v ms, full time = %v ms\n", loadt.Milliseconds(), elps.Milliseconds())
	return cache
}

func RqsParam(rqs *http.Request, key string, def string) string {
	keys, ok := rqs.URL.Query()[key]
	if !ok || len(keys[0]) < 1 {
		return def
	}
	return keys[0]
}

func RqsParamInt(rqs *http.Request, key string, def int) int {
	s := RqsParam(rqs, key, fmt.Sprintf("%d", def))
	v, _ := strconv.Atoi(s)
	return v
}

func RqsParamFloat(rqs *http.Request, key string, def float32) float32 {
	s := RqsParam(rqs, key, fmt.Sprintf("%f", def))
	var result float32
	fmt.Sscanf(s, "%f", &result)
	return result
}

func tss() string {
	now := time.Now()
	return now.Format("2006-01-02 15:04:05")
}

func volume_candles(w http.ResponseWriter, req *http.Request) {
	var out []CandleData
	var accum CandleData

	ts := RqsParamInt(req, "from", 1628665260)
	exch := RqsParam(req, "exchange", "bitfinex")
	ticker := RqsParam(req, "ticker", "btcusd")
	limit := RqsParamInt(req, "limit", 100000)
	carry_mul := RqsParamFloat(req, "carry", 1.0)

	key := exch + ":" + ticker

	var cache *CandleCache
	var ok bool
	if cache, ok = cache_map[key]; ok {
		fmt.Printf("Data for %s is cached\n", key)
	} else {
		cache = LoadCache(exch, ticker)
	}

	istart := cache.Find(ts)

	accum = cache.data[istart]
	accum.volume = 0

	tresh := RqsParamFloat(req, "treshold", 10000.0)
	fmt.Printf("[%s]. #PERF: processing volume_candles request, from = %d, treshold = %.1f", tss(), ts, tresh)

	cstart := time.Now()

	buff_len := cache.Count()

	for i := istart; i < buff_len && limit >= 0; i++ {
		src := cache.data[i]

		if accum.volume >= tresh {
			carry := carry_mul * (accum.volume - tresh) // zero if disabled
			accum.volume -= carry                       // == tresh if carry enabled
			out = append(out, accum)
			accum = src
			accum.volume += carry
		} else {
			accum.volume += src.volume
			if src.high > accum.high {
				accum.high = src.high
			}
			if src.low < accum.low {
				accum.low = src.low
			}
			accum.close = src.close
		}

		limit--
	}
	if accum.volume > 0 {
		out = append(out, accum) // last candle must set
	} else {
		fmt.Fprintf(w, "#ERROR: no data processed, from %d to %d ", istart, buff_len)
	}

	elps := time.Since(cstart)
	elps_ms := float32(elps.Microseconds()) / 1000.0

	count := len(out)

	fmt.Fprintf(w, "#PERF: Source range [%d..%d], product count = %d, comb. time = %.2f ms\n", istart, buff_len, count, elps_ms)
	fmt.Printf(", product count = %d, cycle time = %.3f ms \n", count, elps_ms)

	var dsp_saldo float32 = 0.0
	var diffs float32 = 0.0

	for istart = 0; istart < count-1; istart++ {
		for i := istart + 1; i < count; i++ {
			diff := out[i].close - out[istart].close
			if diff >= 0 {
				dsp_saldo += diff
			} else {
				dsp_saldo -= diff
			}
			diffs += 1.0
		}
	}

	fmt.Fprintf(w, "#STATS: close dispersion summ = %.3f\n", dsp_saldo/diffs)

	print_out := RqsParamInt(req, "print_data", 1)

	if print_out > 0 {
		for _, rec := range out {
			fmt.Fprintf(w, "%d,%f,%f,%f,%f,%f\n", rec.ts, rec.open, rec.close, rec.low, rec.high, rec.volume)
		}
	}

}

func main() {
	// TODO: readonly user
	cache_map = make(map[string]*CandleCache)
	OpenDB()
	LoadCache("bitfinex", "btcusd")
	// Execute the query
	// WHERE ts > '2020-01-01 10:00:00'
	http.HandleFunc("/headers", headers)
	http.HandleFunc("/volume_candles", volume_candles)
	http.ListenAndServe(":8090", nil)
}

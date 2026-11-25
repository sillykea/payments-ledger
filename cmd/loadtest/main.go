// Command loadtest hammers a running ledger with concurrent transfers, then
// proves the books still balance.
//
//	loadtest -url http://localhost:8080 -token $LEDGER_API_TOKEN -duration 30s
//
// Each worker sends one request at a time (a closed loop), so the latencies
// reported are service times under that concurrency, not open-loop latency
// at a fixed arrival rate. A share of requests are deliberate retries that
// reuse an earlier idempotency key: they must be replayed, never posted twice.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type client struct {
	url, token string
	http       *http.Client
}

func (c *client) do(method, path, key string, body any) (int, map[string]any, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.url+path, r)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, nil
}

func (c *client) must(method, path, key string, body any, want int) map[string]any {
	code, out, err := c.do(method, path, key, body)
	if err != nil || code != want {
		fmt.Fprintf(os.Stderr, "setup failed: %d %v %v\n", code, out, err)
		os.Exit(1)
	}
	return out
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func main() {
	url := flag.String("url", "http://localhost:8080", "ledger base URL")
	token := flag.String("token", os.Getenv("LEDGER_API_TOKEN"), "API token")
	accounts := flag.Int("accounts", 50, "customer accounts")
	workers := flag.Int("workers", 32, "concurrent clients")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	retryRate := flag.Float64("retry-rate", 0.1, "share of requests that retry an earlier key")
	flag.Parse()

	c := &client{url: *url, token: *token, http: &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *workers * 2},
	}}
	run := time.Now().UnixNano()
	bank := c.must("POST", "/v1/accounts", "", map[string]any{"name": "loadtest bank", "currency": "EUR", "allow_negative": true}, 201)["id"].(string)
	ids := make([]string, *accounts)
	for i := range ids {
		ids[i] = c.must("POST", "/v1/accounts", "", map[string]any{"name": fmt.Sprint("user ", i), "currency": "EUR"}, 201)["id"].(string)
		c.must("POST", "/v1/transfers", fmt.Sprintf("seed-%d-%d", run, i), map[string]any{"from": bank, "to": ids[i], "amount": "1000.00", "currency": "EUR"}, 201)
	}

	var posted, replayed, insufficient, failed atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	for w := range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(run), uint64(w)))
			type sent struct {
				key  string
				body map[string]any
			}
			var history []sent
			local := []time.Duration{}
			for i := 0; time.Now().Before(deadline); i++ {
				var s sent
				retry := len(history) > 0 && r.Float64() < *retryRate
				if retry {
					s = history[r.IntN(len(history))]
				} else {
					from, to := r.IntN(len(ids)), r.IntN(len(ids)-1)
					if to >= from {
						to++
					}
					s = sent{fmt.Sprintf("lt-%d-%d-%d", run, w, i), map[string]any{
						"from": ids[from], "to": ids[to], "currency": "EUR",
						"amount": fmt.Sprintf("%d.%02d", 1+r.IntN(30), r.IntN(100)),
					}}
				}
				start := time.Now()
				code, body, err := c.do("POST", "/v1/transfers", s.key, s.body)
				local = append(local, time.Since(start))
				switch {
				case err != nil:
					failed.Add(1)
				case code == 201 && retry:
					replayed.Add(1)
				case code == 201:
					posted.Add(1)
					history = append(history, s)
				case code == 422 && !retry:
					insufficient.Add(1)
				default:
					failed.Add(1)
					fmt.Fprintln(os.Stderr, "unexpected", code, body)
				}
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	slices.Sort(lat)
	elapsed := *duration

	var total int64
	for _, id := range ids {
		_, acc, err := c.do("GET", "/v1/accounts/"+id, "", nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		bal := int64(acc["balance_minor"].(float64))
		if bal < 0 {
			fmt.Fprintf(os.Stderr, "FAIL: %s is negative (%d)\n", id, bal)
			os.Exit(1)
		}
		total += bal
	}
	want := int64(len(ids)) * 100000
	code, rep, _ := c.do("POST", "/v1/reconciliation", "", nil)

	n := posted.Load() + replayed.Load() + insufficient.Load() + failed.Load()
	fmt.Printf("requests            %d in %s (%.0f req/s)\n", n, elapsed, float64(n)/elapsed.Seconds())
	fmt.Printf("posted              %d (%.0f postings/s)\n", posted.Load(), float64(posted.Load())/elapsed.Seconds())
	fmt.Printf("replayed retries    %d\n", replayed.Load())
	fmt.Printf("insufficient funds  %d\n", insufficient.Load())
	fmt.Printf("errors              %d\n", failed.Load())
	fmt.Printf("latency p50/p95/p99 %s / %s / %s (max %s)\n",
		pct(lat, .50).Round(100*time.Microsecond), pct(lat, .95).Round(100*time.Microsecond),
		pct(lat, .99).Round(100*time.Microsecond), lat[len(lat)-1].Round(100*time.Microsecond))
	fmt.Printf("customer money      %d minor units, expected %d\n", total, want)
	fmt.Printf("reconciliation      clean=%v (HTTP %d)\n", rep["clean"], code)
	if total != want || rep["clean"] != true || failed.Load() > 0 {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS (money conserved, no negative balances, ledger reconciles)")
}

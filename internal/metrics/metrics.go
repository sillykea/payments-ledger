// Package metrics writes counters, gauges and one latency histogram in the
// Prometheus text format. It is deliberately small: the service needs a
// handful of metrics, not a client library.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

var buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

type hist struct {
	counts []uint64
	sum    float64
	total  uint64
}

// Registry is safe for concurrent use.
type Registry struct {
	mu       sync.Mutex
	help     map[string]string
	kind     map[string]string
	counters map[string]map[string]float64
	gauges   map[string]map[string]float64
	hists    map[string]map[string]*hist
}

func New() *Registry {
	r := &Registry{
		help: map[string]string{}, kind: map[string]string{},
		counters: map[string]map[string]float64{}, gauges: map[string]map[string]float64{},
		hists: map[string]map[string]*hist{},
	}
	r.describe("ledger_http_requests_total", "counter", "HTTP requests by route and status code.")
	r.describe("ledger_http_request_duration_seconds", "histogram", "HTTP request latency by route.")
	r.describe("ledger_postings_total", "counter", "Posting attempts by kind and result.")
	r.describe("ledger_outbox_published_total", "counter", "Events the outbox relay delivered.")
	r.describe("ledger_outbox_pending", "gauge", "Events written but not yet delivered.")
	r.describe("ledger_reconciliation_mismatches", "gauge", "Problems found by the last reconciliation run (0 = clean).")
	r.describe("ledger_reconciliation_last_run_timestamp_seconds", "gauge", "Unix time of the last reconciliation run.")
	return r
}

func (r *Registry) describe(name, kind, help string) { r.help[name], r.kind[name] = help, kind }

// Labels renders label pairs: Labels("route", "/x", "code", "200").
func Labels(kv ...string) string {
	parts := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(kv[i+1])
		parts = append(parts, fmt.Sprintf(`%s="%s"`, kv[i], v))
	}
	return strings.Join(parts, ",")
}

func (r *Registry) Add(name, labels string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counters[name] == nil {
		r.counters[name] = map[string]float64{}
	}
	r.counters[name][labels] += v
}

func (r *Registry) Set(name, labels string, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gauges[name] == nil {
		r.gauges[name] = map[string]float64{}
	}
	r.gauges[name][labels] = v
}

func (r *Registry) Observe(name, labels string, seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hists[name] == nil {
		r.hists[name] = map[string]*hist{}
	}
	h := r.hists[name][labels]
	if h == nil {
		h = &hist{counts: make([]uint64, len(buckets))}
		r.hists[name][labels] = h
	}
	for i, b := range buckets {
		if seconds <= b {
			h.counts[i]++
		}
	}
	h.sum += seconds
	h.total++
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func braces(labels string) string {
	if labels == "" {
		return ""
	}
	return "{" + labels + "}"
}

func join(labels, extra string) string {
	if labels == "" {
		return extra
	}
	return labels + "," + extra
}

// Write renders every metric.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range sortedKeys(r.help) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, r.help[name], name, r.kind[name])
		for _, l := range sortedKeys(r.counters[name]) {
			fmt.Fprintf(w, "%s%s %g\n", name, braces(l), r.counters[name][l])
		}
		for _, l := range sortedKeys(r.gauges[name]) {
			fmt.Fprintf(w, "%s%s %g\n", name, braces(l), r.gauges[name][l])
		}
		for _, l := range sortedKeys(r.hists[name]) {
			h := r.hists[name][l]
			for i, b := range buckets {
				fmt.Fprintf(w, "%s_bucket{%s} %d\n", name, join(l, fmt.Sprintf(`le="%g"`, b)), h.counts[i])
			}
			fmt.Fprintf(w, "%s_bucket{%s} %d\n%s_sum%s %g\n%s_count%s %d\n",
				name, join(l, `le="+Inf"`), h.total, name, braces(l), h.sum, name, braces(l), h.total)
		}
	}
}

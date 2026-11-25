// Command eventsink is a minimal outbox consumer for the local demo. It shows
// the consumer's half of at-least-once delivery: events are de-duplicated on
// the Idempotency-Key header, so a redelivered event is acknowledged but not
// processed twice.
package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var mu sync.Mutex
	seen := map[string]bool{}
	var received, duplicates int

	http.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("Idempotency-Key")
		io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
		mu.Lock()
		dup := seen[id]
		seen[id] = true
		if dup {
			duplicates++
		} else {
			received++
		}
		n, d := received, duplicates
		mu.Unlock()
		if !dup && n%500 == 0 {
			log.Info("events", "received", n, "duplicates_dropped", d)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	http.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"received":` + itoa(received) + `,"duplicates_dropped":` + itoa(duplicates) + `}`))
	})
	addr := os.Getenv("LISTEN")
	if addr == "" {
		addr = ":8081"
	}
	log.Info("eventsink listening", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Error("stopped", "error", err)
		os.Exit(1)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

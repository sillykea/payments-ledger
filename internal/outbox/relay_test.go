package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sillykea/payments-ledger/internal/ledger"
	"github.com/sillykea/payments-ledger/internal/outbox"
	"github.com/sillykea/payments-ledger/internal/testdb"
)

var ctx = context.Background()

type recorder struct {
	mu     sync.Mutex
	ids    []string
	failAt int // fail the Nth publish (1-based); 0 = never
	calls  int
}

func (r *recorder) Publish(_ context.Context, ev outbox.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls == r.failAt {
		return errors.New("receiver down")
	}
	r.ids = append(r.ids, ev.EventID)
	return nil
}

func post(t *testing.T, l *ledger.Ledger, n int) {
	t.Helper()
	bank, _ := l.CreateAccount(ctx, "bank", "EUR", true)
	user, _ := l.CreateAccount(ctx, "user", "EUR", false)
	for i := range n {
		if _, err := l.Transfer(ctx, fmt.Sprint("k", i), ledger.TransferRequest{From: bank.ID, To: user.ID, Amount: "1.00", Currency: "EUR"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEveryPostingProducesOneEvent(t *testing.T) {
	pool := testdb.New(t)
	post(t, ledger.New(pool), 5)
	rec := &recorder{}
	r := &outbox.Relay{Pool: pool, Publisher: rec, Logger: slog.Default()}
	n, err := r.RunOnce(ctx)
	if err != nil || n != 5 {
		t.Fatalf("published %d, %v", n, err)
	}
	if n, _ := r.RunOnce(ctx); n != 0 {
		t.Errorf("second run republished %d events", n)
	}
	if p, _ := outbox.Pending(ctx, pool); p != 0 {
		t.Errorf("%d still pending", p)
	}
}

func TestFailureKeepsEventsPendingInOrder(t *testing.T) {
	pool := testdb.New(t)
	post(t, ledger.New(pool), 5)
	rec := &recorder{failAt: 3}
	r := &outbox.Relay{Pool: pool, Publisher: rec, Logger: slog.Default()}
	n, err := r.RunOnce(ctx)
	if err == nil || n != 2 {
		t.Fatalf("first run: %d sent, err %v; want 2 sent and an error", n, err)
	}
	if p, _ := outbox.Pending(ctx, pool); p != 3 {
		t.Errorf("pending = %d, want 3", p)
	}
	n, err = r.RunOnce(ctx)
	if err != nil || n != 3 {
		t.Fatalf("retry: %d, %v", n, err)
	}
	if len(rec.ids) != 5 {
		t.Errorf("delivered %d distinct events, want 5", len(rec.ids))
	}
	var attempts int
	pool.QueryRow(ctx, `SELECT attempts FROM outbox ORDER BY id OFFSET 2 LIMIT 1`).Scan(&attempts)
	if attempts != 2 {
		t.Errorf("failed event attempts = %d, want 2", attempts)
	}
}

func TestConcurrentRelaysNeverDoubleSend(t *testing.T) {
	pool := testdb.New(t)
	post(t, ledger.New(pool), 60)
	rec := &recorder{}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &outbox.Relay{Pool: pool, Publisher: rec, Logger: slog.Default(), BatchSize: 7}
			for {
				n, err := r.RunOnce(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					if p, _ := outbox.Pending(ctx, pool); p == 0 {
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	seen := map[string]int{}
	for _, id := range rec.ids {
		seen[id]++
	}
	if len(rec.ids) != 60 || len(seen) != 60 {
		t.Errorf("sent %d events, %d distinct; want 60 and 60", len(rec.ids), len(seen))
	}
}

func TestWebhook(t *testing.T) {
	var gotKey, gotType string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, gotType = r.Header.Get("Idempotency-Key"), r.Header.Get("X-Event-Type")
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &body)
		if gotKey == "evt_fail" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	w := outbox.Webhook{URL: srv.URL}
	if err := w.Publish(ctx, outbox.Event{EventID: "evt_1", Topic: "transaction.posted", Payload: []byte(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if gotKey != "evt_1" || gotType != "transaction.posted" || body["a"] != float64(1) {
		t.Errorf("webhook got key=%q type=%q body=%v", gotKey, gotType, body)
	}
	if err := w.Publish(ctx, outbox.Event{EventID: "evt_fail", Payload: []byte(`{}`)}); err == nil {
		t.Error("a 503 was treated as delivered")
	}
}

func TestWebhookSignature(t *testing.T) {
	var sig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig = r.Header.Get("X-Ledger-Signature")
	}))
	defer srv.Close()
	body := []byte(`{"event_id":"evt_1"}`)

	unsigned := outbox.Webhook{URL: srv.URL}
	if err := unsigned.Publish(ctx, outbox.Event{EventID: "evt_1", Payload: body}); err != nil {
		t.Fatal(err)
	}
	if sig != "" {
		t.Errorf("signed without a secret: %q", sig)
	}

	signed := outbox.Webhook{URL: srv.URL, Secret: []byte("s3cret")}
	if err := signed.Publish(ctx, outbox.Event{EventID: "evt_1", Payload: body}); err != nil {
		t.Fatal(err)
	}
	ts := strings.TrimPrefix(strings.Split(sig, ",")[0], "t=")
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		t.Fatalf("bad signature header %q", sig)
	}
	if want := outbox.Sign([]byte("s3cret"), body, time.Unix(sec, 0)); sig != want {
		t.Errorf("signature %q, want %q", sig, want)
	}
	// A known vector, so a receiver in another language can check its code.
	if got := outbox.Sign([]byte("s3cret"), body, time.Unix(1700000000, 0)); got != "t=1700000000,v1="+knownVector {
		t.Errorf("known vector: got %s", got)
	}
}

const knownVector = "6cee9870342146e9ec63da18ecb1ee83e669ab35b33530cae78683ebcf468945"

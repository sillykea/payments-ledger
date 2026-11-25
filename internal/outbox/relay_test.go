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
	"sync"
	"testing"

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

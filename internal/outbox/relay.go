// Package outbox publishes events that postings wrote to the outbox table.
//
// Delivery is at least once: an event is marked published only after the
// publisher accepts it, so a crash in between sends it again. Consumers
// de-duplicate on event_id. Several relays can run at once: each claims a
// batch with FOR UPDATE SKIP LOCKED, so they never send the same row
// concurrently. See docs/adr/0004.
package outbox

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one outbox row.
type Event struct {
	ID      int64
	EventID string
	Topic   string
	Payload json.RawMessage
}

// Publisher delivers an event somewhere. It must return an error unless the
// event was accepted.
type Publisher interface {
	Publish(ctx context.Context, ev Event) error
}

// Relay moves events from the outbox to a Publisher.
type Relay struct {
	Pool      *pgxpool.Pool
	Publisher Publisher
	Logger    *slog.Logger
	BatchSize int
	Interval  time.Duration
	// OnPublished is called with the number of events sent per batch.
	OnPublished func(n int)
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	interval := r.Interval
	if interval == 0 {
		interval = time.Second
	}
	for {
		n, err := r.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			r.Logger.Warn("outbox relay", "error", err)
		}
		if n > 0 && err == nil {
			continue // more may be waiting: drain before sleeping
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// RunOnce claims and publishes one batch, returning how many were sent.
// Publishing stops at the first failure, so events from one batch go out in
// order; the failed event and the rest stay pending for the next run.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	size := r.BatchSize
	if size <= 0 {
		size = 100
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT id, event_id, topic, payload FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT $1
		FOR UPDATE SKIP LOCKED`, size)
	if err != nil {
		return 0, err
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Event, error) {
		var e Event
		err := row.Scan(&e.ID, &e.EventID, &e.Topic, &e.Payload)
		return e, err
	})
	if err != nil || len(events) == 0 {
		return 0, err
	}

	sent := []int64{}
	var pubErr error
	for _, ev := range events {
		if pubErr = r.Publisher.Publish(ctx, ev); pubErr != nil {
			if _, err := tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1 WHERE id = $1`, ev.ID); err != nil {
				return 0, err
			}
			break
		}
		sent = append(sent, ev.ID)
	}
	if len(sent) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = ANY($1)`, sent); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if r.OnPublished != nil && len(sent) > 0 {
		r.OnPublished(len(sent))
	}
	if pubErr != nil {
		return len(sent), fmt.Errorf("publish %s: %w", events[len(sent)].EventID, pubErr)
	}
	return len(sent), nil
}

// Pending counts events not yet published.
func Pending(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// Webhook POSTs each event as JSON. The event ID goes in the
// Idempotency-Key header so the receiver can drop duplicates.
//
// When Secret is set, each request carries
//
//	X-Ledger-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">
//
// so the receiver can check the event came from this ledger and is recent.
// The timestamp is signed too, so an old request can't be replayed later.
type Webhook struct {
	URL    string
	Secret []byte
	Client *http.Client
	now    func() time.Time
}

// Sign returns the X-Ledger-Signature value for body at time t.
func Sign(secret, body []byte, t time.Time) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts + "."))
	mac.Write(body)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func (w Webhook) Publish(ctx context.Context, ev Event) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(ev.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", ev.EventID)
	req.Header.Set("X-Event-Type", ev.Topic)
	if len(w.Secret) > 0 {
		now := time.Now
		if w.now != nil {
			now = w.now
		}
		req.Header.Set("X-Ledger-Signature", Sign(w.Secret, ev.Payload, now()))
	}
	c := w.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("webhook answered %d", resp.StatusCode)
	}
	return nil
}

// Log writes events to the structured log. Useful when nothing consumes them yet.
type Log struct{ Logger *slog.Logger }

func (l Log) Publish(_ context.Context, ev Event) error {
	l.Logger.Info("event", "event_id", ev.EventID, "topic", ev.Topic)
	return nil
}

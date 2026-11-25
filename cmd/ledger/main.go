// Command ledger runs the payments ledger.
//
//	ledger                 serve the API, run the outbox relay and periodic reconciliation
//	ledger migrate         apply database migrations and exit
//	ledger reconcile       run one reconciliation, print the report, exit 1 if not clean
//	ledger -version        print the version
//	ledger -healthcheck URL exit 0 if URL answers 200 (the image has no curl)
//
// Environment:
//
//	DATABASE_URL         postgres://user:pass@host:5432/db
//	LEDGER_API_TOKEN     bearer token for /v1 (32+ characters)
//	LEDGER_LISTEN        listen address (default :8080)
//	OUTBOX_WEBHOOK_URL   where to POST events (default: write them to the log)
//	RECONCILE_INTERVAL   how often to reconcile in the background (default 1h, 0 = off)
//	AUTO_MIGRATE         apply migrations on start (default true)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sillykea/payments-ledger/internal/api"
	"github.com/sillykea/payments-ledger/internal/ledger"
	"github.com/sillykea/payments-ledger/internal/metrics"
	"github.com/sillykea/payments-ledger/internal/outbox"
	"github.com/sillykea/payments-ledger/internal/recon"
	"github.com/sillykea/payments-ledger/internal/store"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	healthcheck := flag.String("healthcheck", "", "check that this URL answers 200, then exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if *healthcheck != "" {
		os.Exit(checkHealth(*healthcheck))
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("version", version)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch flag.Arg(0) {
	case "", "serve":
		err = serve(ctx, log)
	case "migrate":
		err = migrate(ctx, log)
	case "reconcile":
		err = reconcileOnce(ctx)
	default:
		err = fmt.Errorf("unknown command %q", flag.Arg(0))
	}
	if err != nil {
		log.Error("ledger stopped", "error", err)
		os.Exit(1)
	}
}

func checkHealth(url string) int {
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func open(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	return store.Open(ctx, url)
}

func migrate(ctx context.Context, log *slog.Logger) error {
	pool, err := open(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	applied, err := store.Migrate(ctx, pool)
	if err != nil {
		return err
	}
	log.Info("migrations applied", "applied", applied)
	return nil
}

func reconcileOnce(ctx context.Context) error {
	pool, err := open(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	rep, err := recon.Run(ctx, pool)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]any{"clean": rep.Clean(), "report": rep})
	if !rep.Clean() {
		return errors.New("reconciliation found mismatches")
	}
	return nil
}

func serve(ctx context.Context, log *slog.Logger) error {
	token := os.Getenv("LEDGER_API_TOKEN")
	if len(token) < 32 {
		return errors.New("LEDGER_API_TOKEN must be at least 32 characters")
	}
	interval, err := time.ParseDuration(env("RECONCILE_INTERVAL", "1h"))
	if err != nil {
		return fmt.Errorf("RECONCILE_INTERVAL: %w", err)
	}
	pool, err := open(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	if env("AUTO_MIGRATE", "true") == "true" {
		if _, err := store.Migrate(ctx, pool); err != nil {
			return err
		}
	}

	reg := metrics.New()
	var pub outbox.Publisher = outbox.Log{Logger: log}
	if u := os.Getenv("OUTBOX_WEBHOOK_URL"); u != "" {
		pub = outbox.Webhook{URL: u}
	}
	relay := &outbox.Relay{Pool: pool, Publisher: pub, Logger: log,
		OnPublished: func(n int) { reg.Add("ledger_outbox_published_total", "", float64(n)) }}

	bg, cancelBG := context.WithCancel(context.Background())
	defer cancelBG()
	go relay.Run(bg)
	go every(bg, 15*time.Second, func() {
		if n, err := outbox.Pending(bg, pool); err == nil {
			reg.Set("ledger_outbox_pending", "", float64(n))
		}
	})
	if interval > 0 {
		go every(bg, interval, func() {
			rep, err := recon.Run(bg, pool)
			if err != nil {
				log.Warn("reconciliation", "error", err)
				return
			}
			api.RecordReconciliation(reg, rep)
			if !rep.Clean() {
				log.Error("reconciliation found mismatches", "balances", rep.Balances, "unbalanced", rep.Unbalanced)
			}
		})
	}

	srv := &http.Server{
		Addr:              env("LEDGER_LISTEN", ":8080"),
		Handler:           (&api.Server{Ledger: ledger.New(pool), Pool: pool, Token: token, Metrics: reg, Logger: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("ledger listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx) // finish in-flight postings before stopping the relay
	cancelBG()
	return err
}

func every(ctx context.Context, d time.Duration, fn func()) {
	fn()
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

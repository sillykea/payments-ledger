// Package api is the ledger's HTTP interface.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sillykea/payments-ledger/internal/ledger"
	"github.com/sillykea/payments-ledger/internal/metrics"
	"github.com/sillykea/payments-ledger/internal/money"
	"github.com/sillykea/payments-ledger/internal/recon"
)

const maxBody = 64 << 10

// Server serves the API. Every /v1 route needs Authorization: Bearer <token>.
type Server struct {
	Ledger  *ledger.Ledger
	Pool    *pgxpool.Pool
	Token   string
	Metrics *metrics.Registry
	Logger  *slog.Logger
}

// Handler returns the HTTP handler with routing, auth and metrics.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.Metrics.Write(w)
	})
	mux.HandleFunc("POST /v1/accounts", s.auth(s.createAccount))
	mux.HandleFunc("GET /v1/accounts/{id}", s.auth(s.getAccount))
	mux.HandleFunc("GET /v1/accounts/{id}/entries", s.auth(s.listEntries))
	mux.HandleFunc("POST /v1/transfers", s.auth(s.transfer))
	mux.HandleFunc("POST /v1/conversions", s.auth(s.convert))
	mux.HandleFunc("GET /v1/transactions/{id}", s.auth(s.getTransaction))
	mux.HandleFunc("POST /v1/reconciliation", s.auth(s.reconcile))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "not_found", "no such route")
	})
	return s.observe(mux)
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		defer func() {
			if p := recover(); p != nil {
				s.Logger.Error("panic", "panic", p, "path", r.URL.Path)
				writeErr(sw, http.StatusInternalServerError, "internal", "internal error")
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			s.Metrics.Add("ledger_http_requests_total", metrics.Labels("route", route, "code", strconv.Itoa(sw.code)), 1)
			s.Metrics.Observe("ledger_http_request_duration_seconds", metrics.Labels("route", route), time.Since(start).Seconds())
		}()
		next.ServeHTTP(sw, r)
	})
}

func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(s.Token)) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "missing or wrong bearer token")
			return
		}
		h(w, r)
	}
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Pool.Ping(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "database unreachable")
		return
	}
	w.Write([]byte("ok"))
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_request", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": kind, "message": msg}})
}

// fail maps ledger errors to HTTP. Unexpected errors are logged, never echoed.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ledger.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, ledger.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds):
		writeErr(w, http.StatusUnprocessableEntity, "insufficient_funds", err.Error())
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		writeErr(w, http.StatusUnprocessableEntity, "currency_mismatch", err.Error())
	case errors.Is(err, ledger.ErrIdempotencyReuse):
		writeErr(w, http.StatusUnprocessableEntity, "idempotency_key_reused", err.Error())
	case errors.Is(err, money.ErrOverflow):
		writeErr(w, http.StatusUnprocessableEntity, "amount_out_of_range", err.Error())
	default:
		s.Logger.Error("request failed", "error", err)
		writeErr(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string `json:"name"`
		Currency      string `json:"currency"`
		AllowNegative bool   `json:"allow_negative"`
	}
	if !decode(w, r, &req) {
		return
	}
	a, err := s.Ledger.CreateAccount(r.Context(), req.Name, req.Currency, req.AllowNegative)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	a, err := s.Ledger.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) listEntries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	before, _ := strconv.ParseInt(q.Get("before"), 10, 64)
	entries, err := s.Ledger.ListEntries(r.Context(), r.PathValue("id"), before, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := map[string]any{"entries": entries}
	if n := len(entries); n > 0 {
		resp["next_before"] = entries[n-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) writePosting(w http.ResponseWriter, kind string, res ledger.Result, err error) {
	result := "posted"
	switch {
	case err != nil:
		result = "rejected"
	case res.Replayed:
		result = "replayed"
	}
	s.Metrics.Add("ledger_postings_total", metrics.Labels("kind", kind, "result", result), 1)
	if err != nil {
		s.fail(w, err)
		return
	}
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, res.Status, res.Transaction)
}

func (s *Server) transfer(w http.ResponseWriter, r *http.Request) {
	var req ledger.TransferRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := s.Ledger.Transfer(r.Context(), r.Header.Get("Idempotency-Key"), req)
	s.writePosting(w, "transfer", res, err)
}

func (s *Server) convert(w http.ResponseWriter, r *http.Request) {
	var req ledger.ConversionRequest
	if !decode(w, r, &req) {
		return
	}
	res, err := s.Ledger.Convert(r.Context(), r.Header.Get("Idempotency-Key"), req)
	s.writePosting(w, "conversion", res, err)
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	t, err := s.Ledger.GetTransaction(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	rep, err := recon.Run(r.Context(), s.Pool)
	if err != nil {
		s.fail(w, err)
		return
	}
	RecordReconciliation(s.Metrics, rep)
	writeJSON(w, http.StatusOK, map[string]any{"clean": rep.Clean(), "report": rep})
}

// RecordReconciliation updates the reconciliation gauges.
func RecordReconciliation(m *metrics.Registry, rep recon.Report) {
	problems := len(rep.Balances) + len(rep.Unbalanced)
	for _, t := range rep.CurrencyTotal {
		if t != 0 {
			problems++
		}
	}
	m.Set("ledger_reconciliation_mismatches", "", float64(problems))
	m.Set("ledger_reconciliation_last_run_timestamp_seconds", "", float64(rep.StartedAt.Unix()))
}

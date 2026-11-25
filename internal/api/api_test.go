package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sillykea/payments-ledger/internal/api"
	"github.com/sillykea/payments-ledger/internal/ledger"
	"github.com/sillykea/payments-ledger/internal/metrics"
	"github.com/sillykea/payments-ledger/internal/testdb"
)

const token = "test-token-0123456789-0123456789-abcdef"

type client struct {
	t   *testing.T
	url string
}

func newServer(t *testing.T) client {
	pool := testdb.New(t)
	s := &api.Server{Ledger: ledger.New(pool), Pool: pool, Token: token, Metrics: metrics.New(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return client{t, srv.URL}
}

func (c client) do(method, path string, body any, headers ...string) (int, http.Header, map[string]any) {
	c.t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		r = strings.NewReader(b)
	default:
		j, _ := json.Marshal(b)
		r = bytes.NewReader(j)
	}
	req, _ := http.NewRequest(method, c.url+path, r)
	req.Header.Set("Authorization", "Bearer "+token)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	json.Unmarshal(raw, &out)
	return resp.StatusCode, resp.Header, out
}

func errCode(m map[string]any) string {
	e, _ := m["error"].(map[string]any)
	s, _ := e["code"].(string)
	return s
}

func (c client) account(name, cur string, neg bool) string {
	code, _, body := c.do("POST", "/v1/accounts", map[string]any{"name": name, "currency": cur, "allow_negative": neg})
	if code != 201 {
		c.t.Fatalf("create account: %d %v", code, body)
	}
	return body["id"].(string)
}

func TestTransferFlow(t *testing.T) {
	c := newServer(t)
	bank := c.account("bank", "EUR", true)
	alice := c.account("alice", "EUR", false)
	bob := c.account("bob", "EUR", false)

	req := map[string]any{"from": bank, "to": alice, "amount": "50.00", "currency": "EUR"}
	code, h, body := c.do("POST", "/v1/transfers", req, "Idempotency-Key", "t1")
	if code != 201 || h.Get("Idempotent-Replayed") != "" {
		t.Fatalf("transfer: %d %v", code, body)
	}
	txID := body["id"].(string)

	code, h, body = c.do("POST", "/v1/transfers", req, "Idempotency-Key", "t1")
	if code != 201 || h.Get("Idempotent-Replayed") != "true" || body["id"] != txID {
		t.Errorf("replay: %d %v %v", code, h, body)
	}

	_, _, acc := c.do("GET", "/v1/accounts/"+alice, nil)
	if acc["balance"] != "50.00" || acc["balance_minor"] != float64(5000) {
		t.Errorf("alice: %v", acc)
	}

	cases := []struct {
		name string
		body any
		key  string
		code int
		err  string
	}{
		{"no key", map[string]any{"from": alice, "to": bob, "amount": "1.00", "currency": "EUR"}, "", 400, "invalid_request"},
		{"too much", map[string]any{"from": alice, "to": bob, "amount": "50.01", "currency": "EUR"}, "t2", 422, "insufficient_funds"},
		{"reused key", map[string]any{"from": bank, "to": alice, "amount": "1.00", "currency": "EUR"}, "t1", 422, "idempotency_key_reused"},
		{"unknown field", `{"from":"a","to":"b","amount":"1","currency":"EUR","fee":"1"}`, "t3", 400, "invalid_request"},
		{"bad json", `{`, "t4", 400, "invalid_request"},
		{"missing account", map[string]any{"from": alice, "to": "acc_nope", "amount": "1.00", "currency": "EUR"}, "t5", 404, "not_found"},
	}
	for _, tc := range cases {
		h := []string{}
		if tc.key != "" {
			h = []string{"Idempotency-Key", tc.key}
		}
		code, _, body := c.do("POST", "/v1/transfers", tc.body, h...)
		if code != tc.code || errCode(body) != tc.err {
			t.Errorf("%s: %d %v; want %d %s", tc.name, code, body, tc.code, tc.err)
		}
	}

	code, _, tx := c.do("GET", "/v1/transactions/"+txID, nil)
	if code != 200 || len(tx["entries"].([]any)) != 2 {
		t.Errorf("get transaction: %d %v", code, tx)
	}
	code, _, page := c.do("GET", "/v1/accounts/"+alice+"/entries?limit=10", nil)
	if code != 200 || len(page["entries"].([]any)) != 1 {
		t.Errorf("entries: %d %v", code, page)
	}
	code, _, rep := c.do("POST", "/v1/reconciliation", nil)
	if code != 200 || rep["clean"] != true {
		t.Errorf("reconciliation: %d %v", code, rep)
	}
}

func TestConversionEndpoint(t *testing.T) {
	c := newServer(t)
	bank := c.account("bank", "EUR", true)
	eur := c.account("eur", "EUR", false)
	usd := c.account("usd", "USD", false)
	c.do("POST", "/v1/transfers", map[string]any{"from": bank, "to": eur, "amount": "10.00", "currency": "EUR"}, "Idempotency-Key", "f")
	code, _, body := c.do("POST", "/v1/conversions", map[string]any{
		"from": eur, "to": usd, "amount": "10.00", "from_currency": "EUR", "to_currency": "USD", "rate": "1.0834",
	}, "Idempotency-Key", "c1")
	if code != 201 || len(body["entries"].([]any)) != 4 {
		t.Fatalf("conversion: %d %v", code, body)
	}
	_, _, acc := c.do("GET", "/v1/accounts/"+usd, nil)
	if acc["balance"] != "10.83" {
		t.Errorf("usd balance %v, want 10.83", acc["balance"])
	}
}

func TestAuthAndOps(t *testing.T) {
	c := newServer(t)
	for _, auth := range []string{"", "Bearer wrong", "Basic " + token, token} {
		req, _ := http.NewRequest("GET", c.url+"/v1/accounts/x", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, _ := http.DefaultClient.Do(req)
		if resp.StatusCode != 401 {
			t.Errorf("auth %q: %d, want 401", auth, resp.StatusCode)
		}
		resp.Body.Close()
	}
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, _ := http.Get(c.url + p)
		if resp.StatusCode != 200 {
			t.Errorf("%s: %d", p, resp.StatusCode)
		}
		resp.Body.Close()
	}
	c.account("a", "EUR", false)
	resp, _ := http.Get(c.url + "/metrics")
	m, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(m), `ledger_http_requests_total{route="POST /v1/accounts",code="201"} 1`) {
		t.Errorf("metrics missing request counter:\n%s", m)
	}
	code, _, body := c.do("POST", "/v1/accounts", map[string]any{"name": "x", "currency": "XYZ"})
	if code != 400 || errCode(body) != "invalid_request" {
		t.Errorf("bad currency: %d %v", code, body)
	}
	big := `{"name":"` + strings.Repeat("a", 70<<10) + `","currency":"EUR"}`
	if code, _, _ := c.do("POST", "/v1/accounts", big); code != 400 {
		t.Errorf("oversized body: %d, want 400", code)
	}
}

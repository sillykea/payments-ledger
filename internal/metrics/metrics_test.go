package metrics

import (
	"strings"
	"testing"
)

func TestWrite(t *testing.T) {
	r := New()
	r.Add("ledger_postings_total", Labels("kind", "transfer", "result", "posted"), 2)
	r.Set("ledger_outbox_pending", "", 3)
	r.Observe("ledger_http_request_duration_seconds", Labels("route", "GET /x"), 0.02)
	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		`ledger_postings_total{kind="transfer",result="posted"} 2`,
		"ledger_outbox_pending 3",
		`ledger_http_request_duration_seconds_bucket{route="GET /x",le="0.025"} 1`,
		`ledger_http_request_duration_seconds_bucket{route="GET /x",le="0.01"} 0`,
		`ledger_http_request_duration_seconds_count{route="GET /x"} 1`,
		"# TYPE ledger_outbox_pending gauge",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if Labels("k", `a"b`) != `k="a\"b"` {
		t.Errorf("label escaping: %s", Labels("k", `a"b`))
	}
}

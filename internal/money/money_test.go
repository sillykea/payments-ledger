package money

import (
	"errors"
	"math"
	"testing"
)

func TestParse(t *testing.T) {
	ok := []struct {
		in, cur string
		want    int64
	}{
		{"12.34", "EUR", 1234}, {"12", "EUR", 1200}, {"0.5", "USD", 50}, {"0.05", "NZD", 5},
		{"1500", "JPY", 1500}, {"1.234", "KWD", 1234}, {"0", "EUR", 0},
		{"92233720368547758.07", "EUR", math.MaxInt64},
	}
	for _, c := range ok {
		got, err := Parse(c.in, c.cur)
		if err != nil || got != c.want {
			t.Errorf("Parse(%q, %s) = %d, %v; want %d", c.in, c.cur, got, err, c.want)
		}
	}
	bad := []struct{ in, cur string }{
		{"1.234", "EUR"}, {"1.5", "JPY"}, {"-1", "EUR"}, {"+1", "EUR"}, {"1e3", "EUR"},
		{"", "EUR"}, {".5", "EUR"}, {"5.", "EUR"}, {"1,50", "EUR"}, {" 1", "EUR"},
		{"92233720368547758.08", "EUR"}, {"1", "XXX"},
	}
	for _, c := range bad {
		if _, err := Parse(c.in, c.cur); err == nil {
			t.Errorf("Parse(%q, %s) accepted, want error", c.in, c.cur)
		}
	}
}

func TestFormat(t *testing.T) {
	cases := []struct {
		minor int64
		cur   string
		want  string
	}{
		{1234, "EUR", "12.34"}, {5, "EUR", "0.05"}, {-5, "EUR", "-0.05"}, {0, "EUR", "0.00"},
		{1500, "JPY", "1500"}, {1, "KWD", "0.001"}, {math.MinInt64, "EUR", "-92233720368547758.08"},
	}
	for _, c := range cases {
		if got := Format(c.minor, c.cur); got != c.want {
			t.Errorf("Format(%d, %s) = %q, want %q", c.minor, c.cur, got, c.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, 99, 100, 101, 123456789} {
		for _, cur := range []string{"EUR", "JPY", "KWD"} {
			got, err := Parse(Format(v, cur), cur)
			if err != nil || got != v {
				t.Errorf("round trip %d %s: %d, %v", v, cur, got, err)
			}
		}
	}
}

func TestAddOverflow(t *testing.T) {
	if _, err := Add(math.MaxInt64, 1); !errors.Is(err, ErrOverflow) {
		t.Error("MaxInt64+1 did not overflow")
	}
	if _, err := Add(math.MinInt64, -1); !errors.Is(err, ErrOverflow) {
		t.Error("MinInt64-1 did not overflow")
	}
	if v, err := Add(-5, 3); err != nil || v != -2 {
		t.Errorf("Add(-5,3) = %d, %v", v, err)
	}
}

func TestConvert(t *testing.T) {
	cases := []struct {
		minor          int64
		from, to, rate string
		want           int64
	}{
		{10000, "EUR", "USD", "1.0834", 10834},   // 100.00 EUR -> 108.34 USD
		{10000, "EUR", "JPY", "162.45", 16245},   // 100.00 EUR -> 16245 JPY
		{16245, "JPY", "EUR", "0.006156", 10000}, // 16245 * 0.006156 = 100.0042 -> 100.00
		{1, "EUR", "USD", "1.5", 2},              // 1.5 cents -> 2 (half to even)
		{1, "EUR", "USD", "2.5", 2},              // 2.5 cents -> 2 (half to even)
		{3, "EUR", "USD", "0.5", 2},              // 1.5 -> 2
		{1000, "EUR", "KWD", "0.33", 3300},       // 10.00 EUR -> 3.300 KWD
	}
	for _, c := range cases {
		got, err := Convert(c.minor, c.from, c.to, c.rate)
		if err != nil || got != c.want {
			t.Errorf("Convert(%d %s->%s @%s) = %d, %v; want %d", c.minor, c.from, c.to, c.rate, got, err, c.want)
		}
	}
	for _, rate := range []string{"0", "-1", "abc", "1e2", "1/3", ""} {
		if _, err := Convert(100, "EUR", "USD", rate); err == nil {
			t.Errorf("rate %q accepted", rate)
		}
	}
}

func FuzzParseFormat(f *testing.F) {
	f.Add(int64(1234))
	f.Fuzz(func(t *testing.T, v int64) {
		if v < 0 {
			return
		}
		got, err := Parse(Format(v, "EUR"), "EUR")
		if err != nil || got != v {
			t.Fatalf("round trip %d: %d, %v", v, got, err)
		}
	})
}

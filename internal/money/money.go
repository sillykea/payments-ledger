// Package money represents amounts as integers in a currency's minor unit
// (cents for EUR, yen for JPY), never as floats. See docs/adr/0001.
package money

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// exponents is the number of minor-unit digits for each supported ISO 4217
// currency. Keeping an explicit list means an unknown code is rejected rather
// than silently assumed to have two decimals.
var exponents = map[string]int{
	"AUD": 2, "CAD": 2, "CHF": 2, "EUR": 2, "GBP": 2, "NZD": 2, "USD": 2,
	"SGD": 2, "SEK": 2, "NOK": 2, "DKK": 2, "PLN": 2, "CZK": 2,
	"JPY": 0, "KRW": 0,
	"BHD": 3, "KWD": 3,
}

var (
	ErrUnknownCurrency = errors.New("unknown currency")
	ErrInvalidAmount   = errors.New("invalid amount")
	ErrOverflow        = errors.New("amount out of range")
)

// Exponent returns the minor-unit digits for a currency.
func Exponent(currency string) (int, error) {
	e, ok := exponents[currency]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrUnknownCurrency, currency)
	}
	return e, nil
}

// ValidCurrency reports whether the code is supported.
func ValidCurrency(currency string) bool {
	_, ok := exponents[currency]
	return ok
}

// Parse turns a decimal string such as "12.34" into minor units (1234 for
// EUR). It rejects more decimals than the currency has, signs, exponents and
// anything that would not fit in an int64.
func Parse(s, currency string) (int64, error) {
	exp, err := Exponent(currency)
	if err != nil {
		return 0, err
	}
	whole, frac, hasDot := strings.Cut(s, ".")
	if whole == "" || (hasDot && frac == "") || len(frac) > exp || !digits(whole) || !digits(frac) {
		return 0, fmt.Errorf("%w: %q for %s", ErrInvalidAmount, s, currency)
	}
	frac += strings.Repeat("0", exp-len(frac))
	n, ok := new(big.Int).SetString(whole+frac, 10)
	if !ok || !n.IsInt64() {
		return 0, ErrOverflow
	}
	return n.Int64(), nil
}

func digits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Format renders minor units as a decimal string: 1234 EUR -> "12.34".
func Format(minor int64, currency string) string {
	exp, err := Exponent(currency)
	if err != nil {
		return fmt.Sprint(minor)
	}
	sign := ""
	u := new(big.Int).SetInt64(minor)
	if minor < 0 {
		sign = "-"
		u.Neg(u)
	}
	s := u.String()
	if exp == 0 {
		return sign + s
	}
	if len(s) <= exp {
		s = strings.Repeat("0", exp-len(s)+1) + s
	}
	return sign + s[:len(s)-exp] + "." + s[len(s)-exp:]
}

// Add returns a+b, or ErrOverflow instead of wrapping around.
func Add(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}

// Convert turns an amount in one currency into another at rate, where rate
// is a decimal string giving units of `to` per one unit of `from` (e.g.
// "1.0834" for EUR->USD). Exponent differences are handled, and the result is
// rounded half to even (banker's rounding), so rounding errors don't drift in
// one direction across many conversions.
func Convert(minor int64, from, to, rate string) (int64, error) {
	ef, err := Exponent(from)
	if err != nil {
		return 0, err
	}
	et, err := Exponent(to)
	if err != nil {
		return 0, err
	}
	r, ok := new(big.Rat).SetString(rate)
	if !ok || r.Sign() <= 0 || strings.ContainsAny(rate, "eE/") {
		return 0, fmt.Errorf("%w: rate %q", ErrInvalidAmount, rate)
	}
	// result = minor * 10^-ef * rate * 10^et
	v := new(big.Rat).SetInt64(minor)
	v.Mul(v, r)
	scale := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(abs(et-ef))), nil))
	if et >= ef {
		v.Mul(v, scale)
	} else {
		v.Quo(v, scale)
	}
	q := roundHalfEven(v)
	if !q.IsInt64() {
		return 0, ErrOverflow
	}
	return q.Int64(), nil
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func roundHalfEven(v *big.Rat) *big.Int {
	num, den := v.Num(), v.Denom()
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	// Compare 2*|rem| with den.
	twice := new(big.Int).Abs(rem)
	twice.Lsh(twice, 1)
	switch twice.Cmp(den) {
	case 1:
		q.Add(q, big.NewInt(int64(num.Sign())))
	case 0:
		if q.Bit(0) == 1 {
			q.Add(q, big.NewInt(int64(num.Sign())))
		}
	}
	return q
}

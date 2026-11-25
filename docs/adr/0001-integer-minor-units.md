# 1. Money as integer minor units

Status: accepted

## Context

Floating point can't represent 0.10 exactly, so sums of floats drift by
fractions of a cent. A ledger has to be exact to the smallest unit, forever.

## Decision

- Amounts are `bigint` in the currency's minor unit (cents for EUR, yen for
  JPY, fils for KWD), in Go and in Postgres. The exponent per currency comes
  from an explicit ISO 4217 table, so an unknown code is rejected rather than
  assumed to have two decimals.
- The API takes and returns decimal *strings* ("12.50"). JSON numbers are
  floats in most clients, so a number would reintroduce the problem at the
  edge. Parsing rejects more decimals than the currency has, signs and
  exponent notation.
- Additions check for int64 overflow instead of wrapping.
- Conversions use exact rational arithmetic and round half to even once, at
  the end, so rounding doesn't drift in one direction across many trades.

## Consequences

- A balance tops out at about 92 quadrillion minor units, far beyond need.
- Fractional amounts below the minor unit (some FX and interest cases) can't
  be stored; they'd need a separate higher-precision column. Not needed here.

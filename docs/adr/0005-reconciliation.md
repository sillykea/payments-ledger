# 5. Reconciliation as a separate check

Status: accepted

## Context

The database already refuses unbalanced transactions, entries in the wrong
currency, edits to entries and negative customer balances. What it can't see
is a cached balance that drifted from the entries, for example from a manual
`UPDATE` or a future bug.

## Decision

`recon.Run` reads one REPEATABLE READ snapshot (so postings landing mid-run
can't cause false alarms) and checks that:

1. every account's cached balance equals the sum of its entries,
2. every transaction's entries sum to zero per currency and it has at least
   two entries, and
3. each currency nets to zero across the whole ledger.

It runs on a timer inside the service (`RECONCILE_INTERVAL`), on demand via
`POST /v1/reconciliation`, and as `ledger reconcile`, which exits non-zero on
a mismatch so a cron job or CI step fails loudly.

## Consequences

- Cost grows with the number of entries. At large scale it would run against
  a read replica, or incrementally from a checkpoint.
- `ledger_reconciliation_mismatches` should alert on anything above zero.

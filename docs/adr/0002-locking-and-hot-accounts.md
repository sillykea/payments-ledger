# 2. Row locks in a fixed order

Status: accepted

## Context

Two concurrent transfers out of the same account must not both see the old
balance and both succeed (a lost update that creates money). Options:
SERIALIZABLE isolation with retries, optimistic version columns with
retries, or pessimistic row locks.

## Decision

Each posting runs at READ COMMITTED and takes `SELECT ... FOR UPDATE` on every
account it touches, **sorted by account ID**. The balance check and update
happen while the locks are held. Because every transaction locks in the same
order, A→B and B→A at the same moment queue instead of deadlocking.

The cached balance on `accounts` is a convenience. The database also enforces
`balance >= 0` for non-negative accounts with a CHECK constraint, and
reconciliation recomputes every balance from entries (ADR 0005).

## Consequences

- No retry loops in application code, and no serialization failures to tune.
- A single very hot account (one merchant receiving most payments) serialises
  its writers. The load test shows the cost: with 50 accounts the p99 is
  ~180 ms; with 5 accounts it is ~680 ms and throughput halves. The usual
  fixes, if a real hot account appears, are sharding it into N sub-accounts
  summed for display, or batching its credits asynchronously.

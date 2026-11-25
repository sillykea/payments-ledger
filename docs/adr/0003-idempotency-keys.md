# 3. Idempotency keys stored with the posting

Status: accepted

## Context

Clients retry on timeouts. Without protection, a timeout after the server
committed but before the client heard back turns one payment into two.

## Decision

- `POST /v1/transfers` and `/v1/conversions` require an `Idempotency-Key`.
- The key row is inserted **in the same database transaction** as the
  entries. If the posting rolls back, so does the key; if it commits, the key
  and the stored response commit with it. There is no window where money
  moved but the key wasn't recorded, or the reverse.
- A concurrent request with the same key blocks on the unique index until the
  first one finishes, then replays its stored response with
  `Idempotent-Replayed: true`. A test fires 20 identical requests at once and
  checks exactly one transaction exists.
- The key is bound to a hash of the request. Reusing a key with a different
  body is refused (`422 idempotency_key_reused`), which catches client bugs
  that would otherwise silently return the wrong result.

## Consequences

- Failed postings (for example insufficient funds) are not remembered, so
  retrying the same key after funding the account succeeds. Stripe stores
  failures too; storing only successes is simpler and was a deliberate
  choice.
- Keys are kept forever. A production system would expire them after a
  retention period (e.g. 30 days) with a scheduled delete.

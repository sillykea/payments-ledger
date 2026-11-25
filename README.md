# payments-ledger

[![ci](https://github.com/sillykea/payments-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/sillykea/payments-ledger/actions/workflows/ci.yml)

A double-entry ledger service in Go and Postgres: accounts, transfers and multi-currency conversions, with idempotent APIs, a transactional outbox for events and a reconciliation job that proves the books balance.

It's built around the failures that actually cost money: a retry that charges twice, two concurrent payments that both spend the same balance, an event announced for a payment that rolled back, and a cached balance that quietly drifts from the entries.

```mermaid
flowchart LR
    C[Client] -->|POST /v1/transfers<br/>Idempotency-Key| API[ledger API]
    API --> TX{{One Postgres transaction}}
    TX --> L[lock accounts<br/>in ID order]
    L --> E[balanced entries<br/>+ balances]
    E --> O[outbox event]
    O --> K[idempotency key<br/>+ stored response]
    R[outbox relay<br/>SKIP LOCKED] -->|at least once| W[Webhook / consumer]
    REC[reconciliation] -.reads snapshot.-> TX
    API --> M[/metrics/]
```

## What it guarantees

| Failure | How it's prevented | Proved by |
|---|---|---|
| Client retries after a timeout | Idempotency key stored in the same transaction as the entries; retries replay the stored response | 20 simultaneous identical requests post exactly once |
| Two payments spend the same balance | `SELECT … FOR UPDATE` on every account, always in ID order, so no deadlocks | 16 workers, random transfers both ways: no negative balance, money conserved |
| Unbalanced or edited books | Postgres refuses: deferred trigger checks every transaction sums to zero per currency; entries are append-only; currency must match the account | Tests that bypass the service and write SQL directly are rejected |
| Event sent for a payment that rolled back, or lost | Transactional outbox; relay marks events published only after a 2xx | 4 concurrent relays never double-send; a failing receiver loses nothing |
| Cached balance drifts from entries | Reconciliation recomputes every balance from entries in one snapshot | A test corrupts a balance and checks it's reported |
| Floating-point rounding | Integer minor units, decimal strings on the wire, exact FX maths with banker's rounding | Unit tests and a fuzz test |

## Load test

`cmd/loadtest` sends concurrent transfers, including 10% deliberate retries that reuse an earlier key, then checks every balance and runs reconciliation. Run on a 2-vCPU machine with Postgres 16, the service and the load generator all sharing it:

| Scenario | Throughput | p50 | p95 | p99 | Duplicate postings | Books balance |
|---|---|---|---|---|---|---|
| 32 clients, 50 accounts | 957 postings/s (1,062 req/s) | 22 ms | 82 ms | 142 ms | 0 of 3,086 retries | ✅ |
| 32 clients, 5 hot accounts | 470 postings/s (523 req/s) | 13 ms | 319 ms | 605 ms | 0 of 1,020 retries | ✅ |

The second row is the honest cost of row locks: when everyone writes to the same few accounts, they queue. [ADR 0002](docs/adr/0002-locking-and-hot-accounts.md) covers why that trade-off was chosen and how a real hot account would be split. Clients are a closed loop (one request at a time each), so these are service times under that concurrency, not latencies at a fixed arrival rate. In the same runs, all 38,155 outbox events were delivered and none were left pending.

## Try it in 2 minutes

```bash
make up          # Postgres, the ledger, and an event consumer
make loadtest    # 30 seconds of traffic, then checks the books
make reconcile   # run reconciliation from the CLI
```

Or by hand:

```bash
T=demo-token-change-me-0123456789abcdef
api() { curl -s -H "Authorization: Bearer $T" -H 'Content-Type: application/json' "$@"; }

BANK=$(api localhost:8080/v1/accounts -d '{"name":"bank","currency":"EUR","allow_negative":true}' | jq -r .id)
ALICE=$(api localhost:8080/v1/accounts -d '{"name":"alice","currency":"EUR"}' | jq -r .id)

api localhost:8080/v1/transfers -H 'Idempotency-Key: fund-alice-1' \
  -d "{\"from\":\"$BANK\",\"to\":\"$ALICE\",\"amount\":\"100.00\",\"currency\":\"EUR\"}"
# Send the same request again: same transaction back, header Idempotent-Replayed: true.

api localhost:8080/v1/accounts/$ALICE          # "balance": "100.00"
```

## API

All `/v1` routes need `Authorization: Bearer $LEDGER_API_TOKEN`. Amounts are decimal strings.

| Method | Path | |
|---|---|---|
| `POST` | `/v1/accounts` | `{name, currency, allow_negative}`. Contra accounts (an external bank) set `allow_negative`. |
| `GET` | `/v1/accounts/{id}` | Account and current balance |
| `GET` | `/v1/accounts/{id}/entries?limit=&before=` | Entries newest first, keyset-paginated |
| `POST` | `/v1/transfers` | `{from, to, amount, currency, description}` + `Idempotency-Key` |
| `POST` | `/v1/conversions` | `{from, to, amount, from_currency, to_currency, rate}` + `Idempotency-Key`. Posts four entries through per-currency FX pool accounts, so each currency balances on its own. |
| `GET` | `/v1/transactions/{id}` | A transaction and its entries |
| `POST` | `/v1/reconciliation` | Run reconciliation now and return the report |
| `GET` | `/healthz` `/readyz` `/metrics` | Liveness, readiness (database reachable), Prometheus |

Errors are `{"error": {"code", "message"}}` with codes `invalid_request` (400), `unauthorized` (401), `not_found` (404), `insufficient_funds`, `currency_mismatch`, `idempotency_key_reused`, `amount_out_of_range` (422).

## Design decisions

1. [Money as integer minor units](docs/adr/0001-integer-minor-units.md)
2. [Row locks in a fixed order, and the hot-account cost](docs/adr/0002-locking-and-hot-accounts.md)
3. [Idempotency keys stored with the posting](docs/adr/0003-idempotency-keys.md)
4. [Transactional outbox for events](docs/adr/0004-transactional-outbox.md)
5. [Reconciliation as a separate check](docs/adr/0005-reconciliation.md)

## Layout

```
cmd/ledger        service: serve | migrate | reconcile
cmd/loadtest      concurrent load + correctness check
cmd/eventsink     demo consumer that de-duplicates events
internal/money    minor units, parsing, FX conversion
internal/ledger   postings, locking, idempotency, outbox writes
internal/outbox   relay and publishers (webhook, log)
internal/recon    reconciliation
internal/api      HTTP handlers, auth, errors
internal/store    pool and embedded SQL migrations
```

Tests run against a real Postgres (CI starts one; locally `make up` then `make test`). Each test gets its own schema, so packages run in parallel.

## Configuration

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | required | e.g. `postgres://user:pass@host:5432/ledger` |
| `LEDGER_API_TOKEN` | required | 32+ characters |
| `LEDGER_LISTEN` | `:8080` | |
| `OUTBOX_WEBHOOK_URL` | log only | Where events are POSTed |
| `OUTBOX_WEBHOOK_SECRET` | unset | Signs each event: `X-Ledger-Signature: t=<unix>,v1=<HMAC-SHA256 of "<t>.<body>">` |
| `RECONCILE_INTERVAL` | `1h` | `0` turns the background run off |
| `AUTO_MIGRATE` | `true` | Apply migrations on start (guarded by an advisory lock) |

## Roadmap

- Deploy on [llm-gateway-platform](https://github.com/sillykea/llm-gateway-platform) with RDS Postgres
- Reversal transactions (`POST /v1/transactions/{id}/reverse`) instead of corrections
- Idempotency-key and outbox retention jobs
- Per-client API keys and scopes instead of one token
- Kafka publisher, feeding the planned real-time fraud detection service

## License

MIT

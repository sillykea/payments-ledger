# 4. Transactional outbox for events

Status: accepted

## Context

Other services need to know when money moves. Publishing directly to a queue
after committing loses the event if the process dies in between; publishing
before committing announces a payment that might still roll back.

## Decision

- Each posting writes a `transaction.posted` event to the `outbox` table in
  the same database transaction as its entries.
- A relay claims batches with `FOR UPDATE SKIP LOCKED`, delivers them, and
  marks them published. Several relays can run at once without sending the
  same event concurrently (tested with four relays and 60 events).
- Delivery is **at least once**. An event is marked published only after the
  receiver answers 2xx, so a crash between delivery and marking resends it.
  Each event carries a unique `event_id` (also sent as the webhook's
  `Idempotency-Key`), and consumers de-duplicate on it. `cmd/eventsink` shows
  the consumer side.
- A batch stops at the first failure, so events from a batch are delivered in
  order; the failed event and the rest stay pending, with an attempts count.

## Consequences

- No message broker is needed to start: the default publisher is an HTTP
  webhook. Swapping in Kafka or SQS means implementing one `Publish` method.
- Published rows accumulate; a retention job would delete old ones.
- `ledger_outbox_pending` shows backlog; alert if it keeps growing.

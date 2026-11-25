-- Double-entry ledger. The database enforces the rules that matter most, so
-- a bug in the application can't silently corrupt the books:
--   * every transaction's entries sum to zero, per currency (deferred check)
--   * an entry's currency matches its account's currency
--   * entries are append-only: no UPDATE, no DELETE
--   * a balance can't go negative unless the account allows it

CREATE TABLE accounts (
    id             text PRIMARY KEY,
    name           text NOT NULL,
    currency       char(3) NOT NULL,
    -- Contra accounts (an external bank, FX pools) may go negative; customer
    -- accounts may not.
    allow_negative boolean NOT NULL DEFAULT false,
    -- Cached sum of this account's entries, updated in the same transaction
    -- that writes them. Reconciliation recomputes it from entries.
    balance_minor  bigint NOT NULL DEFAULT 0,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT balance_floor CHECK (allow_negative OR balance_minor >= 0)
);

CREATE TABLE transactions (
    id          text PRIMARY KEY,
    kind        text NOT NULL CHECK (kind IN ('transfer', 'conversion')),
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE entries (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id text NOT NULL REFERENCES transactions (id),
    account_id     text NOT NULL REFERENCES accounts (id),
    amount_minor   bigint NOT NULL CHECK (amount_minor <> 0),
    currency       char(3) NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX entries_account ON entries (account_id, id);
CREATE INDEX entries_transaction ON entries (transaction_id);

CREATE FUNCTION entries_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM entries WHERE transaction_id = NEW.transaction_id
        GROUP BY currency HAVING sum(amount_minor) <> 0
    ) THEN
        RAISE EXCEPTION 'transaction % is not balanced', NEW.transaction_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

-- Deferred to commit, because a transaction's entries are inserted one by one.
CREATE CONSTRAINT TRIGGER entries_balanced
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION entries_check_balanced();

CREATE FUNCTION entries_check_currency() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.currency <> (SELECT currency FROM accounts WHERE id = NEW.account_id) THEN
        RAISE EXCEPTION 'entry currency % does not match account %', NEW.currency, NEW.account_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER entries_currency
    BEFORE INSERT ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_check_currency();

CREATE FUNCTION entries_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'entries are append-only: post a reversing transaction instead'
        USING ERRCODE = 'insufficient_privilege';
END $$;

CREATE TRIGGER entries_immutable
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_append_only();

-- One row per Idempotency-Key. The unique key makes a concurrent retry wait
-- for the first attempt, then replay its stored response.
CREATE TABLE idempotency_keys (
    key            text PRIMARY KEY,
    request_hash   text NOT NULL,
    status_code    int NOT NULL,
    response       jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Transactional outbox: events are written in the same transaction as the
-- entries, and a relay publishes them afterwards (docs/adr/0004).
CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     text NOT NULL UNIQUE,
    topic        text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    attempts     int NOT NULL DEFAULT 0,
    published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;

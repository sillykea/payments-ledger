// Package ledger posts double-entry transactions to Postgres.
//
// Every money movement is one database transaction that, together:
//   - locks the affected accounts in a fixed order (docs/adr/0002),
//   - checks balances and writes balanced entries,
//   - updates the cached balances,
//   - writes an outbox event (docs/adr/0004), and
//   - stores the response under the caller's idempotency key (docs/adr/0003).
//
// Either all of that happens or none of it does.
package ledger

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sillykea/payments-ledger/internal/money"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrCurrencyMismatch  = errors.New("currency mismatch")
	ErrInvalid           = errors.New("invalid request")
	ErrIdempotencyReuse  = errors.New("idempotency key reused with a different request")
)

// Ledger is safe for concurrent use.
type Ledger struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// New returns a Ledger backed by pool.
func New(pool *pgxpool.Pool) *Ledger {
	return &Ledger{pool: pool, now: time.Now}
}

// Account is a ledger account. Balance is in minor units.
type Account struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Currency      string    `json:"currency"`
	AllowNegative bool      `json:"allow_negative"`
	BalanceMinor  int64     `json:"balance_minor"`
	Balance       string    `json:"balance"`
	CreatedAt     time.Time `json:"created_at"`
}

// Entry is one leg of a transaction.
type Entry struct {
	ID            int64     `json:"id"`
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	AmountMinor   int64     `json:"amount_minor"`
	Amount        string    `json:"amount"`
	Currency      string    `json:"currency"`
	CreatedAt     time.Time `json:"created_at"`
}

// Transaction is a balanced set of entries.
type Transaction struct {
	ID          string    `json:"id"`
	Kind        string    `json:"kind"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	Entries     []Entry   `json:"entries"`
}

func newID(prefix string) string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	return prefix + "_" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

// CreateAccount opens an account with a zero balance.
func (l *Ledger) CreateAccount(ctx context.Context, name, currency string, allowNegative bool) (Account, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 200 {
		return Account{}, fmt.Errorf("%w: name must be 1-200 characters", ErrInvalid)
	}
	if !money.ValidCurrency(currency) {
		return Account{}, fmt.Errorf("%w: unsupported currency %q", ErrInvalid, currency)
	}
	a := Account{ID: newID("acc"), Name: name, Currency: currency, AllowNegative: allowNegative}
	err := l.pool.QueryRow(ctx,
		`INSERT INTO accounts (id, name, currency, allow_negative) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		a.ID, a.Name, a.Currency, a.AllowNegative).Scan(&a.CreatedAt)
	a.Balance = money.Format(0, currency)
	return a, err
}

// GetAccount returns an account with its current balance.
func (l *Ledger) GetAccount(ctx context.Context, id string) (Account, error) {
	var a Account
	err := l.pool.QueryRow(ctx,
		`SELECT id, name, currency, allow_negative, balance_minor, created_at FROM accounts WHERE id = $1`, id).
		Scan(&a.ID, &a.Name, &a.Currency, &a.AllowNegative, &a.BalanceMinor, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	a.Balance = money.Format(a.BalanceMinor, a.Currency)
	return a, err
}

// ListEntries returns an account's entries newest first. Pass the last ID
// you saw as before to get the next page (keyset pagination: each page is an
// index range scan, however deep you page).
func (l *Ledger) ListEntries(ctx context.Context, accountID string, before int64, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if _, err := l.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}
	if before <= 0 {
		before = 1<<63 - 1
	}
	rows, err := l.pool.Query(ctx, `
		SELECT id, transaction_id, account_id, amount_minor, currency, created_at
		FROM entries WHERE account_id = $1 AND id < $2 ORDER BY id DESC LIMIT $3`,
		accountID, before, limit)
	if err != nil {
		return nil, err
	}
	return collectEntries(rows)
}

func collectEntries(rows pgx.Rows) ([]Entry, error) {
	defer rows.Close()
	out := []Entry{}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.AmountMinor, &e.Currency, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Amount = money.Format(e.AmountMinor, e.Currency)
		out = append(out, e)
	}
	return out, rows.Err()
}

// GetTransaction returns a transaction and its entries.
func (l *Ledger) GetTransaction(ctx context.Context, id string) (Transaction, error) {
	var t Transaction
	err := l.pool.QueryRow(ctx, `SELECT id, kind, description, created_at FROM transactions WHERE id = $1`, id).
		Scan(&t.ID, &t.Kind, &t.Description, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transaction{}, ErrNotFound
	}
	if err != nil {
		return Transaction{}, err
	}
	rows, err := l.pool.Query(ctx, `
		SELECT id, transaction_id, account_id, amount_minor, currency, created_at
		FROM entries WHERE transaction_id = $1 ORDER BY id`, id)
	if err != nil {
		return Transaction{}, err
	}
	t.Entries, err = collectEntries(rows)
	return t, err
}

// leg is one entry to post: amount is signed (negative = debit the account's
// balance, positive = credit it).
type leg struct {
	account string
	amount  int64
}

type lockedAccount struct {
	currency      string
	allowNegative bool
	balance       int64
}

// lockAccounts takes row locks on the accounts in ID order. Every posting
// locks in the same order, so two transfers between the same pair of
// accounts in opposite directions queue instead of deadlocking.
func lockAccounts(ctx context.Context, tx pgx.Tx, ids []string) (map[string]*lockedAccount, error) {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	rows, err := tx.Query(ctx, `
		SELECT id, currency, allow_negative, balance_minor FROM accounts
		WHERE id = ANY($1) ORDER BY id FOR UPDATE`, sorted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*lockedAccount{}
	for rows.Next() {
		var id string
		a := &lockedAccount{}
		if err := rows.Scan(&id, &a.currency, &a.allowNegative, &a.balance); err != nil {
			return nil, err
		}
		out[id] = a
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if out[id] == nil {
			return nil, fmt.Errorf("%w: account %s", ErrNotFound, id)
		}
	}
	return out, nil
}

// post writes a balanced transaction inside tx. The accounts in legs must
// already be locked.
func post(ctx context.Context, tx pgx.Tx, locked map[string]*lockedAccount, kind, description string, legs []leg) (string, error) {
	sums := map[string]int64{}
	for _, lg := range legs {
		a := locked[lg.account]
		next, err := money.Add(a.balance, lg.amount)
		if err != nil {
			return "", err
		}
		if next < 0 && !a.allowNegative {
			return "", fmt.Errorf("%w: account %s", ErrInsufficientFunds, lg.account)
		}
		a.balance = next
		if sums[a.currency], err = money.Add(sums[a.currency], lg.amount); err != nil {
			return "", err
		}
	}
	for cur, s := range sums {
		if s != 0 {
			// The database would reject this too; failing here gives a clear error.
			return "", fmt.Errorf("internal: %s legs sum to %d, not zero", cur, s)
		}
	}

	id := newID("txn")
	if _, err := tx.Exec(ctx, `INSERT INTO transactions (id, kind, description) VALUES ($1, $2, $3)`,
		id, kind, description); err != nil {
		return "", err
	}
	batch := &pgx.Batch{}
	for _, lg := range legs {
		batch.Queue(`INSERT INTO entries (transaction_id, account_id, amount_minor, currency) VALUES ($1, $2, $3, $4)`,
			id, lg.account, lg.amount, locked[lg.account].currency)
	}
	for accID, a := range locked {
		batch.Queue(`UPDATE accounts SET balance_minor = $2 WHERE id = $1`, accID, a.balance)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return "", err
	}
	return id, nil
}

// inTx runs fn in a transaction and commits it. Constraint violations raised
// by the database at commit (unbalanced entries, a negative balance) come back
// as errors, and the whole transaction is rolled back.
func (l *Ledger) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

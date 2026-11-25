// Package recon checks the ledger against itself. It recomputes every
// account's balance from its entries and checks every transaction balances,
// so a bug that updated a cached balance without an entry (or the reverse)
// shows up as a mismatch instead of going unnoticed. See docs/adr/0005.
package recon

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BalanceMismatch is an account whose cached balance differs from its entries.
type BalanceMismatch struct {
	AccountID string `json:"account_id"`
	Cached    int64  `json:"cached_minor"`
	FromEntry int64  `json:"from_entries_minor"`
}

// UnbalancedTransaction is a transaction whose entries don't sum to zero.
type UnbalancedTransaction struct {
	TransactionID string `json:"transaction_id"`
	Currency      string `json:"currency"`
	Sum           int64  `json:"sum_minor"`
}

// Report is one reconciliation run.
type Report struct {
	StartedAt     time.Time               `json:"started_at"`
	Duration      string                  `json:"duration"`
	Accounts      int64                   `json:"accounts_checked"`
	Transactions  int64                   `json:"transactions_checked"`
	Balances      []BalanceMismatch       `json:"balance_mismatches"`
	Unbalanced    []UnbalancedTransaction `json:"unbalanced_transactions"`
	CurrencyTotal map[string]int64        `json:"currency_totals_minor"`
}

// Clean reports whether the run found nothing wrong.
func (r Report) Clean() bool {
	for _, total := range r.CurrencyTotal {
		if total != 0 {
			return false
		}
	}
	return len(r.Balances) == 0 && len(r.Unbalanced) == 0
}

// Run reconciles inside one REPEATABLE READ snapshot, so postings that land
// while it runs can't make it report a false mismatch.
func Run(ctx context.Context, pool *pgxpool.Pool) (Report, error) {
	rep := Report{StartedAt: time.Now().UTC(), Balances: []BalanceMismatch{}, Unbalanced: []UnbalancedTransaction{}, CurrencyTotal: map[string]int64{}}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return rep, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck

	if err := tx.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&rep.Accounts); err != nil {
		return rep, err
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM transactions`).Scan(&rep.Transactions); err != nil {
		return rep, err
	}

	rows, err := tx.Query(ctx, `
		SELECT a.id, a.balance_minor, COALESCE(e.total, 0)
		FROM accounts a
		LEFT JOIN (SELECT account_id, sum(amount_minor)::bigint AS total FROM entries GROUP BY account_id) e
		  ON e.account_id = a.id
		WHERE a.balance_minor <> COALESCE(e.total, 0)
		ORDER BY a.id`)
	if err != nil {
		return rep, err
	}
	rep.Balances, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (BalanceMismatch, error) {
		var m BalanceMismatch
		return m, r.Scan(&m.AccountID, &m.Cached, &m.FromEntry)
	})
	if err != nil {
		return rep, err
	}

	rows, err = tx.Query(ctx, `
		SELECT transaction_id, currency, sum(amount_minor)::bigint FROM entries
		GROUP BY transaction_id, currency HAVING sum(amount_minor) <> 0
		UNION ALL
		-- A transaction with fewer than two entries can't be a real posting.
		SELECT t.id, '', 0 FROM transactions t
		WHERE (SELECT count(*) FROM entries e WHERE e.transaction_id = t.id) < 2
		ORDER BY 1`)
	if err != nil {
		return rep, err
	}
	rep.Unbalanced, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (UnbalancedTransaction, error) {
		var u UnbalancedTransaction
		return u, r.Scan(&u.TransactionID, &u.Currency, &u.Sum)
	})
	if err != nil {
		return rep, err
	}

	// Across the whole ledger each currency must net to zero too.
	rows, err = tx.Query(ctx, `SELECT currency, sum(amount_minor)::bigint FROM entries GROUP BY currency ORDER BY 1`)
	if err != nil {
		return rep, err
	}
	defer rows.Close()
	for rows.Next() {
		var cur string
		var total int64
		if err := rows.Scan(&cur, &total); err != nil {
			return rep, err
		}
		rep.CurrencyTotal[cur] = total
	}
	rep.Duration = time.Since(rep.StartedAt).Round(time.Millisecond).String()
	return rep, rows.Err()
}

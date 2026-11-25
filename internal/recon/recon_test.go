package recon_test

import (
	"context"
	"testing"

	"github.com/sillykea/payments-ledger/internal/ledger"
	"github.com/sillykea/payments-ledger/internal/recon"
	"github.com/sillykea/payments-ledger/internal/testdb"
)

// Reconciliation must catch a cached balance that drifted from its entries,
// which is exactly the bug the database constraints can't see.
func TestDetectsDrift(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	l := ledger.New(pool)
	bank, _ := l.CreateAccount(ctx, "bank", "EUR", true)
	alice, _ := l.CreateAccount(ctx, "alice", "EUR", false)
	if _, err := l.Transfer(ctx, "k", ledger.TransferRequest{From: bank.ID, To: alice.ID, Amount: "10.00", Currency: "EUR"}); err != nil {
		t.Fatal(err)
	}
	if rep, _ := recon.Run(ctx, pool); !rep.Clean() {
		t.Fatalf("clean ledger reported dirty: %+v", rep)
	}
	if _, err := pool.Exec(ctx, `UPDATE accounts SET balance_minor = balance_minor + 1 WHERE id = $1`, alice.ID); err != nil {
		t.Fatal(err)
	}
	rep, err := recon.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Clean() || len(rep.Balances) != 1 || rep.Balances[0].AccountID != alice.ID ||
		rep.Balances[0].Cached != 1001 || rep.Balances[0].FromEntry != 1000 {
		t.Errorf("drift not reported correctly: %+v", rep)
	}
}

// The database itself must refuse unbalanced or edited entries, even from
// code that bypasses the ledger package.
func TestDatabaseGuards(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	l := ledger.New(pool)
	a, _ := l.CreateAccount(ctx, "a", "EUR", true)
	b, _ := l.CreateAccount(ctx, "b", "EUR", true)
	u, _ := l.CreateAccount(ctx, "u", "USD", true)
	res, err := l.Transfer(ctx, "k", ledger.TransferRequest{From: a.ID, To: b.ID, Amount: "1.00", Currency: "EUR"})
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := pool.Begin(ctx)
	tx.Exec(ctx, `INSERT INTO transactions (id, kind) VALUES ('txn_bad', 'transfer')`)
	tx.Exec(ctx, `INSERT INTO entries (transaction_id, account_id, amount_minor, currency) VALUES ('txn_bad', $1, 500, 'EUR')`, a.ID)
	if err := tx.Commit(ctx); err == nil {
		t.Error("unbalanced transaction committed")
	}

	if _, err := pool.Exec(ctx, `INSERT INTO entries (transaction_id, account_id, amount_minor, currency) VALUES ($1, $2, 5, 'EUR')`,
		res.Transaction.ID, u.ID); err == nil {
		t.Error("entry in the wrong currency accepted")
	}
	if _, err := pool.Exec(ctx, `UPDATE entries SET amount_minor = 1 WHERE transaction_id = $1`, res.Transaction.ID); err == nil {
		t.Error("entry was updated")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM entries WHERE transaction_id = $1`, res.Transaction.ID); err == nil {
		t.Error("entry was deleted")
	}
	c, _ := l.CreateAccount(ctx, "c", "EUR", false)
	if _, err := pool.Exec(ctx, `UPDATE accounts SET balance_minor = -1 WHERE id = $1`, c.ID); err == nil {
		t.Error("customer balance set negative")
	}
}

package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/sillykea/payments-ledger/internal/ledger"
	"github.com/sillykea/payments-ledger/internal/recon"
	"github.com/sillykea/payments-ledger/internal/testdb"
)

var ctx = context.Background()

func setup(t *testing.T) (*ledger.Ledger, func() recon.Report) {
	pool := testdb.New(t)
	l := ledger.New(pool)
	return l, func() recon.Report {
		t.Helper()
		rep, err := recon.Run(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
}

func mustAccount(t *testing.T, l *ledger.Ledger, name, cur string, allowNeg bool) ledger.Account {
	t.Helper()
	a, err := l.CreateAccount(ctx, name, cur, allowNeg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func balance(t *testing.T, l *ledger.Ledger, id string) int64 {
	t.Helper()
	a, err := l.GetAccount(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return a.BalanceMinor
}

func transfer(l *ledger.Ledger, key, from, to, amount string) (ledger.Result, error) {
	return l.Transfer(ctx, key, ledger.TransferRequest{From: from, To: to, Amount: amount, Currency: "EUR"})
}

func TestTransfer(t *testing.T) {
	l, reconcile := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	alice := mustAccount(t, l, "alice", "EUR", false)

	res, err := transfer(l, "k1", bank.ID, alice.ID, "100.00")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 201 || res.Replayed || len(res.Transaction.Entries) != 2 {
		t.Fatalf("unexpected result %+v", res)
	}
	if got := balance(t, l, alice.ID); got != 10000 {
		t.Errorf("alice = %d, want 10000", got)
	}
	if got := balance(t, l, bank.ID); got != -10000 {
		t.Errorf("bank = %d, want -10000", got)
	}
	if rep := reconcile(); !rep.Clean() {
		t.Errorf("reconciliation not clean: %+v", rep)
	}
}

func TestInsufficientFundsChangesNothing(t *testing.T) {
	l, reconcile := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	alice := mustAccount(t, l, "alice", "EUR", false)
	bob := mustAccount(t, l, "bob", "EUR", false)
	if _, err := transfer(l, "fund", bank.ID, alice.ID, "10.00"); err != nil {
		t.Fatal(err)
	}
	_, err := transfer(l, "too-much", alice.ID, bob.ID, "10.01")
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want insufficient funds", err)
	}
	if balance(t, l, alice.ID) != 1000 || balance(t, l, bob.ID) != 0 {
		t.Error("a failed transfer changed balances")
	}
	// The key wasn't consumed by the failure, so the same key works later.
	if _, err := transfer(l, "too-much", alice.ID, bob.ID, "10.00"); err != nil {
		t.Errorf("retry after failure: %v", err)
	}
	if rep := reconcile(); !rep.Clean() {
		t.Errorf("reconciliation not clean: %+v", rep)
	}
}

func TestValidation(t *testing.T) {
	l, _ := setup(t)
	eur := mustAccount(t, l, "eur", "EUR", true)
	eur2 := mustAccount(t, l, "eur2", "EUR", false)
	usd := mustAccount(t, l, "usd", "USD", false)
	cases := map[string]struct {
		req  ledger.TransferRequest
		want error
	}{
		"same account":   {ledger.TransferRequest{From: eur.ID, To: eur.ID, Amount: "1", Currency: "EUR"}, ledger.ErrInvalid},
		"zero":           {ledger.TransferRequest{From: eur.ID, To: eur2.ID, Amount: "0", Currency: "EUR"}, ledger.ErrInvalid},
		"too precise":    {ledger.TransferRequest{From: eur.ID, To: eur2.ID, Amount: "1.001", Currency: "EUR"}, ledger.ErrInvalid},
		"negative":       {ledger.TransferRequest{From: eur.ID, To: eur2.ID, Amount: "-1", Currency: "EUR"}, ledger.ErrInvalid},
		"unknown":        {ledger.TransferRequest{From: eur.ID, To: "acc_missing", Amount: "1", Currency: "EUR"}, ledger.ErrNotFound},
		"wrong currency": {ledger.TransferRequest{From: eur.ID, To: usd.ID, Amount: "1", Currency: "EUR"}, ledger.ErrCurrencyMismatch},
	}
	for name, c := range cases {
		if _, err := l.Transfer(ctx, "k-"+name, c.req); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
	if _, err := l.Transfer(ctx, "", ledger.TransferRequest{From: eur.ID, To: eur2.ID, Amount: "1", Currency: "EUR"}); !errors.Is(err, ledger.ErrInvalid) {
		t.Errorf("missing key: %v", err)
	}
}

func TestIdempotentReplay(t *testing.T) {
	l, _ := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	alice := mustAccount(t, l, "alice", "EUR", false)

	first, err := transfer(l, "same-key", bank.ID, alice.ID, "25.00")
	if err != nil {
		t.Fatal(err)
	}
	again, err := transfer(l, "same-key", bank.ID, alice.ID, "25.00")
	if err != nil {
		t.Fatal(err)
	}
	if !again.Replayed || again.Transaction.ID != first.Transaction.ID {
		t.Errorf("retry was not replayed: %+v", again)
	}
	if got := balance(t, l, alice.ID); got != 2500 {
		t.Errorf("retry moved money twice: alice = %d", got)
	}
	if _, err := transfer(l, "same-key", bank.ID, alice.ID, "99.00"); !errors.Is(err, ledger.ErrIdempotencyReuse) {
		t.Errorf("different body under same key: %v", err)
	}
}

// Many clients retrying the same request at the same moment must still move
// the money exactly once.
func TestConcurrentRetriesPostOnce(t *testing.T) {
	l, reconcile := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	alice := mustAccount(t, l, "alice", "EUR", false)

	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := transfer(l, "storm", bank.ID, alice.ID, "5.00")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- res.Transaction.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Errorf("%d different transactions for one key", len(seen))
	}
	if got := balance(t, l, alice.ID); got != 500 {
		t.Errorf("alice = %d, want 500", got)
	}
	if rep := reconcile(); !rep.Clean() {
		t.Errorf("reconciliation not clean: %+v", rep)
	}
}

// Random transfers between accounts in both directions at once. Balances must
// never go negative, money must be conserved, and nothing may deadlock.
func TestConcurrentTransfersConserveMoney(t *testing.T) {
	l, reconcile := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	const n, perAccount = 8, 10000 // 8 accounts with 100.00 each
	accts := make([]string, n)
	for i := range accts {
		accts[i] = mustAccount(t, l, fmt.Sprint("user", i), "EUR", false).ID
		if _, err := transfer(l, "seed"+accts[i], bank.ID, accts[i], "100.00"); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, short := 0, 0
	for w := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 1))
			for i := range 60 {
				from, to := r.IntN(n), r.IntN(n)
				if from == to {
					continue
				}
				amt := fmt.Sprintf("%d.%02d", r.IntN(40), r.IntN(100))
				if amt == "0.00" {
					continue
				}
				_, err := transfer(l, fmt.Sprintf("w%d-%d", w, i), accts[from], accts[to], amt)
				mu.Lock()
				switch {
				case err == nil:
					ok++
				case errors.Is(err, ledger.ErrInsufficientFunds):
					short++
				default:
					t.Error(err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	var total int64
	for _, id := range accts {
		b := balance(t, l, id)
		if b < 0 {
			t.Errorf("%s went negative: %d", id, b)
		}
		total += b
	}
	if total != n*perAccount {
		t.Errorf("money not conserved: users hold %d, want %d", total, n*perAccount)
	}
	if rep := reconcile(); !rep.Clean() {
		t.Errorf("reconciliation not clean: %+v", rep)
	}
	t.Logf("%d transfers posted, %d refused for insufficient funds", ok, short)
	if ok == 0 || short == 0 {
		t.Errorf("test exercised too little: %d ok, %d short", ok, short)
	}
}

func TestConversion(t *testing.T) {
	l, reconcile := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	eur := mustAccount(t, l, "alice eur", "EUR", false)
	jpy := mustAccount(t, l, "alice jpy", "JPY", false)
	if _, err := transfer(l, "fund", bank.ID, eur.ID, "100.00"); err != nil {
		t.Fatal(err)
	}
	res, err := l.Convert(ctx, "fx1", ledger.ConversionRequest{
		From: eur.ID, To: jpy.ID, Amount: "40.00", FromCurrency: "EUR", ToCurrency: "JPY", Rate: "162.45",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Transaction.Entries) != 4 {
		t.Fatalf("want 4 entries, got %d", len(res.Transaction.Entries))
	}
	if b := balance(t, l, eur.ID); b != 6000 {
		t.Errorf("eur = %d, want 6000", b)
	}
	if b := balance(t, l, jpy.ID); b != 6498 {
		t.Errorf("jpy = %d, want 6498", b)
	}
	if b := balance(t, l, "sys_fx_eur"); b != 4000 {
		t.Errorf("fx pool eur = %d, want 4000", b)
	}
	if b := balance(t, l, "sys_fx_jpy"); b != -6498 {
		t.Errorf("fx pool jpy = %d, want -6498", b)
	}
	rep := reconcile()
	if !rep.Clean() || rep.CurrencyTotal["JPY"] != 0 || rep.CurrencyTotal["EUR"] != 0 {
		t.Errorf("reconciliation: %+v", rep)
	}
	_, err = l.Convert(ctx, "fx2", ledger.ConversionRequest{
		From: eur.ID, To: jpy.ID, Amount: "1.00", FromCurrency: "EUR", ToCurrency: "USD", Rate: "1.1",
	})
	if !errors.Is(err, ledger.ErrCurrencyMismatch) {
		t.Errorf("wrong to-currency: %v", err)
	}
}

func TestEntriesPagination(t *testing.T) {
	l, _ := setup(t)
	bank := mustAccount(t, l, "bank", "EUR", true)
	alice := mustAccount(t, l, "alice", "EUR", false)
	for i := range 7 {
		if _, err := transfer(l, fmt.Sprint("p", i), bank.ID, alice.ID, "1.00"); err != nil {
			t.Fatal(err)
		}
	}
	var all []ledger.Entry
	var before int64
	for {
		page, err := l.ListEntries(ctx, alice.ID, before, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		before = page[len(page)-1].ID
	}
	if len(all) != 7 {
		t.Fatalf("paged %d entries, want 7", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].ID >= all[i-1].ID {
			t.Fatal("entries not strictly newest first")
		}
	}
}

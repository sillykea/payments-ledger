package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/sillykea/payments-ledger/internal/money"
)

// TransferRequest moves money between two accounts in the same currency.
type TransferRequest struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Amount      string `json:"amount"` // decimal string, e.g. "12.50"
	Currency    string `json:"currency"`
	Description string `json:"description"`
}

// ConversionRequest moves money between accounts in different currencies at
// a quoted rate. It is booked through an FX pool account per currency, so
// each currency still balances on its own.
type ConversionRequest struct {
	From         string `json:"from"`
	To           string `json:"to"`
	Amount       string `json:"amount"` // in the From account's currency
	FromCurrency string `json:"from_currency"`
	ToCurrency   string `json:"to_currency"`
	Rate         string `json:"rate"` // units of ToCurrency per one FromCurrency
	Description  string `json:"description"`
}

// Result is what a posting returns, and what an idempotent retry replays.
type Result struct {
	Status      int
	Transaction Transaction
	Replayed    bool
}

func validDescription(d string) error {
	if len(d) > 500 {
		return fmt.Errorf("%w: description longer than 500 characters", ErrInvalid)
	}
	return nil
}

// Transfer posts a transfer under an idempotency key.
func (l *Ledger) Transfer(ctx context.Context, key string, req TransferRequest) (Result, error) {
	if req.From == "" || req.To == "" || req.From == req.To {
		return Result{}, fmt.Errorf("%w: from and to must be two different accounts", ErrInvalid)
	}
	if err := validDescription(req.Description); err != nil {
		return Result{}, err
	}
	amount, err := money.Parse(req.Amount, req.Currency)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if amount == 0 {
		return Result{}, fmt.Errorf("%w: amount must be greater than zero", ErrInvalid)
	}
	return l.idempotent(ctx, key, "transfer", req, func(tx pgx.Tx) (string, error) {
		locked, err := lockAccounts(ctx, tx, []string{req.From, req.To})
		if err != nil {
			return "", err
		}
		for _, id := range []string{req.From, req.To} {
			if locked[id].currency != req.Currency {
				return "", fmt.Errorf("%w: account %s is %s, request is %s", ErrCurrencyMismatch, id, locked[id].currency, req.Currency)
			}
		}
		return post(ctx, tx, locked, "transfer", req.Description, []leg{
			{req.From, -amount}, {req.To, amount},
		})
	})
}

// fxPool returns the ID of the FX pool account for a currency, creating it if
// needed. Pools may go negative: their balances are the open FX position.
func fxPool(ctx context.Context, tx pgx.Tx, currency string) (string, error) {
	id := "sys_fx_" + strings.ToLower(currency)
	_, err := tx.Exec(ctx, `
		INSERT INTO accounts (id, name, currency, allow_negative) VALUES ($1, $2, $3, true)
		ON CONFLICT (id) DO NOTHING`, id, "FX pool "+currency, currency)
	return id, err
}

// Convert posts a currency conversion under an idempotency key:
//
//	from      -amount  (FromCurrency)
//	FX pool A +amount  (FromCurrency)
//	FX pool B -target  (ToCurrency)
//	to        +target  (ToCurrency)
func (l *Ledger) Convert(ctx context.Context, key string, req ConversionRequest) (Result, error) {
	if req.From == "" || req.To == "" || req.From == req.To {
		return Result{}, fmt.Errorf("%w: from and to must be two different accounts", ErrInvalid)
	}
	if req.FromCurrency == req.ToCurrency {
		return Result{}, fmt.Errorf("%w: use a transfer for same-currency moves", ErrInvalid)
	}
	if err := validDescription(req.Description); err != nil {
		return Result{}, err
	}
	amount, err := money.Parse(req.Amount, req.FromCurrency)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	target, err := money.Convert(amount, req.FromCurrency, req.ToCurrency, req.Rate)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if amount == 0 || target == 0 {
		return Result{}, fmt.Errorf("%w: amount too small to convert", ErrInvalid)
	}
	return l.idempotent(ctx, key, "conversion", req, func(tx pgx.Tx) (string, error) {
		poolFrom, err := fxPool(ctx, tx, req.FromCurrency)
		if err != nil {
			return "", err
		}
		poolTo, err := fxPool(ctx, tx, req.ToCurrency)
		if err != nil {
			return "", err
		}
		locked, err := lockAccounts(ctx, tx, []string{req.From, req.To, poolFrom, poolTo})
		if err != nil {
			return "", err
		}
		if locked[req.From].currency != req.FromCurrency || locked[req.To].currency != req.ToCurrency {
			return "", fmt.Errorf("%w: account currencies don't match the request", ErrCurrencyMismatch)
		}
		return post(ctx, tx, locked, "conversion", req.Description, []leg{
			{req.From, -amount}, {poolFrom, amount},
			{poolTo, -target}, {req.To, target},
		})
	})
}

// event is the payload written to the outbox for every posted transaction.
type event struct {
	EventID     string      `json:"event_id"`
	Type        string      `json:"type"`
	Transaction Transaction `json:"transaction"`
}

func readTransaction(ctx context.Context, tx pgx.Tx, id string) (Transaction, error) {
	var t Transaction
	if err := tx.QueryRow(ctx, `SELECT id, kind, description, created_at FROM transactions WHERE id = $1`, id).
		Scan(&t.ID, &t.Kind, &t.Description, &t.CreatedAt); err != nil {
		return t, err
	}
	rows, err := tx.Query(ctx, `
		SELECT id, transaction_id, account_id, amount_minor, currency, created_at
		FROM entries WHERE transaction_id = $1 ORDER BY id`, id)
	if err != nil {
		return t, err
	}
	t.Entries, err = collectEntries(rows)
	return t, err
}

func writeOutbox(ctx context.Context, tx pgx.Tx, t Transaction) error {
	ev := event{EventID: newID("evt"), Type: "transaction.posted", Transaction: t}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (event_id, topic, payload) VALUES ($1, $2, $3)`,
		ev.EventID, ev.Type, payload)
	return err
}

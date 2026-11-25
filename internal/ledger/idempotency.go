package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5"
)

// idempotent runs a posting at most once per key.
//
// The key row is inserted first, inside the posting's own transaction, with
// a placeholder response. If another request already holds the key:
//   - and it is still in flight, the INSERT waits on the unique index until
//     that transaction commits or rolls back;
//   - and it committed, we replay its stored response (or refuse, if the
//     request body differs);
//   - and it rolled back (e.g. insufficient funds), the key was never stored,
//     so this attempt runs normally.
//
// Failed postings are not remembered: retrying a request that failed for
// lack of funds will succeed once the account is funded. See docs/adr/0003.
func (l *Ledger) idempotent(ctx context.Context, key, kind string, req any, fn func(pgx.Tx) (string, error)) (Result, error) {
	if key == "" || len(key) > 255 {
		return Result{}, fmt.Errorf("%w: Idempotency-Key header must be 1-255 characters", ErrInvalid)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}
	sum := sha256.Sum256(append([]byte(kind+"\n"), body...))
	hash := hex.EncodeToString(sum[:])

	var res Result
	err = l.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (key, request_hash, status_code, response)
			VALUES ($1, $2, 0, 'null') ON CONFLICT (key) DO NOTHING`, key, hash)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return l.replay(ctx, tx, key, hash, &res)
		}

		id, err := fn(tx)
		if err != nil {
			return err
		}
		t, err := readTransaction(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := writeOutbox(ctx, tx, t); err != nil {
			return err
		}
		stored, err := json.Marshal(t)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET status_code = $2, response = $3 WHERE key = $1`,
			key, http.StatusCreated, stored); err != nil {
			return err
		}
		res = Result{Status: http.StatusCreated, Transaction: t}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

func (l *Ledger) replay(ctx context.Context, tx pgx.Tx, key, hash string, res *Result) error {
	var storedHash string
	var status int
	var body []byte
	err := tx.QueryRow(ctx, `SELECT request_hash, status_code, response FROM idempotency_keys WHERE key = $1`, key).
		Scan(&storedHash, &status, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		// The other attempt rolled back between our INSERT and SELECT. Rare;
		// the caller can simply retry.
		return fmt.Errorf("%w: a concurrent request with this key just failed, retry", ErrInvalid)
	}
	if err != nil {
		return err
	}
	if storedHash != hash {
		return ErrIdempotencyReuse
	}
	var t Transaction
	if err := json.Unmarshal(body, &t); err != nil {
		return err
	}
	*res = Result{Status: status, Transaction: t, Replayed: true}
	return nil
}

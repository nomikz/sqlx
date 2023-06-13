package sqlx

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type ctxKey struct{}

func ctxWithTx(ctx context.Context, tx *sqlx.Tx) (context.Context, error) {
	return context.WithValue(ctx, ctxKeyTx{}, tx), nil
}

func txFromCtx(ctx context.Context) (*sqlx.Tx, bool) {
	v := ctx.Value(ctxKeyTx{})

	tx, ok := v.(*sqlx.Tx)
	return tx, ok
}

func DoInTx(ctx context.Context, db *DB, txFunc func(ctx context.Context) error) error {
	txConn, err := db.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("wrapper: db.BeginTxx: %w", err)
	}
	ctx, err = ctxWithTx(ctx, txConn)
	if err != nil {
		return fmt.Errorf("wrapper: injectTx: %w", err)
	}

	defer func() {
		if err := txConn.Rollback(); err != nil {
			err = fmt.Errorf("wrapper: txConn.Rollback: %w", err)
		}
	}()

	err = txFunc(ctx)
	if err != nil {
		return fmt.Errorf("wrapper: txFunx: %w", err)
	}

	err = txConn.Commit()
	if err != nil {
		return fmt.Errorf("wrapper: txConn.Commit: %w", err)
	}

	return nil
}

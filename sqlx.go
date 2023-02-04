package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type DB struct {
	db *sqlx.DB
}

func New(driverName, dsn string) (*DB, error) {
	sqlxDB, err := sqlx.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlx.Open: %w", err)
	}

	return &DB{db: sqlxDB}, nil
}

func (db *DB) QueryRow(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	return db.getConn(ctx).QueryRowxContext(ctx, query, args)
}
func (db *DB) Query(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return db.getConn(ctx).QueryxContext(ctx, query, args)
}
func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.getConn(ctx).ExecContext(ctx, query, args)
}

func (db *DB) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.getConn(ctx).GetContext(ctx, dest, query, args)
}
func (db *DB) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return db.getConn(ctx).SelectContext(ctx, dest, query, args)
}

func (db *DB) NamedExec(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return db.getConn(ctx).NamedExecContext(ctx, query, arg)
}

func (db *DB) Ping(ctx context.Context) error {
	return db.db.PingContext(ctx)
}

type querier interface {
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
}

func (db *DB) getConn(ctx context.Context) querier {
	txConn, ok := txFromCtx(ctx)
	if ok {
		return txConn
	}

	return db.db
}

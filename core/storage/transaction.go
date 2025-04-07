package storage

import "context"

// Transaction provides ACID transaction support
type Transaction interface {
	Commit() error
	Rollback() error
	Writer() Writer
}

// TransactionalStorage extends Storage with transactions
type TransactionalStorage interface {
	Storage
	Begin(ctx context.Context) (Transaction, error)
}

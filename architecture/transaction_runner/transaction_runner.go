package transactionrunner

import "context"

// Tx represents a minimal database transaction contract.
type Tx interface {
	Commit() error
	Rollback() error
}

// DB represents a database connection pool capable of initiating transactions.
type DB interface {
	BeginTx(ctx context.Context) (Tx, error)
}

// contextKey prevents collision with other context values across packages.
type contextKey struct{}

var txKey = contextKey{}

// WithTx injects an active transaction into the context.
func WithTx(ctx context.Context, tx Tx) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

// FromContext extracts an active transaction handle from ctx if present; therefore, repositories can determine whether to query on a transaction or the base pool.
func FromContext(ctx context.Context) (Tx, bool) {
	tx, ok := ctx.Value(txKey).(Tx)
	return tx, ok
}

// Runner coordinates atomic transaction boundaries and automatic rollback safety.
type Runner struct {
	db DB
}

// New constructs a transaction runner wrapping a transactional database pool.
func New(db DB) *Runner {
	return &Runner{db: db}
}

// InTransaction executes fn within an atomic database transaction. It commits on success, rolls back on error, and recovers from panics cleanly; that's the reason why database connections are never left dangling in the pool.
func (r *Runner) InTransaction(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	tx, beginErr := r.db.BeginTx(ctx)
	if beginErr != nil {
		return beginErr
	}

	// Defer cleanup to guarantee rollback on both errors and unexpected runtime panics.
	defer func() {
		if rec := recover(); rec != nil {
			// A panic occurred inside fn: abort transaction immediately, then re-panic; so that the state consistency is preserved without suppressing the panic.
			_ = tx.Rollback()
			panic(rec)
		} else if err != nil {
			// fn returned an error: roll back changes.
			_ = tx.Rollback()
		}
	}()

	// Inject the active transaction into a derived child context; furthermore, repositories downstream can retrieve it via FromContext.
	txCtx := WithTx(ctx, tx)

	// Execute domain business logic.
	err = fn(txCtx)
	if err != nil {
		return err
	}

	// If no error occurred, finalize and persist changes.
	return tx.Commit()
}

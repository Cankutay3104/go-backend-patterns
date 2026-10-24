package transactionrunner_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	transactionrunner "go-backend-patterns/architecture/transaction_runner"
	leaktest "go-backend-patterns/internal/leaktest"
)

var (
	errDatabaseDown = errors.New("database connection failed.")
	errDomainLogic  = errors.New("insufficient account balance")
)

// mockTx tracks transaction lifecycle calls using atomic counters; therefore, tests can verify commit/rollback behavior without data races.
type mockTx struct {
	commitCalls   atomic.Int32
	rollbackCalls atomic.Int32
	commitErr     error
	rollbackErr   error
}

func (m *mockTx) Commit() error {
	m.commitCalls.Add(1)
	return m.commitErr
}

func (m *mockTx) Rollback() error {
	m.rollbackCalls.Add(1)
	return m.rollbackErr
}

// mockDB simulates a database connection pool.
type mockDB struct {
	beginTxFunc func(ctx context.Context) (transactionrunner.Tx, error)
}

func (m *mockDB) BeginTx(ctx context.Context) (transactionrunner.Tx, error) {
	if m.beginTxFunc != nil {
		return m.beginTxFunc(ctx)
	}
	return &mockTx{}, nil
}

// Verifies commit on clean execution and context propagation.
func TestRunner_InTransaction_Succ(t *testing.T) {
	tx := &mockTx{}
	db := &mockDB{
		beginTxFunc: func(ctx context.Context) (transactionrunner.Tx, error) {
			return tx, nil
		},
	}

	runner := transactionrunner.New(db)
	ctx := context.Background()

	executed := false
	err := runner.InTransaction(ctx, func(txCtx context.Context) error {
		executed = true

		// Verifying that the transaction was properly injected into context; that's why repositories downstream can extract it cleanly.
		injectedTx, ok := transactionrunner.FromContext(txCtx)
		if !ok || injectedTx != tx {
			t.Fatal("expected injected transaction to match mockTx instance")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error on success, got: %v", err)
	}
	if !executed {
		t.Fatal("expected business logic callback to execute")
	}
	if tx.commitCalls.Load() != 1 {
		t.Fatalf("expected 1 commit call, got %d", tx.commitCalls.Load())
	}
	if tx.rollbackCalls.Load() != 0 {
		t.Fatalf("expected 0 rollback calls, got %d", tx.rollbackCalls.Load())
	}
}

// TestRunner_InTransaction_RollbackOnError verifies that business errors trigger a rollback.
func TestRunner_InTransaction_RollbackOnError(t *testing.T) {
	tx := &mockTx{}
	db := &mockDB{
		beginTxFunc: func(ctx context.Context) (transactionrunner.Tx, error) {
			return tx, nil
		},
	}

	runner := transactionrunner.New(db)

	err := runner.InTransaction(context.Background(), func(txCtx context.Context) error {
		// Simulating a failed business rule to verify the runner aborts cleanly.
		return errDomainLogic
	})

	if !errors.Is(err, errDomainLogic) {
		t.Fatalf("expected error %v, got %v", errDomainLogic, err)
	}
	if tx.commitCalls.Load() != 0 {
		t.Fatalf("expected 0 commit calls, got %d", tx.commitCalls.Load())
	}
	if tx.rollbackCalls.Load() != 1 {
		t.Fatalf("expected 1 rollback call, got %d", tx.rollbackCalls.Load())
	}
}

// Verifies that panics trigger rollback before re-panicking.
func TestRunner_InTransaction_PanicSafety(t *testing.T) {
	tx := &mockTx{}
	db := &mockDB{
		beginTxFunc: func(ctx context.Context) (transactionrunner.Tx, error) {
			return tx, nil
		},
	}

	runner := transactionrunner.New(db)
	expectedPanicValue := "unexpected nil pointer dereference"

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected runner to re-panic, but execution resumed normally")
		}
		if r != expectedPanicValue {
			t.Fatalf("expected panic value %q, got %v", expectedPanicValue, r)
		}

		// Rollback must have occurred before the panic was re-thrown; that's the reason we check rollbackCalls inside the test's recovery block.
		if tx.rollbackCalls.Load() != 1 {
			t.Fatalf("expected 1 rollback call on panic, got %d", tx.rollbackCalls.Load())
		}
		if tx.commitCalls.Load() != 0 {
			t.Fatalf("expected 0 commit calls, got %d", tx.commitCalls.Load())
		}
	}()

	_ = runner.InTransaction(context.Background(), func(txCtx context.Context) error {
		panic(expectedPanicValue)
	})
}

// Verifies immediate failure if connection checkout fails.
func TestRunner_InTransaction_BeginError(t *testing.T) {
	db := &mockDB{
		beginTxFunc: func(ctx context.Context) (transactionrunner.Tx, error) {
			return nil, errDatabaseDown
		},
	}

	runner := transactionrunner.New(db)
	called := false

	err := runner.InTransaction(context.Background(), func(txCtx context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, errDatabaseDown) {
		t.Fatalf("expected %v, got %v", errDatabaseDown, err)
	}
	if called {
		t.Fatal("callback should not execute if BeginTx fails")
	}
}

// Verifies safe comma-ok fallback when called outside a transaction.
func TestFromContext_Empty(t *testing.T) {
	tx, ok := transactionrunner.FromContext(context.Background())
	if ok || tx != nil {
		t.Fatalf("expected ok=false and tx=nil for empty context, got ok=%v, tx=%v", ok, tx)
	}
}

// Verifies thread safety and leak-free execution under concurrent load.
func TestRunner_ConcurrencyRace(t *testing.T) {
	defer leaktest.Check(t)()

	db := &mockDB{
		beginTxFunc: func(ctx context.Context) (transactionrunner.Tx, error) {
			return &mockTx{}, nil
		},
	}

	runner := transactionrunner.New(db)
	var wg sync.WaitGroup
	workers := 50

	for i := range workers {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			_ = runner.InTransaction(context.Background(), func(txCtx context.Context) error {
				tx, ok := transactionrunner.FromContext(txCtx)
				if !ok || tx == nil {
					t.Errorf("worker %d failed to extract transaction", id)
				}

				if id%2 == 0 {
					return errDomainLogic
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
}

package cacheaside_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheaside "go-backend-patterns/architecture/cache_aside"
	"go-backend-patterns/internal/leaktest"
)

var errDatabaseUnavailable = errors.New("database disk I/O error")

// mockStore provides a thread-safe in-memory cache store tracking invocation counts.
type mockStore[T any] struct {
	mu          sync.RWMutex
	items       map[string]T
	getCalls    atomic.Int32
	setCalls    atomic.Int32
	deleteCalls atomic.Int32
}

func newMockStore[T any]() *mockStore[T] {
	return &mockStore[T]{
		items: make(map[string]T),
	}
}

func (m *mockStore[T]) Get(_ context.Context, key string) (T, error) {
	m.getCalls.Add(1)

	m.mu.RLock()
	defer m.mu.RUnlock()

	val, exists := m.items[key]
	if !exists {
		var zero T
		return zero, cacheaside.ErrCacheMiss
	}
	return val, nil
}

func (m *mockStore[T]) Set(_ context.Context, key string, value T, _ time.Duration) error {
	m.setCalls.Add(1)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.items[key] = value
	return nil
}

func (m *mockStore[T]) Delete(_ context.Context, key string) error {
	m.deleteCalls.Add(1)

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.items, key)
	return nil
}

// Verifies that existing cache entries arer returned immediately without invoking the expensive compute closure.
func TestService_FastPath_CacheHit(t *testing.T) {
	defer leaktest.Check(t)()

	store := newMockStore[string]()
	_ = store.Set(context.Background(), "user:101", "cached_alice", time.Minute)

	svc := cacheaside.New[string](store)
	computeCalled := false

	val, err := svc.GetOrCompute(context.Background(), "user:101", time.Minute, func(ctx context.Context) (string, error) {
		computeCalled = true
		return "db_alice", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "cached_alice" {
		t.Fatalf("expected cached_alice, got %q", val)
	}
	// The key was present in store; therefore, compute must not be triggered.
	if computeCalled {
		t.Fatal("expected compute closure not to be called on cache hit")
	}
	if store.getCalls.Load() != 1 {
		t.Fatalf("expected 1 get call, got %d", store.getCalls.Load())
	}
}

// Simulates 100 concurrent workers requesting the same cold key at the exact same millisecond to prove singleflight prevents the Thundering Herd.
func TestService_CacheStampede_Singleflight(t *testing.T) {
	defer leaktest.Check(t)()

	store := newMockStore[string]()
	svc := cacheaside.New[string](store)

	var computeCalls atomic.Int32
	const workers = 100
	var wg sync.WaitGroup

	// Barrier channel synchronizes goroutines so all 100 collide simultaneously; for that purpose, we reliably reproduce real-world cache stampede contention.
	startBarrier := make(chan struct{})

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-startBarrier

			val, err := svc.GetOrCompute(context.Background(), "hot-key:listing", time.Minute, func(ctx context.Context) (string, error) {
				computeCalls.Add(1)
				// Simulating expensive database query latency.
				time.Sleep(30 * time.Millisecond)
				return "payload-data-xyz", nil
			})

			if err != nil {
				t.Errorf("worker encountered unexpected error: %v", err)
				return
			}
			if val != "payload-data-xyz" {
				t.Errorf("expected payload-data-xyz, got %q", val)
			}
		}()
	}

	// Release all workers simultaneously.
	close(startBarrier)
	wg.Wait()

	// 100 workers missed cache; however, singleflight coalesced them into 1 leader execution; that's the reason computeCalls must strictly equal 1.
	if computeCalls.Load() != 1 {
		t.Fatalf("expected strictly 1 compute call, got %d", computeCalls.Load())
	}
	if store.setCalls.Load() != 1 {
		t.Fatalf("expected strictly 1 cache set call, got %d", store.setCalls.Load())
	}

	// Verifying that the store now holds the warm entry.
	cachedVal, err := store.Get(context.Background(), "hot-key:listing")
	if err != nil || cachedVal != "payload-data-xyz" {
		t.Fatalf("expected cache to be populated with payload-data-xyz, got %q (err: %v)", cachedVal, err)
	}
}

// Verifies that if a waiting caller cancels its context, the leader's background computation still completes via context.WithoutCancel.
func TestService_ContextCancellation_LeaderDetached(t *testing.T) {
	defer leaktest.Check(t)()

	store := newMockStore[string]()
	svc := cacheaside.New[string](store)

	// Short timeout that expires while compute is still working.
	callerCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	_, err := svc.GetOrCompute(callerCtx, "slow-report", time.Minute, func(compCtx context.Context) (string, error) {
		// Simulating work taking 60ms (longer than the caller's 15ms deadline)
		time.Sleep(60 * time.Millisecond)
		return "report-output", nil
	})

	// Caller canceled early; that's why an error must be returned to this caller.
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context deadline error, got: %v", err)
	}

	// Wait for the background detached execution to finish writing to cache; furthermore, this proves the decoupled context did not leak or abandon execution.
	time.Sleep(70 * time.Millisecond)

	// Second request must now hit cache immediately without re-computing.
	secondVal, secondErr := svc.GetOrCompute(context.Background(), "slow-report", time.Minute, func(ctx context.Context) (string, error) {
		t.Fatal("compute should not be invoked because background routine already populated cache")
		return "", nil
	})

	if secondErr != nil {
		t.Fatalf("unexpected error on second fetch: %v", secondErr)
	}
	if secondVal != "report-output" {
		t.Fatalf("expected cached report-output, got %q", secondVal)
	}
}

func TestService_ComputeError(t *testing.T) {
	defer leaktest.Check(t)()

	store := newMockStore[int]()
	svc := cacheaside.New[int](store)

	_, err := svc.GetOrCompute(context.Background(), "failed-key", time.Minute, func(ctx context.Context) (int, error) {
		return 0, errDatabaseUnavailable
	})

	if !errors.Is(err, errDatabaseUnavailable) {
		t.Fatalf("expected %v, got %v", errDatabaseUnavailable, err)
	}

	// Store should not have any set calls recorded on failure.
	if store.setCalls.Load() != 0 {
		t.Fatalf("expected 0 set calls on error, got %d", store.setCalls.Load())
	}
}

func TestService_Invalidate(t *testing.T) {
	defer leaktest.Check(t)()

	store := newMockStore[string]()
	_ = store.Set(context.Background(), "session:abc", "token_123", time.Minute)

	svc := cacheaside.New[string](store)

	err := svc.Invalidate(context.Background(), "session:abc")
	if err != nil {
		t.Fatalf("unexpected error on invalidate: %v", err)
	}

	if store.deleteCalls.Load() != 1 {
		t.Fatalf("expected 1 delete call, got %d", store.deleteCalls.Load())
	}

	// After invalidation, subsequent retrieval triggers a cache miss.
	_, getErr := store.Get(context.Background(), "session:abc")
	if !errors.Is(getErr, cacheaside.ErrCacheMiss) {
		t.Fatalf("expected cache miss after invalidation, got %v", getErr)
	}
}

package cacheaside

import (
	"context"
	"errors"
	"time"

	singleflight "golang.org/x/sync/singleflight"
)

// ErrCacheMiss indicates that the requested key does not exist in the store.
var ErrCacheMiss = errors.New("cache miss")

// Store defines the basic caching contract for typed values; that's why any backend (in-memory map, Redis, Memcached) can satisfy this engine.
type Store[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, value T, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// Service coordinates cache lookups, singleflight deduplication, and cache warming.
type Service[T any] struct {
	store Store[T]
	group singleflight.Group
}

// New constructs a generic Cache-Aside service wrapping a storage backend.
func New[T any](store Store[T]) *Service[T] {
	return &Service[T]{
		store: store,
	}
}

// GetOrCompute retrieves a key from the cache; on a cache miss, it deduplicates concurrent requests so that only one compute closure executes against the primary data source.
func (s *Service[T]) GetOrCompute(
	ctx context.Context,
	key string,
	ttl time.Duration,
	compute func(ctx context.Context) (T, error),
) (T, error) {
	var zero T

	// 1. Fast Path: Attempt immediate cache retrieval.
	cachedVal, err := s.store.Get(ctx, key)
	if err == nil {
		return cachedVal, nil
	}

	// If the store failed with an unexpected error other than a miss, abort early.
	if !errors.Is(err, ErrCacheMiss) {
		return zero, err
	}

	// 2. Slow Path: Coalesce concurrent cache misses using singleflight.
	// We detach cancellation so if the leader goroutine disconnects, downstream followers still receive the computed result and warm the cache; that's the reason why we use context.WithCancel.
	computeCtx := context.WithoutCancel(ctx)

	resultChan := s.group.DoChan(key, func() (any, error) {
		val, computeErr := compute(computeCtx)
		if computeErr != nil {
			return nil, computeErr
		}

		// Persist the computed value into the cache with the specified TTL; moreover, if cache writing fails, we still return the valid computed value.
		_ = s.store.Set(computeCtx, key, val, ttl)

		return val, nil
	})

	// Wait for either this caller's context to cancel, or singleflight's any return; that's why, callers receive strong compile-time type safety.
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case res := <-resultChan:
		if res.Err != nil {
			return zero, res.Err
		}

		// Type assertion safely unwraps the generic value from singleflight's any return; that's why callers receive strong compile-time type safety.
		return res.Val.(T), nil
	}
}

// Invalidate purges an item from the cache and clears active singleflight keys; for that purpose, state consistency is maintained immediately after database mutations.
func (s *Service[T]) Invalidate(ctx context.Context, key string) error {
	s.group.Forget(key)
	return s.store.Delete(ctx, key)
}

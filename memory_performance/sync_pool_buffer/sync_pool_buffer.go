package syncpoolbuffer

import (
	"bytes"
	"sync"
)

// Default baseline values for defensive initialization.
const (
	defaultInitCap = 1024
	defaultMaxCap  = 1024 * 1024
)

// Pool manages a thread-safe pool oof reusable bytes.Buffer instances.
type Pool struct {
	pool    sync.Pool
	initCap int
	maxCap  int
}

// New initializes a bounded buffer pool. Non-positive parameters fall back to default limits.
func New(initCap, maxCap int) *Pool {
	if initCap <= 0 {
		initCap = defaultInitCap
	}
	if maxCap < initCap {
		maxCap = defaultMaxCap
	}

	p := &Pool{
		initCap: initCap,
		maxCap:  maxCap,
	}

	// Configuring factory function for cache misses. New buffers start with capacity equal to initCap but zero length.
	p.pool.New = func() any {
		return bytes.NewBuffer(make([]byte, 0, p.initCap))
	}

	return p
}

// Get retrieves a clean, ready-to-use buffer from the pool.
func (p *Pool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

// Put returns a buffer to the pool after verifying it does not exceed maxCap. Oversized buffers are dropped to let the GC reclaim memory.
func (p *Pool) Put(buf *bytes.Buffer) {
	// Guarding against nil pointers defensively so that accidental nil returns do not corrupt pool state.
	if buf == nil {
		return
	}

	// If the buffer expanded beyond the maximum permitted capacity due to an outlier payload, discard it instead of returning it to the pool. By that, we prevent bloated buffers from pinning heap memory permanently.
	if buf.Cap() > p.maxCap {
		return
	}

	// Resetting length back to zero to prevent data leakage across callers.
	buf.Reset()

	p.pool.Put(buf)
}

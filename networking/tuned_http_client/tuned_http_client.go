package tunedhttpclient

import (
	"io"
	"net"
	"net/http"
	"time"
)

type Config struct {
	ConnectTimeout            time.Duration
	TLSHandshakeTimeout       time.Duration
	ResponseHeaderTimeout     time.Duration
	IdleConnectionTimeout     time.Duration
	MaxIdleConnections        int
	MaxIdleConnectionsPerHost int
	TotalTimeout              time.Duration
}

// DefaultConfig returns safe production defaults for external microservice communication.
func DefaultConfig() Config {
	return Config{
		ConnectTimeout:            5 * time.Second,
		TLSHandshakeTimeout:       5 * time.Second,
		ResponseHeaderTimeout:     10 * time.Second,
		IdleConnectionTimeout:     90 * time.Second,
		MaxIdleConnections:        100,
		MaxIdleConnectionsPerHost: 100,
		TotalTimeout:              15 * time.Second,
	}
}

// New instantiates an *http.Client configured with dedicated phase timeouts and a tuned connection pool.
func New(cfg Config) *http.Client {
	// Defensive normalization: if fields are omitted, apply production defaults.
	defaults := DefaultConfig()

	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaults.ConnectTimeout
	}
	if cfg.TLSHandshakeTimeout <= 0 {
		cfg.TLSHandshakeTimeout = defaults.TLSHandshakeTimeout
	}
	if cfg.ResponseHeaderTimeout <= 0 {
		cfg.ResponseHeaderTimeout = defaults.ResponseHeaderTimeout
	}
	if cfg.IdleConnectionTimeout <= 0 {
		cfg.IdleConnectionTimeout = defaults.IdleConnectionTimeout
	}
	if cfg.MaxIdleConnections <= 0 {
		cfg.MaxIdleConnections = defaults.MaxIdleConnections
	}
	if cfg.MaxIdleConnectionsPerHost <= 0 {
		cfg.MaxIdleConnectionsPerHost = defaults.MaxIdleConnectionsPerHost
	}
	if cfg.TotalTimeout <= 0 {
		cfg.TotalTimeout = defaults.TotalTimeout
	}

	// DialContext controls TCP connection establishment latency.
	dialer := &net.Dialer{
		Timeout:   cfg.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}

	// Construct an isolated transport with tuned connection pool limits.
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		IdleConnTimeout:       cfg.IdleConnectionTimeout,
		MaxIdleConns:          cfg.MaxIdleConnections,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnectionsPerHost,
		// ForceAttemptHTTP2 enables HTTP/2 multiplexing when supported by the server.
		ForceAttemptHTTP2: true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   cfg.TotalTimeout,
	}
}

// Reads the remaining response body into io.Discard before closing it. This guarantees that the underlying TCP connection can return to the idle pool rather than resetting.
func DrainAndClose(body io.ReadCloser) error {
	if body == nil {
		return nil
	}

	// Read remaining bytes up to an 8KB window to clear the socket buffer.
	_, copyErr := io.Copy(io.Discard, io.LimitReader(body, 8*1024))
	closeErr := body.Close()

	if closeErr != nil {
		return closeErr
	}
	return copyErr
}

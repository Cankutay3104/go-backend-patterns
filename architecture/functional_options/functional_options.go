package functionaloptions

import (
	"crypto/tls"
	"errors"
	"time"
)

const (
	defaultReadTimeout  = 5 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultMaxConns     = 1000
)

// Logger defines the minimal logging contract required by Server; therefore, callers can inject log.Default(), Zap, Zerolog, or custom adapters.
type Logger interface {
	Printf(format string, v ...any)
}

// noOpLogger provides an empty logging implementation; for that purpose, internal server routines can execute logging calls safely without checking for nil references or causing runtime panics.
type noOpLogger struct{}

func (n *noOpLogger) Printf(format string, v ...any) {}

// Server represents a configured network service instance. Fields remain unexported to enforce post-construction immutability; that's the reason why external callers cannot mutate configuration during runtime.
type Server struct {
	addr         string
	readTimeout  time.Duration
	writeTimeout time.Duration
	maxConns     int
	tlsConfig    *tls.Config
	logger       Logger
}

// Option defines a functional configuration closure that mutates *Server; moreover, it returns an error if the supplied parameter fails validation.
type Option func(*Server) error

// WithTimeout configures positive read and write deadlines.
func WithTimeout(read, write time.Duration) Option {
	return func(s *Server) error {
		if read <= 0 || write <= 0 {
			return errors.New("timeouts must be strictly positive durations")
		}
		s.readTimeout = read
		s.writeTimeout = write
		return nil
	}
}

// WithMaxConns sets the upper ceiling for concurrent client connections.
func WithMaxConns(n int) Option {
	return func(s *Server) error {
		if n <= 0 {
			return errors.New("max connections must be greater than zero")
		}
		s.maxConns = n
		return nil
	}
}

// WithTLS injects a TLS configuration for encrypted transport; that's why passing an uninitialized nil pointer returns an immediate error.
func WithTLS(tlsConfig *tls.Config) Option {
	return func(s *Server) error {
		if tlsConfig == nil {
			return errors.New("tls configuration cannot be nil")
		}
		s.tlsConfig = tlsConfig
		return nil
	}
}

// WithLogger injects a custom logging implementation.
func WithLogger(logger Logger) Option {
	return func(s *Server) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}
		s.logger = logger
		return nil
	}
}

func New(address string, options ...Option) (*Server, error) {
	if address == "" {
		return nil, errors.New("server address cannot be empty")
	}

	// Initialize safe production defaults first so that a caller who invokes New(":8080") receives a fully functional server.
	s := &Server{
		addr:         address,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		maxConns:     defaultMaxConns,
		logger:       &noOpLogger{},
	}

	for _, opt := range options {
		if err := opt(s); err != nil {
			// Fail fast upon the first invalid configuration; therefore, no partially configured servers enter production.
			return nil, err
		}
	}
	return s, nil
}

// Read-only getters preserving encapsulation.
func (s *Server) Addr() string                { return s.addr }
func (s *Server) ReadTimeout() time.Duration  { return s.readTimeout }
func (s *Server) WriteTimeout() time.Duration { return s.writeTimeout }
func (s *Server) MaxConns() int               { return s.maxConns }
func (s *Server) TLSConfig() *tls.Config      { return s.tlsConfig }
func (s *Server) Logger() Logger              { return s.logger }

package functionaloptions_test

import (
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	functionaloptions "go-backend-patterns/architecture/functional_options"
)

// spyLogger records formatted log messages to verify custom logger injection.
type spyLogger struct {
	messages []string
}

func (s *spyLogger) Printf(format string, v ...any) {
	s.messages = append(s.messages, fmt.Sprintf(format, v...))
}

// Verifies that constructing a server with zero options initializes safe production defaults without nil pointer dereferences.
func TestNew_DefaultConfiguration(t *testing.T) {
	addr := ":8080"
	s, err := functionaloptions.New(addr)
	if err != nil {
		t.Fatalf("expected clean initialization with defaults, got error: %v", err)
	}

	if s.Addr() != addr {
		t.Errorf("expected addr %q, got %q", addr, s.Addr())
	}
	if s.ReadTimeout() != 5*time.Second {
		t.Errorf("expected default read timeout 5s, got %v", s.ReadTimeout())
	}
	if s.WriteTimeout() != 10*time.Second {
		t.Errorf("expected default write timeout 10s, got %v", s.WriteTimeout())
	}
	if s.MaxConns() != 1000 {
		t.Errorf("expected default maxConns 1000, got %d", s.MaxConns())
	}
	if s.TLSConfig() != nil {
		t.Errorf("expected default tlsConfig to be nil, got %v", s.TLSConfig())
	}

	// Logger must fall back to noOpLogger rather than nil; therefore, logging routines can run safely without causing panics.
	if s.Logger() == nil {
		t.Fatal("expected non-nil default noOpLogger")
	}

	// Executing a log call to verify noOpLogger does not panic.
	s.Logger().Printf("test diagnostic event: %d", 42)
}

// Verifies that passing functional options properly overrides defaults.
func TestNew_CustomOptions(t *testing.T) {
	tlsConf := &tls.Config{MinVersion: tls.VersionTLS13}
	logger := &spyLogger{}

	s, err := functionaloptions.New(
		":9090",
		functionaloptions.WithTimeout(15*time.Second, 30*time.Second),
		functionaloptions.WithMaxConns(2500),
		functionaloptions.WithTLS(tlsConf),
		functionaloptions.WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("unexpected error during server construction: %v", err)
	}

	if s.Addr() != ":9090" {
		t.Errorf("expected addr :9090, got %q", s.Addr())
	}
	if s.ReadTimeout() != 15*time.Second {
		t.Errorf("expected read timeout 15s, got %v", s.ReadTimeout())
	}
	if s.WriteTimeout() != 30*time.Second {
		t.Errorf("expected write timeout 30s, got %v", s.WriteTimeout())
	}
	if s.MaxConns() != 2500 {
		t.Errorf("expected maxConns 2500, got %d", s.MaxConns())
	}
	if s.TLSConfig() != tlsConf {
		t.Errorf("expected custom TLS config %v, got %v", tlsConf, s.TLSConfig())
	}

	// Testing cuustom logger invocation to verify the message is captured by our spy.
	s.Logger().Printf("worker connection count: %d", 10)
	if len(logger.messages) != 1 || logger.messages[0] != "worker connection count: 10" {
		t.Fatalf("expected logger to record message, got %v", logger.messages)
	}
}

// Evaluates fail-fast validation behavior across all options.
func TestNew_ValidationFailures(t *testing.T) {
	testCases := []struct {
		name          string
		addr          string
		opts          []functionaloptions.Option
		expectedError string
	}{
		{
			name:          "Empty address",
			addr:          "",
			opts:          nil,
			expectedError: "server address cannot be empty",
		},
		{
			name: "Zero read timeout",
			addr: ":8080",
			opts: []functionaloptions.Option{
				functionaloptions.WithTimeout(0, 10*time.Second),
			},
			expectedError: "timeouts must be strictly positive durations",
		},
		{
			name: "Negative write timeout",
			addr: ":8080",
			opts: []functionaloptions.Option{
				functionaloptions.WithTimeout(5*time.Second, -2*time.Second),
			},
			expectedError: "timeouts must be strictly positive durations",
		},
		{
			name: "Zero max connections",
			addr: ":8080",
			opts: []functionaloptions.Option{
				functionaloptions.WithMaxConns(0),
			},
			expectedError: "max connections must be greater than zero",
		},
		{
			name: "Negative max connections",
			addr: ":8080",
			opts: []functionaloptions.Option{
				functionaloptions.WithMaxConns(-50),
			},
			expectedError: "max connections must be greater than zero",
		},
		{
			name: "Nil TLS configuration",
			addr: ":8080",
			opts: []functionaloptions.Option{
				functionaloptions.WithTLS(nil),
			},
			expectedError: "tls configuration cannot be nil",
		},
		{
			name: "Nil Logger interface",
			addr: ":8080",
			opts: []functionaloptions.Option{
				functionaloptions.WithLogger(nil),
			},
			expectedError: "logger cannot be nil",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := functionaloptions.New(tc.addr, tc.opts...)

			// In case of invalid configuration, constructor must return a nil instance; that's why broken servers never make it into execution.
			if s != nil {
				t.Fatalf("expected server pointer to be nil upon error, got %+v", s)
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if err.Error() != tc.expectedError {
				t.Fatalf("expected error %q, got %q", tc.expectedError, err.Error())
			}
		})
	}
}

// Ensures subsequent options are aborted if an earlier option fails.
func TestNew_FailFast(t *testing.T) {
	appliedSecondOption := false

	sentinelOpt := func(s *functionaloptions.Server) error {
		appliedSecondOption = true
		return nil
	}

	_, err := functionaloptions.New(
		":8080",
		functionaloptions.WithMaxConns(-10), // fails first
		sentinelOpt,                         // should never be reached
	)

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// If the loop continued after the error, appliedSecondOption would be true; that's the reason verifying false proves the constructor fails fast immediately.
	if appliedSecondOption {
		t.Fatal("expected constructor loop to abort immediately upon first error")
	}
}

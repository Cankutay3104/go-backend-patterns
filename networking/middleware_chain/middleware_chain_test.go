package middlewarechain_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	middleware "go-backend-patterns/networking/middleware_chain"
)

// Verifies that middlewares are wrapped recursively in the correct order, ensuring the first provided middleware executes first.
func TestChain_ExecutionOrder(t *testing.T) {
	var executionOrder []string

	// Closure to generate identical middlewares that log their name.
	makeMiddleware := func(name string) middleware.Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executionOrder = append(executionOrder, name)

				next.ServeHTTP(w, r)
			})
		}
	}

	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executionOrder = append(executionOrder, "Final")
	})

	chain := middleware.New(makeMiddleware("A"), makeMiddleware("B"), makeMiddleware("C"))
	wrapped := chain.Wrap(final)

	// Simulate an incoming HTTP GET request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	wrapped.ServeHTTP(rec, req)

	expected := []string{"A", "B", "C", "Final"}
	if !reflect.DeepEqual(executionOrder, expected) {
		t.Fatalf("execution order mismatch\nexpected: %v\ngot: %v", expected, executionOrder)
	}
}

// Verifies that appending to an existing chain creates a distinct memory instance without corrupting the base chain.
func TestChain_AppendImmutability(t *testing.T) {
	var executionOrder []string

	makeMiddleware := func(name string) middleware.Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executionOrder = append(executionOrder, name)

				next.ServeHTTP(w, r)
			})
		}
	}

	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No operation for this test
	})

	baseChain := middleware.New(makeMiddleware("Base"))
	extendedChain := baseChain.Append(makeMiddleware("Extended"))

	// Execute base chain
	baseWrapped := baseChain.Wrap(final)
	baseWrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	// Base chain should only contain name "Base"
	if !reflect.DeepEqual(executionOrder, []string{"Base"}) {
		t.Fatalf("base chain mutated, got: %v", executionOrder)
	}

	// Reset shared state
	executionOrder = nil

	// Execute extended chain
	extendedWrapped := extendedChain.Wrap(final)
	extendedWrapped.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	// Extended chain should contain both
	if !reflect.DeepEqual(executionOrder, []string{"Base", "Extended"}) {
		t.Fatalf("extended chain failed, got: %v", executionOrder)
	}
}

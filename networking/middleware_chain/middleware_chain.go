package middlewarechain

import (
	"net/http"
)

// Middleware defines the standard signature for an HTTP middleware.
// It accepts an exissting handler and returns a new handler that wraps it.
type Middleware func(http.Handler) http.Handler

// Chain is an immutable sequence of HTTP middlewares.
// It is designed to be passed by value to ensure route branching does not mutate shared state.
type Chain struct {
	chain []Middleware
}

// Instantiates a new Chain with the provided middlewares.
func New(middlewares ...Middleware) Chain {
	return Chain{chain: middlewares}
}

// Applies the configured middlewares to the target handler.
// Iterates in backward order so the first middleware in the slice becomes the outermost wrapper, executing first upon an incoming request.
func (c Chain) Wrap(h http.Handler) http.Handler {
	for i := len(c.chain); i > 0; i-- {
		h = c.chain[i-1](h)
	}
	return h
}

// Creates a new Chain by appending additional middlewares to the existing chain.
// Pre-allocates a new array to ensure the original chain is never modified.
func (c Chain) Append(middlewares ...Middleware) Chain {
	totalLength := len(middlewares) + len(c.chain)

	newMidSlice := make([]Middleware, 0, totalLength)

	newMidSlice = append(newMidSlice, c.chain...)
	newMidSlice = append(newMidSlice, middlewares...)

	return Chain{chain: newMidSlice}
}

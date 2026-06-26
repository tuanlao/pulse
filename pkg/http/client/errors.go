package client

import "fmt"

// HTTPError is returned by the JSON helpers when the response status is not 2xx.
// It carries enough context to debug without the caller re-reading the body.
type HTTPError struct {
	Method     string
	URL        string
	StatusCode int
	Status     string
	// Snippet is a bounded prefix of the response body (for diagnostics).
	Snippet string
}

// Error implements error.
func (e *HTTPError) Error() string {
	return fmt.Sprintf("%s %s: unexpected status %d (%s): %s",
		e.Method, e.URL, e.StatusCode, e.Status, e.Snippet)
}

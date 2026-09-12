package marketdata

import (
	"fmt"
)

// LoadError identifies a schema, source, or row validation failure without
// changing the input file. Row is zero-based when a row is available, and -1
// denotes a file-level failure.
type LoadError struct {
	Path   string
	Row    int64
	Column string
	Reason string
	Err    error
}

func (e *LoadError) Error() string {
	location := e.Path
	if e.Row >= 0 {
		location = fmt.Sprintf("%s row %d", location, e.Row)
	}
	if e.Column != "" {
		location += " column " + e.Column
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", location, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s: %s", location, e.Reason)
}

func (e *LoadError) Unwrap() error { return e.Err }

package marketdata

import (
	"errors"
	"io"
	"strings"
	"time"
)

// Load drains one source, validates every decoded row in source order, and
// returns no partial result on failure. Source adapters remain responsible for
// decoding their own formats; this function owns shared market-data rules.
func Load(source MinuteSource, symbol string) (result []SourceMinute, err error) {
	if source == nil {
		return nil, &LoadError{Row: -1, Reason: "source is nil"}
	}
	defer func() {
		closeErr := source.Close()
		if err == nil && closeErr != nil {
			result = nil
			err = &LoadError{Row: -1, Reason: "close source", Err: closeErr}
		}
	}()
	if strings.TrimSpace(symbol) == "" {
		return nil, &LoadError{Row: -1, Reason: "symbol is empty"}
	}

	result = make([]SourceMinute, 0, 256)
	var previous time.Time
	for {
		row, readErr := source.Next()
		if errors.Is(readErr, io.EOF) {
			return result, nil
		}
		if readErr != nil {
			var loadErr *LoadError
			if errors.As(readErr, &loadErr) {
				return nil, readErr
			}
			return nil, &LoadError{Row: int64(len(result)), Reason: "read source rows", Err: readErr}
		}

		minute, validateErr := validateRow(row, symbol, previous)
		if validateErr != nil {
			return nil, validateErr
		}
		result = append(result, minute)
		previous = minute.IntervalStart
	}
}

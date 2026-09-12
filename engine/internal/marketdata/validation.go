package marketdata

import (
	"fmt"
	"math"
	"time"
)

func validateRow(row RawMinute, symbol string, previous time.Time) (SourceMinute, error) {
	missing := func(column string) (SourceMinute, error) {
		return SourceMinute{}, &LoadError{Path: row.SourceFile, Row: row.SourceRow, Column: column, Reason: "null required value"}
	}
	if row.IntervalStart == nil {
		return missing("ts_event")
	}
	if row.Symbol == nil {
		return missing("symbol")
	}
	if row.Open == nil {
		return missing("open")
	}
	if row.High == nil {
		return missing("high")
	}
	if row.Low == nil {
		return missing("low")
	}
	if row.Close == nil {
		return missing("close")
	}
	if row.Volume == nil {
		return missing("volume")
	}

	ts := row.IntervalStart.UTC()
	if ts.UnixNano()%int64(minute) != 0 {
		return SourceMinute{}, &LoadError{Path: row.SourceFile, Row: row.SourceRow, Column: "ts_event", Reason: "timestamp is not minute-aligned"}
	}
	if !previous.IsZero() && !ts.After(previous) {
		return SourceMinute{}, &LoadError{Path: row.SourceFile, Row: row.SourceRow, Column: "ts_event", Reason: "timestamps must be strictly increasing"}
	}

	prices := []struct {
		name  string
		value float64
	}{
		{"open", *row.Open}, {"high", *row.High}, {"low", *row.Low}, {"close", *row.Close},
	}
	for _, price := range prices {
		if math.IsNaN(price.value) || math.IsInf(price.value, 0) || price.value <= 0 {
			return SourceMinute{}, &LoadError{Path: row.SourceFile, Row: row.SourceRow, Column: price.name, Reason: "price must be finite and positive"}
		}
	}
	if *row.Low > *row.Open || *row.Low > *row.Close || *row.High < *row.Open || *row.High < *row.Close || *row.Low > *row.High {
		return SourceMinute{}, &LoadError{Path: row.SourceFile, Row: row.SourceRow, Column: "ohlc", Reason: "low/high bounds are inconsistent"}
	}
	if *row.Symbol != symbol {
		return SourceMinute{}, &LoadError{Path: row.SourceFile, Row: row.SourceRow, Column: "symbol", Reason: fmt.Sprintf("got %q, want %q", *row.Symbol, symbol)}
	}
	return SourceMinute{
		Symbol: *row.Symbol, IntervalStart: ts, IntervalEnd: ts.Add(minute), AvailableAt: ts.Add(minute),
		Open: *row.Open, High: *row.High, Low: *row.Low, Close: *row.Close, Volume: *row.Volume,
		SourceFile: row.SourceFile, SourceRow: row.SourceRow,
	}, nil
}

package marketdata_test

import (
	"backtest_engine/engine/internal/marketdata"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadAcceptsZeroVolumeAndSetsAvailability(t *testing.T) {
	ts := time.Date(2018, 5, 1, 13, 30, 0, 0, time.UTC)
	row := validRawMinute(ts, "AAL")
	zero := uint64(0)
	row.Volume = &zero
	rows, err := marketdata.Load(&sliceSource{rows: []marketdata.RawMinute{row}}, "AAL")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("loaded %d rows, want 1", len(rows))
	}
	got := rows[0]
	if !got.IntervalEnd.Equal(ts.Add(time.Minute)) || !got.AvailableAt.Equal(ts.Add(time.Minute)) {
		t.Fatalf("availability = %v/%v", got.IntervalEnd, got.AvailableAt)
	}
}

func TestLoadRejectsMalformedRows(t *testing.T) {
	ts := time.Date(2018, 5, 1, 13, 30, 0, 0, time.UTC)
	missingTimestamp := validRawMinute(ts, "AAL")
	missingTimestamp.IntervalStart = nil
	nanClose := validRawMinute(ts, "AAL")
	nan := math.NaN()
	nanClose.Close = &nan
	wrongSymbol := validRawMinute(ts, "MSFT")
	tests := []struct {
		name, column string
		row          marketdata.RawMinute
	}{
		{"null timestamp", "ts_event", missingTimestamp},
		{"nan close", "close", nanClose},
		{"wrong symbol", "symbol", wrongSymbol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := marketdata.Load(&sliceSource{rows: []marketdata.RawMinute{tt.row}}, "AAL")
			var loadErr *marketdata.LoadError
			if !errors.As(err, &loadErr) || loadErr.Column != tt.column {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestLoadRejectsDuplicateOrDecreasingTimestamps(t *testing.T) {
	first := time.Date(2018, 5, 1, 13, 30, 0, 0, time.UTC)
	for _, next := range []time.Time{first, first.Add(-time.Minute)} {
		source := &sliceSource{rows: []marketdata.RawMinute{validRawMinute(first, "AAL"), validRawMinute(next, "AAL")}}
		rows, err := marketdata.Load(source, "AAL")
		var loadErr *marketdata.LoadError
		if !errors.As(err, &loadErr) || loadErr.Column != "ts_event" || rows != nil || !source.closed {
			t.Fatalf("rows = %v, error = %v, source closed = %v", rows, err, source.closed)
		}
	}
}

func TestLoadUsesSharedValidationForAnySource(t *testing.T) {
	first := time.Date(2018, 5, 1, 13, 30, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	source := &sliceSource{rows: []marketdata.RawMinute{validRawMinute(first, "AAL"), validRawMinute(second, "AAL")}}
	rows, err := marketdata.Load(source, "AAL")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || !source.closed {
		t.Fatalf("rows = %d, source closed = %v", len(rows), source.closed)
	}
}

func TestOpenParquetMissingPathHasContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.parquet")
	_, err := marketdata.OpenParquet(path)
	var loadErr *marketdata.LoadError
	if !errors.As(err, &loadErr) || loadErr.Path != path || loadErr.Row != -1 {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadAALFixture(t *testing.T) {
	path := os.Getenv("AAL_PARQUET")
	if path == "" {
		path = `C:\Users\torra\Desktop\data\stocks\AAL\2018\05\01\ohlc.parquet`
	}
	if _, err := os.Stat(path); err != nil {
		t.Skipf("recorded AAL fixture is unavailable: %v", err)
	}
	source, err := marketdata.OpenParquet(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := marketdata.Load(source, "AAL")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 404 {
		t.Fatalf("loaded %d rows, want 404", len(rows))
	}
	if rows[0].IntervalStart.Format(time.RFC3339) != "2018-05-01T12:15:00Z" {
		t.Fatalf("first timestamp = %s", rows[0].IntervalStart.Format(time.RFC3339))
	}
	if rows[0].SourceFile != path || rows[0].SourceRow != 0 {
		t.Fatalf("provenance = %s row %d", rows[0].SourceFile, rows[0].SourceRow)
	}
	if !rows[len(rows)-1].IntervalStart.After(rows[0].IntervalStart) {
		t.Fatal("rows are not strictly chronological")
	}
}

type sliceSource struct {
	rows   []marketdata.RawMinute
	index  int
	closed bool
}

func (s *sliceSource) Next() (marketdata.RawMinute, error) {
	if s.index == len(s.rows) {
		return marketdata.RawMinute{}, io.EOF
	}
	row := s.rows[s.index]
	s.index++
	return row, nil
}

func (s *sliceSource) Close() error {
	s.closed = true
	return nil
}

func validRawMinute(ts time.Time, symbol string) marketdata.RawMinute {
	open, high, low, closePrice, volume := 10.0, 11.0, 9.0, 10.5, uint64(1)
	return marketdata.RawMinute{
		Symbol: &symbol, IntervalStart: &ts,
		Open: &open, High: &high, Low: &low, Close: &closePrice, Volume: &volume,
		SourceFile: "sample.source", SourceRow: 0,
	}
}

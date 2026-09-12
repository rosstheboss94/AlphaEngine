package marketdata_test

import (
	"backtest_engine/engine/internal/marketdata"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCanonicalCSV(t *testing.T) {
	path := filepath.Join("testdata", "canonical.csv")
	source, err := marketdata.OpenCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := marketdata.Load(source, "AAL")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("loaded %d rows, want 3", len(rows))
	}
	if rows[0].Volume != 0 || rows[0].SourceRow != 0 || rows[2].SourceRow != 2 {
		t.Fatalf("first/last row = %#v / %#v", rows[0], rows[2])
	}
	if rows[0].SourceFile != path {
		t.Fatalf("source file = %q, want %q", rows[0].SourceFile, path)
	}
}

func TestOpenCSVRejectsNonCanonicalHeader(t *testing.T) {
	path := writeTempCSV(t, "symbol,time,open,high,low,close,volume\nAAL,2018-05-01T13:30:00Z,10,11,9,10.5,1\n")
	_, err := marketdata.OpenCSV(path)
	var loadErr *marketdata.LoadError
	if !errors.As(err, &loadErr) || loadErr.Column != "header" {
		t.Fatalf("error = %v", err)
	}
}

func TestCSVBlankRequiredFieldUsesSharedValidation(t *testing.T) {
	path := writeTempCSV(t, "symbol,ts_event,open,high,low,close,volume\nAAL,2018-05-01T13:30:00Z,10,11,9,,1\n")
	source, err := marketdata.OpenCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = marketdata.Load(source, "AAL")
	var loadErr *marketdata.LoadError
	if !errors.As(err, &loadErr) || loadErr.Column != "close" || loadErr.Row != 0 {
		t.Fatalf("error = %v", err)
	}
}

func TestCSVRejectsNonUTCTimestamp(t *testing.T) {
	path := writeTempCSV(t, "symbol,ts_event,open,high,low,close,volume\nAAL,2018-05-01T09:30:00-04:00,10,11,9,10.5,1\n")
	source, err := marketdata.OpenCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = marketdata.Load(source, "AAL")
	var loadErr *marketdata.LoadError
	if !errors.As(err, &loadErr) || loadErr.Column != "ts_event" || loadErr.Row != 0 {
		t.Fatalf("error = %v", err)
	}
}

func TestCSVRejectsMalformedRecordWithRowContext(t *testing.T) {
	path := writeTempCSV(t, "symbol,ts_event,open,high,low,close,volume\nAAL,2018-05-01T13:30:00Z,10,11,9,10.5\n")
	source, err := marketdata.OpenCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = marketdata.Load(source, "AAL")
	var loadErr *marketdata.LoadError
	if !errors.As(err, &loadErr) || loadErr.Row != 0 || loadErr.Reason != "read csv row" {
		t.Fatalf("error = %v", err)
	}
}

func writeTempCSV(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.csv")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

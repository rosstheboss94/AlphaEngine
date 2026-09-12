package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const candles = `symbol,ts_event,open,high,low,close,volume
AAL,2018-05-01T13:30:00Z,9,10,9,10,1
AAL,2018-05-01T13:31:00Z,10,10,10,10,1
AAL,2018-05-01T13:32:00Z,12,12,11,11,1
AAL,2018-05-01T13:33:00Z,12,12,12,12,1
`

func fixture(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "minutes.CSV")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRun(t *testing.T) {
	path := fixture(t, candles)
	var out, diagnostics bytes.Buffer
	args := []string{"-input", path, "-symbol", "AAL", "-cash", "100", "-buy-target", "50", "-output", "json"}
	if code := execute(args, &out, &diagnostics); code != 0 {
		t.Fatal(diagnostics.String())
	}
	var report struct {
		Account map[string]string
		Orders  []struct{ Signal, Status string }
		Fills   []struct {
			ExecutionTime   string `json:"execution_time"`
			Quantity, Price string
		}
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Account["cash"] != "110" || report.Account["realized"] != "10" || report.Account["quantity"] != "0" {
		t.Fatal(report.Account)
	}
	if len(report.Orders) != 2 || report.Orders[0].Signal != "buy" || report.Orders[1].Signal != "exit" || report.Orders[1].Status != "filled" {
		t.Fatal(report.Orders)
	}
	if len(report.Fills) != 2 || report.Fills[0].ExecutionTime != "2018-05-01T13:32:00Z" || report.Fills[1].ExecutionTime != "2018-05-01T13:34:00Z" {
		t.Fatal(report.Fills)
	}
	if diagnostics.Len() != 0 {
		t.Fatal(diagnostics.String())
	}
	out.Reset()
	if code := execute([]string{"-input", path, "-symbol", "AAL", "-slippage-bps", "100"}, &out, &diagnostics); code != 0 {
		t.Fatal(diagnostics.String())
	}
	if !strings.Contains(out.String(), "Entry price: 0.00") || !strings.Contains(out.String(), "price-only; input adjustment status unknown") {
		t.Fatal(out.String())
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != candles {
		t.Fatal("input changed", err)
	}
}

func TestFailuresAndHelp(t *testing.T) {
	path := fixture(t, candles)
	base := []string{"-input", path, "-symbol", "AAL"}
	cases := [][]string{
		{}, {"-unknown"}, {"-input"}, append(append([]string{}, base...), "extra"),
		{"-input", path, "-symbol", "WRONG"},
		{"-input", filepath.Join(t.TempDir(), "missing.csv"), "-symbol", "AAL"},
		{"-input", filepath.Join(t.TempDir(), "missing.parquet"), "-symbol", "AAL"},
		{"-input", "data.txt", "-symbol", "AAL"},
		append(append([]string{}, base...), "-output", "xml"),
		append(append([]string{}, base...), "-cash", "NaN"),
		append(append([]string{}, base...), "-buy-target", "0"),
		append(append([]string{}, base...), "-slippage-bps", "10000"),
		{"-input", fixture(t, "bad header\n"), "-symbol", "AAL"},
		{"-input", fixture(t, strings.SplitN(candles, "\n", 2)[0]+"\n"), "-symbol", "AAL"},
	}
	for _, args := range cases {
		var out, diag bytes.Buffer
		if code := execute(args, &out, &diag); code != 1 || out.Len() != 0 || diag.Len() == 0 {
			t.Fatalf("%v: code %d output %q diagnostic %q", args, code, out.String(), diag.String())
		}
	}
	for _, arg := range []string{"-h", "-help"} {
		var out, diag bytes.Buffer
		if code := execute([]string{arg}, &out, &diag); code != 0 || out.Len() != 0 || !strings.Contains(diag.String(), "eligible session minutes") {
			t.Fatal(code, diag.String())
		}
	}
}

type brokenOutput struct{ err error }

func (w brokenOutput) Write(p []byte) (int, error) { return 0, w.err }
func TestOutputFailure(t *testing.T) {
	path := fixture(t, candles)
	sentinel := errors.New("output failed")
	for _, format := range []string{"json", "summary"} {
		args := []string{"-input", path, "-symbol", "AAL", "-output", format}
		if err := run(args, brokenOutput{sentinel}, io.Discard); !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		if err := run(args, brokenOutput{}, io.Discard); !errors.Is(err, io.ErrShortWrite) {
			t.Fatal(err)
		}
		if code := execute(args, brokenOutput{sentinel}, io.Discard); code != 1 {
			t.Fatal(code)
		}
	}
}

package results_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"

	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/results"
	"backtest_engine/engine/internal/simulation"
	"backtest_engine/engine/internal/strategy"
)

func run(t *testing.T, exit bool, price float64) *simulation.Result {
	t.Helper()
	start := time.Date(2026, 9, 8, 14, 0, 0, 0, time.UTC)
	var minutes []marketdata.SourceMinute
	var bars []strategy.Bar
	for i, p := range []float64{80, price, 84, 88} {
		s := start.Add(time.Duration(i) * time.Minute)
		e := s.Add(time.Minute)
		minutes = append(minutes, marketdata.SourceMinute{Symbol: "AAL", IntervalStart: s, IntervalEnd: e, AvailableAt: e, Open: p, High: p, Low: p, Close: p})
		bars = append(bars, strategy.Bar{Symbol: "AAL", IntervalStart: s, IntervalEnd: e, AvailableAt: e, Open: p, High: p, Low: p, Close: p})
	}
	calls := 0
	r, err := simulation.Run(simulation.Config{BuyTarget: "808", SlippageBPS: "100"}, minutes, bars, func(strategy.Context) strategy.Signal {
		calls++
		if calls == 1 {
			return strategy.Buy
		}
		if exit && calls == 3 {
			return strategy.Exit
		}
		return strategy.Hold
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReportSimulationAndExactJSON(t *testing.T) {
	for _, exit := range []bool{false, true} {
		for _, price := range []float64{80, 80.1} {
			input := run(t, exit, price)
			report, err := results.Build(input)
			if err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := report.WriteJSON(&out); err != nil {
				t.Fatal(err)
			}
			var decoded struct {
				Status     string
				Account    map[string]string
				Orders     []json.RawMessage
				Fills      []struct{ Quantity, Price, Value string }
				Limitation string
			}
			if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.Status != "success" || decoded.Limitation != simulation.PriceOnlyLimitation {
				t.Fatal(decoded)
			}
			for key, want := range map[string]*big.Rat{"initial_cash": input.InitialCash, "cash": input.Cash, "quantity": input.Quantity, "entry_price": input.EntryPrice, "basis": input.Basis, "mark": input.Mark, "realized": input.Realized, "unrealized": input.Unrealized, "equity": input.Equity} {
				got, ok := new(big.Rat).SetString(decoded.Account[key])
				if !ok || got.Cmp(want) != 0 {
					t.Fatalf("%s: %q != %s", key, decoded.Account[key], want)
				}
			}
			if len(decoded.Orders) != len(input.Orders) || len(decoded.Fills) != len(input.Fills) {
				t.Fatal("ledger length changed")
			}
			for i, f := range decoded.Fills {
				if f.Quantity != input.Fills[i].Quantity.RatString() || f.Price != input.Fills[i].Price.RatString() || f.Value != input.Fills[i].Value.RatString() {
					t.Fatal("fill changed")
				}
			}
			var summary bytes.Buffer
			if err := report.WriteSummary(&summary); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(summary.String(), "Equity: "+input.Equity.FloatString(2)) || !strings.Contains(summary.String(), "Quantity: "+input.Quantity.FloatString(8)) || !strings.Contains(summary.String(), simulation.PriceOnlyLimitation) {
				t.Fatal(summary.String())
			}
			after, _ := json.Marshal(input)
			if !bytes.Equal(before, after) {
				t.Fatal("formatting mutated input")
			}
			input.Cash.SetInt64(99)
			input.Fills[0].Price.SetInt64(99)
			input.Orders[0].Reason = "changed"
			var again bytes.Buffer
			if err := report.WriteJSON(&again); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out.Bytes(), again.Bytes()) {
				t.Fatal("report shares mutable input")
			}
		}
	}
}

func TestRejectInvalidResults(t *testing.T) {
	cases := map[string]func(*simulation.Result){
		"missing":           func(r *simulation.Result) { r.Mark = nil },
		"negative cash":     func(r *simulation.Result) { r.Cash.SetInt64(-1) },
		"negative quantity": func(r *simulation.Result) { r.Quantity.SetInt64(-1) },
		"mark":              func(r *simulation.Result) { r.Mark.SetInt64(0) },
		"basis":             func(r *simulation.Result) { r.Basis.SetInt64(1) },
		"unrealized":        func(r *simulation.Result) { r.Unrealized.SetInt64(1) },
		"equity":            func(r *simulation.Result) { r.Equity.SetInt64(1) },
		"realized":          func(r *simulation.Result) { r.Realized.SetInt64(1) },
		"limitation":        func(r *simulation.Result) { r.Limitation = "adjusted" },
		"fill missing":      func(r *simulation.Result) { r.Fills[0].Quantity = nil },
		"fill value":        func(r *simulation.Result) { r.Fills[0].Value.SetInt64(1) },
		"pending":           func(r *simulation.Result) { r.Orders[0].Status = simulation.Pending },
		"signal":            func(r *simulation.Result) { r.Fills[0].Signal = strategy.Hold },
	}
	if r, err := results.Build(nil); err == nil || r != nil {
		t.Fatal("accepted nil")
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			input := run(t, false, 80)
			mutate(input)
			if r, err := results.Build(input); err == nil || r != nil {
				t.Fatal("accepted invalid result")
			}
		})
	}
}

type failingWriter struct {
	err   error
	calls int
}

func (w *failingWriter) Write(p []byte) (int, error) { w.calls++; return len(p) / 2, w.err }
func TestWriterFailures(t *testing.T) {
	r, err := results.Build(run(t, false, 80))
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("disk failure")
	for _, write := range []func(io.Writer) error{r.WriteJSON, r.WriteSummary} {
		for _, want := range []error{sentinel, io.ErrShortWrite} {
			w := &failingWriter{}
			if want == sentinel {
				w.err = sentinel
			}
			if err := write(w); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
			if w.calls != 1 {
				t.Fatal("unexpected retry")
			}
		}
		if write(nil) == nil {
			t.Fatal("accepted nil writer")
		}
	}
	for _, invalid := range []*results.Report{nil, new(results.Report)} {
		var b bytes.Buffer
		if invalid.WriteJSON(&b) == nil || invalid.WriteSummary(&b) == nil || b.Len() != 0 {
			t.Fatal("invalid report produced output")
		}
		if _, err := invalid.MarshalJSON(); err == nil {
			t.Fatal("invalid report marshaled")
		}
	}
}

func TestEmptyLedgers(t *testing.T) {
	input := run(t, true, 80)
	input.Orders = nil
	input.Fills = nil
	r, err := results.Build(input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte(`"orders":[]`)) || !bytes.Contains(data, []byte(`"fills":[]`)) {
		t.Fatal(string(data))
	}
}

func TestJSONEncodingFailure(t *testing.T) {
	input := run(t, false, 80)
	input.Orders[0].SignalTime = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	report, err := results.Build(input)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if report.WriteJSON(&b) == nil || b.Len() != 0 {
		t.Fatal("encoding failure wrote output")
	}
}

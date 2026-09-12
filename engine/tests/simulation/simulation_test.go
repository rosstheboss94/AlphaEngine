package simulation_test

import (
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/simulation"
	"backtest_engine/engine/internal/strategy"
)

var origin = time.Date(2026, 9, 8, 13, 30, 0, 0, time.UTC)

func minute(offset int, price float64) marketdata.SourceMinute {
	start := origin.Add(time.Duration(offset) * time.Minute)
	return marketdata.SourceMinute{
		Symbol: "AAL", IntervalStart: start, IntervalEnd: start.Add(time.Minute), AvailableAt: start.Add(time.Minute),
		Open: price, High: price, Low: price, Close: price, Volume: 1,
	}
}

func bar(end int, price float64) strategy.Bar {
	m := minute(end-1, price)
	return strategy.Bar{
		Symbol: m.Symbol, IntervalStart: m.IntervalStart, IntervalEnd: m.IntervalEnd, AvailableAt: m.AvailableAt,
		Open: price, High: price, Low: price, Close: price, Volume: 1,
	}
}

func equalRat(t *testing.T, got *big.Rat, want string) {
	t.Helper()
	r, ok := new(big.Rat).SetString(want)
	if !ok || got == nil || got.Cmp(r) != 0 {
		t.Fatalf("got %v, want %s", got, want)
	}
}

func reconcile(t *testing.T, r *simulation.Result) {
	t.Helper()
	expected := new(big.Rat).Add(r.InitialCash, r.Realized)
	expected.Add(expected, r.Unrealized)
	if r.Equity.Cmp(expected) != 0 || r.Cash.Sign() < 0 {
		t.Fatalf("account does not reconcile: %+v", r)
	}
	if r.Limitation != "price-only; input adjustment status unknown" {
		t.Fatalf("missing price-only limitation: %q", r.Limitation)
	}
}

func TestBuyAccounting(t *testing.T) {
	for _, tc := range []struct {
		name, cash, target, bps              string
		price                                float64
		quantity, residual, basis, execution string
	}{
		{"defaults", "", "", "", 80, "12.5", "0", "1000", "80"},
		{"fractional residual", "", "", "", 3, "333.33333333", "0.00000001", "999.99999999", "3"},
		{"limited cash", "250", "", "", 80, "3.125", "0", "250", "80"},
		{"target budget", "", "200", "", 80, "2.5", "800", "200", "80"},
		{"slippage", "", "", "10", 100, "9.99000999", "0.000000001", "999.999999999", "100.1"},
		{"decimal exponent", "1e3", "8.08e2", "1e2", 80, "10", "192", "808", "80.8"},
		{"zero cash", "0", "", "", 80, "0", "0", "0", ""},
		{"below quantity unit", "0.000000001", "", "", 1, "0", "0.000000001", "0", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := simulation.Run(simulation.Config{InitialCash: tc.cash, BuyTarget: tc.target, SlippageBPS: tc.bps},
				[]marketdata.SourceMinute{minute(0, 50), minute(1, tc.price)}, []strategy.Bar{bar(1, 50)},
				func(strategy.Context) strategy.Signal { return strategy.Buy })
			if err != nil {
				t.Fatal(err)
			}
			equalRat(t, r.Quantity, tc.quantity)
			equalRat(t, r.Cash, tc.residual)
			equalRat(t, r.Basis, tc.basis)
			reconcile(t, r)
			if len(r.Orders) != 1 {
				t.Fatalf("orders: %+v", r.Orders)
			}
			if tc.execution == "" {
				if r.Orders[0].Status != simulation.Skipped || r.Orders[0].Reason == "" || len(r.Fills) != 0 {
					t.Fatalf("expected recorded skip: %+v", r)
				}
			} else {
				if len(r.Fills) != 1 || r.Orders[0].Status != simulation.Filled {
					t.Fatalf("missing fill: %+v", r)
				}
				equalRat(t, r.Fills[0].Price, tc.execution)
				equalRat(t, r.EntryPrice, tc.execution)
			}
		})
	}
}

func TestFillTimingAndNoLookahead(t *testing.T) {
	for _, offset := range []int{1, 4, 1440} {
		var previous []strategy.Context
		for _, close := range []float64{104, 108} {
			m := minute(offset, close)
			m.Open, m.Low = 103, 103
			var seen []strategy.Context
			r, err := simulation.Run(simulation.Config{}, []marketdata.SourceMinute{minute(0, 100), m},
				[]strategy.Bar{bar(1, 100)}, func(ctx strategy.Context) strategy.Signal {
					seen = append(seen, ctx)
					return strategy.Buy
				})
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Fills) != 1 || !r.Fills[0].ExecutionTime.Equal(m.IntervalEnd) || !r.Fills[0].SignalTime.Equal(origin.Add(time.Minute)) {
				t.Fatalf("wrong timing: %+v", r.Fills)
			}
			if r.Fills[0].Price.Cmp(new(big.Rat).SetFloat64(close)) != 0 {
				t.Fatal("fill did not use later close")
			}
			if previous != nil && !reflect.DeepEqual(previous, seen) {
				t.Fatal("future close changed earlier callback")
			}
			previous = seen
		}
	}
}

func TestFillBeforeCallbackAndFullExit(t *testing.T) {
	for _, tc := range []struct{ bps, cash, realized string }{
		{"0", "1100", "100"}, {"100", "1063.2", "63.2"},
	} {
		config := simulation.Config{SlippageBPS: tc.bps}
		if tc.bps == "100" {
			config.BuyTarget = "808"
		}
		var states []strategy.Context
		s, err := strategy.NewBuilder().EntryWhen(func(ctx strategy.Context) bool {
			states = append(states, ctx)
			return true
		}).ExitWhen(func(ctx strategy.Context) bool {
			states = append(states, ctx)
			return true
		}).Build()
		if err != nil {
			t.Fatal(err)
		}
		r, err := simulation.Run(config, []marketdata.SourceMinute{minute(0, 70), minute(1, 80), minute(2, 88)},
			[]strategy.Bar{bar(1, 70), bar(2, 80), bar(3, 88)}, s.OnCompletedBar)
		if err != nil {
			t.Fatal(err)
		}
		if len(states) != 3 || states[0].InPosition || !states[1].InPosition || states[2].InPosition {
			t.Fatalf("states: %+v", states)
		}
		if len(r.Fills) != 2 || len(r.Orders) != 3 || r.Orders[2].Status != simulation.Expired {
			t.Fatalf("orders: %+v", r.Orders)
		}
		for i, f := range r.Fills {
			if f.ID != i+1 || f.OrderID != i+1 || !f.ExecutionTime.Equal(origin.Add(time.Duration(i+2)*time.Minute)) {
				t.Fatalf("fill: %+v", f)
			}
		}
		equalRat(t, r.Cash, tc.cash)
		equalRat(t, r.Realized, tc.realized)
		equalRat(t, r.Quantity, "0")
		equalRat(t, r.Basis, "0")
		equalRat(t, r.EntryPrice, "0")
		equalRat(t, r.Unrealized, "0")
		reconcile(t, r)
	}
}

func TestSignalCannotFillAnIntervalAlreadyStarted(t *testing.T) {
	b := bar(2, 80)
	b.IntervalEnd = b.IntervalEnd.Add(-30 * time.Second)
	b.AvailableAt = b.IntervalEnd
	r, err := simulation.Run(simulation.Config{},
		[]marketdata.SourceMinute{minute(0, 80), minute(1, 90), minute(2, 100)}, []strategy.Bar{b},
		func(strategy.Context) strategy.Signal { return strategy.Buy })
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Fills) != 1 || !r.Fills[0].ExecutionTime.Equal(origin.Add(3*time.Minute)) {
		t.Fatalf("signal filled an interval that had already started: %+v", r.Fills)
	}
	equalRat(t, r.Fills[0].Price, "100")
}

func TestCumulativeRealizedAcrossPositions(t *testing.T) {
	prices := []float64{80, 80, 88, 100, 90}
	var minutes []marketdata.SourceMinute
	var bars []strategy.Bar
	for i, price := range prices {
		minutes = append(minutes, minute(i, price))
		if i < len(prices)-1 {
			bars = append(bars, bar(i+1, price))
		}
	}
	r, err := simulation.Run(simulation.Config{}, minutes, bars, func(ctx strategy.Context) strategy.Signal {
		if ctx.InPosition {
			return strategy.Exit
		}
		return strategy.Buy
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Fills) != 4 {
		t.Fatalf("fills: %+v", r.Fills)
	}
	equalRat(t, r.Realized, "0")
	equalRat(t, r.Cash, "1000")
	reconcile(t, r)
}

func TestPendingSignalsAndScheduledGapCallbacks(t *testing.T) {
	minutes := []marketdata.SourceMinute{minute(0, 80), minute(4, 80), minute(8, 88)}
	bars := []strategy.Bar{bar(1, 80), bar(2, 80), bar(3, 80), bar(5, 80), bar(6, 80), bar(7, 80), bar(9, 88)}
	signals := []strategy.Signal{strategy.Buy, strategy.Exit, strategy.Buy, strategy.Exit, strategy.Buy, strategy.Exit, strategy.Hold}
	var states []strategy.Context
	r, err := simulation.Run(simulation.Config{}, minutes, bars, func(ctx strategy.Context) strategy.Signal {
		states = append(states, ctx)
		return signals[len(states)-1]
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != len(bars) || len(r.Orders) != 2 || len(r.Fills) != 2 {
		t.Fatalf("states/orders/fills: %d/%d/%d", len(states), len(r.Orders), len(r.Fills))
	}
	for _, i := range []int{1, 2, 4, 5} {
		if !states[i].Pending {
			t.Fatalf("callback %d did not see pending order", i)
		}
	}
	if !states[3].InPosition || states[3].Pending || states[6].InPosition || states[6].Pending {
		t.Fatalf("fill order wrong: %+v", states)
	}
	equalRat(t, r.Cash, "1100")
	reconcile(t, r)
}

func TestIgnoreFlatExitAndHeldBuy(t *testing.T) {
	signals := []strategy.Signal{strategy.Exit, strategy.Hold, strategy.Buy, strategy.Buy, strategy.Hold}
	var minutes []marketdata.SourceMinute
	var bars []strategy.Bar
	for i := range signals {
		minutes = append(minutes, minute(i, 80))
		bars = append(bars, bar(i+1, 80))
	}
	i := 0
	r, err := simulation.Run(simulation.Config{}, minutes, bars, func(strategy.Context) strategy.Signal { s := signals[i]; i++; return s })
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Orders) != 1 || len(r.Fills) != 1 {
		t.Fatalf("ignored signals created orders: %+v", r.Orders)
	}
	equalRat(t, r.Quantity, "12.5")
	reconcile(t, r)
}

func TestTerminalMarkAndScheduledCompletion(t *testing.T) {
	for _, finalBar := range []int{0, 3, 10} {
		bars := []strategy.Bar{bar(1, 80)}
		if finalBar != 0 {
			bars = append(bars, bar(finalBar, 84))
		}
		calls := 0
		r, err := simulation.Run(simulation.Config{BuyTarget: "808", SlippageBPS: "100"},
			[]marketdata.SourceMinute{minute(0, 80), minute(1, 80), minute(2, 84)}, bars,
			func(ctx strategy.Context) strategy.Signal {
				calls++
				if ctx.InPosition {
					return strategy.Exit
				}
				return strategy.Buy
			})
		if err != nil {
			t.Fatal(err)
		}
		if calls != len(bars) || len(r.Fills) != 1 {
			t.Fatal("incorrect completion or forced liquidation")
		}
		if finalBar != 0 && r.Orders[1].Status != simulation.Expired {
			t.Fatal("terminal exit did not expire")
		}
		equalRat(t, r.Cash, "192")
		equalRat(t, r.Quantity, "10")
		equalRat(t, r.Mark, "84")
		equalRat(t, r.Equity, "1032")
		equalRat(t, r.Unrealized, "32")
		reconcile(t, r)
	}
	r, err := simulation.Run(simulation.Config{}, []marketdata.SourceMinute{minute(0, 84)}, nil,
		func(strategy.Context) strategy.Signal {
			t.Fatal("unexpected callback for absent aggregates")
			return strategy.Hold
		})
	if err != nil {
		t.Fatal(err)
	}
	equalRat(t, r.Equity, "1000")
	reconcile(t, r)
}

func TestSkippedEntryClearsPending(t *testing.T) {
	var seen []strategy.Context
	r, err := simulation.Run(simulation.Config{InitialCash: "0"}, []marketdata.SourceMinute{minute(0, 1), minute(1, 1)},
		[]strategy.Bar{bar(1, 1), bar(2, 1)}, func(ctx strategy.Context) strategy.Signal { seen = append(seen, ctx); return strategy.Buy })
	if err != nil {
		t.Fatal(err)
	}
	if seen[1].Pending || seen[1].InPosition || len(r.Orders) != 2 || r.Orders[0].Status != simulation.Skipped || r.Orders[1].Status != simulation.Expired {
		t.Fatalf("skip transition: %+v", r.Orders)
	}
}

func TestExactBinaryPriceAndFractionalExit(t *testing.T) {
	calls := 0
	r, err := simulation.Run(simulation.Config{InitialCash: "1", SlippageBPS: "0.1"},
		[]marketdata.SourceMinute{minute(0, 1), minute(1, 0.1), minute(2, 0.3)},
		[]strategy.Bar{bar(1, 1), bar(2, 0.1)}, func(strategy.Context) strategy.Signal {
			calls++
			if calls == 1 {
				return strategy.Buy
			}
			return strategy.Exit
		})
	if err != nil {
		t.Fatal(err)
	}
	wantPrice := new(big.Rat).Mul(new(big.Rat).SetFloat64(0.1), big.NewRat(100001, 100000))
	if r.Fills[0].Price.Cmp(wantPrice) != 0 {
		t.Fatal("binary64 price was rounded")
	}
	if r.Fills[0].Quantity.Cmp(r.Fills[1].Quantity) != 0 {
		t.Fatal("exit re-rounded stored quantity")
	}
	if r.Fills[0].Value.Cmp(big.NewRat(1, 1)) > 0 {
		t.Fatal("buy exceeded cash")
	}
	unitCost := new(big.Rat).Quo(wantPrice, big.NewRat(100000000, 1))
	residual := new(big.Rat).Sub(big.NewRat(1, 1), r.Fills[0].Value)
	if residual.Sign() < 0 || residual.Cmp(unitCost) >= 0 {
		t.Fatal("incorrect downward quantization")
	}
	reconcile(t, r)
}

func TestInvalidConfigBeforeCallbacks(t *testing.T) {
	for _, c := range []simulation.Config{
		{InitialCash: "-1"}, {InitialCash: "NaN"}, {InitialCash: "Inf"}, {InitialCash: "1/2"}, {InitialCash: "0x10"},
		{InitialCash: " "}, {BuyTarget: "0"}, {BuyTarget: "-1"}, {BuyTarget: "NaN"},
		{SlippageBPS: "-1"}, {SlippageBPS: "10000"}, {SlippageBPS: "Infinity"}, {SlippageBPS: "1e"},
	} {
		r, err := simulation.Run(c, []marketdata.SourceMinute{minute(0, 1)}, []strategy.Bar{bar(1, 1)},
			func(strategy.Context) strategy.Signal { t.Fatal("callback on invalid config"); return strategy.Hold })
		if err == nil || r != nil {
			t.Fatalf("accepted invalid config %+v", c)
		}
	}
}

func TestInvalidInputsBeforeCallbacks(t *testing.T) {
	for name, mutate := range map[string]func([]marketdata.SourceMinute, []strategy.Bar){
		"wrong symbol": func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].Symbol = "OTHER" },
		"empty symbol": func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[0].Symbol = "" },
		"duplicate":    func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1] = m[0] },
		"unordered":    func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[0], m[1] = m[1], m[0] },
		"off minute": func(m []marketdata.SourceMinute, _ []strategy.Bar) {
			m[1].IntervalStart = m[1].IntervalStart.Add(time.Second)
		},
		"early availability": func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].AvailableAt = m[1].IntervalStart },
		"bad end":            func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].IntervalEnd = m[1].IntervalStart },
		"not UTC": func(m []marketdata.SourceMinute, _ []strategy.Bar) {
			m[1].IntervalStart = m[1].IntervalStart.In(time.FixedZone("offset", 3600))
		},
		"nan":            func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].Close = math.NaN() },
		"infinite":       func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].High = math.Inf(1) },
		"zero price":     func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].Low = 0 },
		"bounds":         func(m []marketdata.SourceMinute, _ []strategy.Bar) { m[1].Close = 100 },
		"bar bounds":     func(_ []marketdata.SourceMinute, b []strategy.Bar) { b[1].Low = 100 },
		"bar symbol":     func(_ []marketdata.SourceMinute, b []strategy.Bar) { b[1].Symbol = "OTHER" },
		"bar incomplete": func(_ []marketdata.SourceMinute, b []strategy.Bar) { b[1].AvailableAt = b[1].IntervalStart },
		"bar duration":   func(_ []marketdata.SourceMinute, b []strategy.Bar) { b[1].IntervalStart = b[1].IntervalEnd },
		"bar overlap":    func(_ []marketdata.SourceMinute, b []strategy.Bar) { b[1].IntervalStart = b[0].IntervalStart },
	} {
		t.Run(name, func(t *testing.T) {
			m := []marketdata.SourceMinute{minute(0, 1), minute(1, 1)}
			b := []strategy.Bar{bar(1, 1), bar(2, 1)}
			mutate(m, b)
			r, err := simulation.Run(simulation.Config{}, m, b, func(strategy.Context) strategy.Signal {
				t.Fatal("callback before full validation")
				return strategy.Hold
			})
			if err == nil || r != nil {
				t.Fatal("accepted invalid input")
			}
		})
	}
	for _, minutes := range [][]marketdata.SourceMinute{nil, {minute(0, 1)}} {
		r, err := simulation.Run(simulation.Config{}, minutes, nil, nil)
		if err == nil || r != nil {
			t.Fatal("nil callback accepted")
		}
	}
	r, err := simulation.Run(simulation.Config{}, nil, nil, func(strategy.Context) strategy.Signal { return strategy.Hold })
	if err == nil || r != nil {
		t.Fatal("empty minutes accepted")
	}
}

func TestStrategyFailureDiscardsResult(t *testing.T) {
	for _, failure := range []func() strategy.Signal{
		func() strategy.Signal { panic("broken rule") },
		func() strategy.Signal { panic(nil) },
		func() strategy.Signal { return strategy.Signal(255) },
	} {
		calls := 0
		r, err := simulation.Run(simulation.Config{}, []marketdata.SourceMinute{minute(0, 1), minute(1, 1), minute(2, 1)},
			[]strategy.Bar{bar(1, 1), bar(2, 1), bar(3, 1)}, func(strategy.Context) strategy.Signal {
				calls++
				if calls == 1 {
					return strategy.Buy
				}
				return failure()
			})
		if err == nil || r != nil || calls != 2 || !strings.Contains(err.Error(), "bar 1") {
			t.Fatalf("failure: result=%v err=%v calls=%d", r, err, calls)
		}
	}
}

func TestInputCopiesAndRepeatIsolation(t *testing.T) {
	run := func(mutate bool) *simulation.Result {
		m := []marketdata.SourceMinute{minute(0, 80), minute(1, 80), minute(2, 88)}
		b := []strategy.Bar{bar(1, 80), bar(2, 80)}
		calls := 0
		r, err := simulation.Run(simulation.Config{}, m, b, func(ctx strategy.Context) strategy.Signal {
			calls++
			if mutate {
				m[1].Close = 1000
				b[1].Close = 1000
			}
			if calls == 2 && ctx.Bar.Close != 80 {
				t.Fatal("callback changed copied bars")
			}
			if ctx.InPosition {
				return strategy.Exit
			}
			return strategy.Buy
		})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	first, second := run(false), run(true)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("identical runs differ")
	}
	first.Cash.SetInt64(-1)
	first.Fills[0].Quantity.SetInt64(999)
	equalRat(t, second.Cash, "1100")
	equalRat(t, second.Fills[0].Quantity, "12.5")
	reconcile(t, second)
}

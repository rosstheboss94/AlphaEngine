package simulation_test

import (
	"fmt"
	"time"

	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/simulation"
	"backtest_engine/engine/internal/strategy"
)

func Example_run() {
	// The caller has selected eligible minutes and completed one-minute bars.
	start := time.Date(2026, 9, 8, 13, 30, 0, 0, time.UTC)
	var minutes []marketdata.SourceMinute
	var bars []strategy.Bar
	for i, close := range []float64{80, 80, 84} {
		from := start.Add(time.Duration(i) * time.Minute)
		end := from.Add(time.Minute)
		minutes = append(minutes, marketdata.SourceMinute{
			Symbol: "AAL", IntervalStart: from, IntervalEnd: end, AvailableAt: end,
			Open: close, High: close, Low: close, Close: close,
		})
		bars = append(bars, strategy.Bar{
			Symbol: "AAL", IntervalStart: from, IntervalEnd: end, AvailableAt: end,
			Open: close, High: close, Low: close, Close: close,
		})
	}
	s, err := strategy.NewBuilder().
		EntryWhen(func(strategy.Context) bool { return true }).
		ExitWhen(func(strategy.Context) bool { return false }).Build()
	if err != nil {
		panic(err)
	}
	r, err := simulation.Run(simulation.Config{}, minutes, bars, s.OnCompletedBar)
	if err != nil {
		panic(err)
	}
	fmt.Println("Shares:", r.Quantity.RatString())
	fmt.Println("Equity:", r.Equity.RatString())
	fmt.Println("Unrealized:", r.Unrealized.RatString())
	fmt.Println(r.Limitation)
	// Output:
	// Shares: 25/2
	// Equity: 1050
	// Unrealized: 50
	// price-only; input adjustment status unknown
}

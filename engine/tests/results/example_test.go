package results_test

import (
	"os"
	"time"

	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/results"
	"backtest_engine/engine/internal/simulation"
	"backtest_engine/engine/internal/strategy"
)

func Example_writeSummary() {
	start := time.Date(2026, 9, 8, 14, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	input, err := simulation.Run(simulation.Config{}, []marketdata.SourceMinute{{
		Symbol: "AAL", IntervalStart: start, IntervalEnd: end, AvailableAt: end,
		Open: 80, High: 80, Low: 80, Close: 80,
	}}, nil, func(strategy.Context) strategy.Signal { return strategy.Hold })
	if err != nil {
		panic(err)
	}
	report, err := results.Build(input)
	if err != nil {
		panic(err)
	}
	if err := report.WriteSummary(os.Stdout); err != nil {
		panic(err)
	}
	// Output:
	// Backtest result: success
	// Final account, rounded display values
	// Initial cash: 1000.00
	// Cash: 1000.00
	// Quantity: 0.00000000
	// Entry price: 0.00
	// Entry basis: 0.00
	// Final mark: 80.00
	// Holdings value: 0.00
	// Realized P&L: 0.00
	// Unrealized P&L: 0.00
	// Equity: 1000.00
	// Orders: 0
	// Fills: 0
	// price-only; input adjustment status unknown
}

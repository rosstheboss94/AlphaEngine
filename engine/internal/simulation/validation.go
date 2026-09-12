package simulation

import (
	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/strategy"
	"fmt"
	"math"
	"strings"
	"time"
)

func validateInputs(minutes []marketdata.SourceMinute, bars []strategy.Bar) error {
	if len(minutes) == 0 {
		return fmt.Errorf("simulation: no eligible source minutes")
	}
	symbol := minutes[0].Symbol
	if strings.TrimSpace(symbol) == "" {
		return fmt.Errorf("simulation: symbol is empty")
	}
	for i, m := range minutes {
		_, offset := m.IntervalStart.Zone()
		if m.Symbol != symbol || m.IntervalStart.IsZero() || offset != 0 || !m.IntervalStart.Equal(m.IntervalStart.Truncate(time.Minute)) ||
			!m.IntervalEnd.Equal(m.IntervalStart.Add(time.Minute)) || !m.AvailableAt.Equal(m.IntervalEnd) ||
			(i > 0 && !m.IntervalStart.After(minutes[i-1].IntervalStart)) || !validOHLC(m.Open, m.High, m.Low, m.Close) {
			return fmt.Errorf("simulation: invalid source minute %d, file %q row %d at %s: check symbol, UTC minute timestamps, ordering and OHLC", i, m.SourceFile, m.SourceRow, m.IntervalStart.Format(time.RFC3339Nano))
		}
	}
	for i, b := range bars {
		if b.Symbol != symbol || b.IntervalStart.IsZero() || !b.IntervalEnd.After(b.IntervalStart) || !b.AvailableAt.Equal(b.IntervalEnd) ||
			(i > 0 && b.IntervalStart.Before(bars[i-1].IntervalEnd)) || !validOHLC(b.Open, b.High, b.Low, b.Close) {
			return fmt.Errorf("simulation: invalid strategy bar %d at %s: check symbol, completed timestamps, ordering and OHLC", i, b.AvailableAt.Format(time.RFC3339Nano))
		}
	}
	return nil
}

func validOHLC(open, high, low, close float64) bool {
	for _, v := range []float64{open, high, low, close} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
			return false
		}
	}
	return low <= open && open <= high && low <= close && close <= high
}

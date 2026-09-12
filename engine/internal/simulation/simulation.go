package simulation

import (
	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/strategy"
	"fmt"
	"math/big"
	"time"
)

// Run merges source closes and scheduled bar completions. A same-time fill
// precedes the callback; a new signal needs a subsequent source interval.
// Inputs are copied and validated before callbacks. The caller guarantees
// session eligibility and aggregate completeness, and supplies fresh callback
// state for each run. Callers must not mutate input slices during the copy.
// Failures return nil and an error. Strategy panics are caught without retry.
func Run(config Config, minutes []marketdata.SourceMinute, bars []strategy.Bar, onBar func(strategy.Context) strategy.Signal) (*Result, error) {
	cash, target, bps, err := parseConfig(config)
	if err != nil {
		return nil, err
	}
	if onBar == nil {
		return nil, fmt.Errorf("simulation: callback is nil")
	}
	minutes = append([]marketdata.SourceMinute(nil), minutes...)
	bars = append([]strategy.Bar(nil), bars...)
	if err := validateInputs(minutes, bars); err != nil {
		return nil, err
	}
	r := &Result{
		InitialCash: new(big.Rat).Set(cash), Cash: cash,
		Quantity: new(big.Rat), EntryPrice: new(big.Rat), Basis: new(big.Rat),
		Realized: new(big.Rat), Limitation: PriceOnlyLimitation,
	}
	buyFactor := new(big.Rat).Add(big.NewRat(1, 1), new(big.Rat).Quo(bps, big.NewRat(10000, 1)))
	sellFactor := new(big.Rat).Sub(big.NewRat(1, 1), new(big.Rat).Quo(bps, big.NewRat(10000, 1)))
	pending := -1
	for mi, bi := 0, 0; mi < len(minutes) || bi < len(bars); {
		if mi < len(minutes) && (bi == len(bars) || !minutes[mi].IntervalEnd.After(bars[bi].AvailableAt)) {
			m := minutes[mi]
			if pending >= 0 && !m.IntervalStart.Before(r.Orders[pending].SignalTime) {
				r.fill(pending, m, target, buyFactor, sellFactor)
				pending = -1
			}
			mi++
			continue
		}
		bar := bars[bi]
		signal, err := callStrategy(onBar, strategy.Context{
			Bar: bar, InPosition: r.Quantity.Sign() > 0, Pending: pending >= 0,
		})
		if err != nil {
			return nil, fmt.Errorf("simulation: bar %d at %s: %w", bi, bar.AvailableAt.Format(time.RFC3339Nano), err)
		}
		if signal > strategy.Exit {
			return nil, fmt.Errorf("simulation: bar %d: invalid signal %d", bi, signal)
		}
		if pending < 0 && ((signal == strategy.Buy && r.Quantity.Sign() == 0) || (signal == strategy.Exit && r.Quantity.Sign() > 0)) {
			r.Orders = append(r.Orders, Order{ID: len(r.Orders) + 1, Signal: signal, SignalTime: bar.AvailableAt, Status: Pending})
			pending = len(r.Orders) - 1
		}
		bi++
	}
	if pending >= 0 {
		r.Orders[pending].Status = Expired
		r.Orders[pending].Reason = "no subsequent eligible source minute"
	}
	r.Mark = new(big.Rat).SetFloat64(minutes[len(minutes)-1].Close)
	holdings := new(big.Rat).Mul(r.Quantity, r.Mark)
	r.Unrealized = new(big.Rat).Sub(holdings, r.Basis)
	r.Equity = new(big.Rat).Add(r.Cash, holdings)
	return r, nil
}

func callStrategy(callback func(strategy.Context) strategy.Signal, ctx strategy.Context) (signal strategy.Signal, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("strategy panic: %v", value)
		}
	}()
	return callback(ctx), nil
}

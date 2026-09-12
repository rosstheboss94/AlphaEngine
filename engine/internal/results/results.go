package results

import (
	"backtest_engine/engine/internal/simulation"
	"backtest_engine/engine/internal/strategy"
	"fmt"
	"math/big"
)

// Report is a private snapshot. Its zero value is not a successful report.
// Completed reports support concurrent reads with independently owned writers.
type Report struct{ data *document }

// Build copies a successful simulation result after checking its accounting.
// The caller must not mutate input during this call.
func Build(input *simulation.Result) (*Report, error) {
	if input == nil {
		return nil, fmt.Errorf("results: nil simulation result")
	}
	values := []*big.Rat{input.InitialCash, input.Cash, input.Quantity, input.EntryPrice, input.Basis, input.Mark, input.Realized, input.Unrealized, input.Equity}
	for _, v := range values {
		if v == nil {
			return nil, fmt.Errorf("results: missing account value")
		}
	}
	if input.InitialCash.Sign() < 0 || input.Cash.Sign() < 0 || input.Quantity.Sign() < 0 || input.EntryPrice.Sign() < 0 || input.Basis.Sign() < 0 || input.Mark.Sign() <= 0 {
		return nil, fmt.Errorf("results: invalid account value")
	}
	if (input.Quantity.Sign() == 0 && input.EntryPrice.Sign() != 0) || (input.Quantity.Sign() > 0 && input.EntryPrice.Sign() <= 0) {
		return nil, fmt.Errorf("results: invalid entry price")
	}
	holdings := new(big.Rat).Mul(input.Quantity, input.Mark)
	checks := []struct {
		name      string
		got, want *big.Rat
	}{
		{"basis", input.Basis, new(big.Rat).Mul(input.Quantity, input.EntryPrice)},
		{"unrealized", input.Unrealized, new(big.Rat).Sub(holdings, input.Basis)},
		{"equity", input.Equity, new(big.Rat).Add(input.Cash, holdings)},
		{"P&L", input.Equity, new(big.Rat).Add(input.InitialCash, new(big.Rat).Add(input.Realized, input.Unrealized))},
	}
	for _, c := range checks {
		if c.got.Cmp(c.want) != 0 {
			return nil, fmt.Errorf("results: %s does not reconcile", c.name)
		}
	}
	if input.Limitation != simulation.PriceOnlyLimitation {
		return nil, fmt.Errorf("results: missing or invalid price-only limitation")
	}
	d := &document{Status: "success", Limitation: input.Limitation, Orders: make([]order, 0, len(input.Orders)), Fills: make([]fill, 0, len(input.Fills))}
	d.Account = account{input.InitialCash.RatString(), input.Cash.RatString(), input.Quantity.RatString(), input.EntryPrice.RatString(), input.Basis.RatString(), input.Mark.RatString(), holdings.RatString(), input.Realized.RatString(), input.Unrealized.RatString(), input.Equity.RatString()}
	for _, o := range input.Orders {
		s, err := signalName(o.Signal)
		if err != nil {
			return nil, err
		}
		if o.Status != simulation.Filled && o.Status != simulation.Skipped && o.Status != simulation.Expired {
			return nil, fmt.Errorf("results: order %d is not final", o.ID)
		}
		d.Orders = append(d.Orders, order{o.ID, s, o.SignalTime, o.Status, o.Reason, o.FillID})
	}
	for _, f := range input.Fills {
		s, err := signalName(f.Signal)
		if err != nil {
			return nil, err
		}
		if f.Quantity == nil || f.Price == nil || f.Value == nil || f.Quantity.Sign() <= 0 || f.Price.Sign() <= 0 || f.Value.Cmp(new(big.Rat).Mul(f.Quantity, f.Price)) != 0 {
			return nil, fmt.Errorf("results: invalid fill %d", f.ID)
		}
		d.Fills = append(d.Fills, fill{f.ID, f.OrderID, s, f.SignalTime, f.ExecutionTime, f.Quantity.RatString(), f.Price.RatString(), f.Value.RatString()})
	}
	return &Report{data: d}, nil
}

func signalName(s strategy.Signal) (string, error) {
	switch s {
	case strategy.Buy:
		return "buy", nil
	case strategy.Exit:
		return "exit", nil
	}
	return "", fmt.Errorf("results: invalid ledger signal %d", s)
}

package simulation

import (
	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/strategy"
	"math/big"
)

func (r *Result) fill(index int, minute marketdata.SourceMinute, target, buyFactor, sellFactor *big.Rat) {
	order := &r.Orders[index]
	price := new(big.Rat).SetFloat64(minute.Close)
	quantity := new(big.Rat).Set(r.Quantity)
	if order.Signal == strategy.Buy {
		price.Mul(price, buyFactor)
		budget := target
		if r.Cash.Cmp(budget) < 0 {
			budget = r.Cash
		}
		scaled := new(big.Rat).Quo(budget, price)
		scaled.Mul(scaled, big.NewRat(100000000, 1))
		units := new(big.Int).Quo(scaled.Num(), scaled.Denom())
		quantity.SetFrac(units, big.NewInt(100000000))
		if quantity.Sign() == 0 {
			order.Status = Skipped
			order.Reason = "buy quantity rounds down to zero"
			return
		}
	} else {
		price.Mul(price, sellFactor)
	}
	value := new(big.Rat).Mul(quantity, price)
	if order.Signal == strategy.Buy {
		r.Cash.Sub(r.Cash, value)
		r.Quantity.Set(quantity)
		r.EntryPrice.Set(price)
		r.Basis.Set(value)
	} else {
		r.Cash.Add(r.Cash, value)
		r.Realized.Add(r.Realized, new(big.Rat).Sub(value, r.Basis))
		r.Quantity.SetInt64(0)
		r.EntryPrice.SetInt64(0)
		r.Basis.SetInt64(0)
	}
	order.Status = Filled
	order.FillID = len(r.Fills) + 1
	r.Fills = append(r.Fills, Fill{
		ID: order.FillID, OrderID: order.ID, Signal: order.Signal,
		SignalTime: order.SignalTime, ExecutionTime: minute.IntervalEnd,
		Quantity: quantity, Price: price, Value: value,
	})
}

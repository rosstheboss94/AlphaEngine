package simulation

import (
	"backtest_engine/engine/internal/strategy"
	"math/big"
	"time"
)

type OrderStatus string

const (
	Pending OrderStatus = "pending"
	Filled  OrderStatus = "filled"
	Skipped OrderStatus = "skipped"
	Expired OrderStatus = "expired"
)

// Order records an accepted signal and its outcome. Ignored signals create no order.
type Order struct {
	ID         int
	Signal     strategy.Signal
	SignalTime time.Time
	Status     OrderStatus
	Reason     string
	FillID     int
}

type Fill struct {
	ID            int
	OrderID       int
	Signal        strategy.Signal
	SignalTime    time.Time
	ExecutionTime time.Time
	Quantity      *big.Rat
	Price         *big.Rat
	Value         *big.Rat
}

// Result owns its rational values. Mutating them cannot affect another run.
// EntryPrice and Basis are zero when flat. Mark never includes slippage.
type Result struct {
	InitialCash *big.Rat
	Cash        *big.Rat
	Quantity    *big.Rat
	EntryPrice  *big.Rat
	Basis       *big.Rat
	Mark        *big.Rat
	Realized    *big.Rat
	Unrealized  *big.Rat
	Equity      *big.Rat
	Limitation  string
	Orders      []Order
	Fills       []Fill
}

const PriceOnlyLimitation = "price-only; input adjustment status unknown"

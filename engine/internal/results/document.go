package results

import (
	"backtest_engine/engine/internal/simulation"
	"time"
)

type account struct {
	InitialCash string `json:"initial_cash"`
	Cash        string `json:"cash"`
	Quantity    string `json:"quantity"`
	EntryPrice  string `json:"entry_price"`
	Basis       string `json:"basis"`
	Mark        string `json:"mark"`
	Holdings    string `json:"holdings"`
	Realized    string `json:"realized"`
	Unrealized  string `json:"unrealized"`
	Equity      string `json:"equity"`
}
type order struct {
	ID         int                    `json:"id"`
	Signal     string                 `json:"signal"`
	SignalTime time.Time              `json:"signal_time"`
	Status     simulation.OrderStatus `json:"status"`
	Reason     string                 `json:"reason,omitempty"`
	FillID     int                    `json:"fill_id,omitempty"`
}
type fill struct {
	ID            int       `json:"id"`
	OrderID       int       `json:"order_id"`
	Signal        string    `json:"signal"`
	SignalTime    time.Time `json:"signal_time"`
	ExecutionTime time.Time `json:"execution_time"`
	Quantity      string    `json:"quantity"`
	Price         string    `json:"price"`
	Value         string    `json:"value"`
}
type document struct {
	Status     string  `json:"status"`
	Account    account `json:"account"`
	Orders     []order `json:"orders"`
	Fills      []fill  `json:"fills"`
	Limitation string  `json:"limitation"`
}

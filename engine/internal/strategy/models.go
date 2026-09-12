package strategy

import (
	"time"
)

// Signal requests an action. The simulation owns orders, sizing, and fills.
type Signal uint8

const (
	Hold Signal = iota
	Buy
	Exit
)

// Bar contains a completed aggregate. The caller must supply only information
// available at AvailableAt, after applying any fills at that time.
type Bar struct {
	Symbol        string
	IntervalStart time.Time
	IntervalEnd   time.Time
	AvailableAt   time.Time
	Open          float64
	High          float64
	Low           float64
	Close         float64
	Volume        uint64
}

// Context is a value snapshot. InPosition means the account holds shares;
// Pending means an entry or exit order is waiting to fill.
type Context struct {
	Bar        Bar
	InPosition bool
	Pending    bool
}

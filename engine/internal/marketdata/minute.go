package marketdata

import (
	"time"
)

const minute = time.Minute

// SourceMinute is one validated source observation. IntervalStart is the
// UTC minute-start timestamp from the source; the complete row is available
// at IntervalEnd. SourceFile and SourceRow identify the source location.
type SourceMinute struct {
	Symbol        string
	IntervalStart time.Time
	IntervalEnd   time.Time
	AvailableAt   time.Time
	Open          float64
	High          float64
	Low           float64
	Close         float64
	Volume        uint64
	SourceFile    string
	SourceRow     int64
}

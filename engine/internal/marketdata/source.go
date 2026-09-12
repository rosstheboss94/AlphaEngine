package marketdata

import "time"

// RawMinute is the normalized candidate emitted by a source adapter. Pointer
// fields preserve null-vs-zero so the shared loader can apply one validation
// policy to every source.
type RawMinute struct {
	Symbol        *string
	IntervalStart *time.Time
	Open          *float64
	High          *float64
	Low           *float64
	Close         *float64
	Volume        *uint64
	SourceFile    string
	SourceRow     int64
}

// MinuteSource streams normalized candidate rows and owns the source reader's
// lifecycle. Adapters are responsible for decoding and source-schema checks;
// Load applies common market-data validation after decoding.
type MinuteSource interface {
	Next() (RawMinute, error)
	Close() error
}

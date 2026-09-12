package results

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
)

// MarshalJSON preserves exact account and fill values as rational strings.
func (r *Report) MarshalJSON() ([]byte, error) {
	if r == nil || r.data == nil {
		return nil, fmt.Errorf("results: invalid report")
	}
	return json.Marshal(r.data)
}

// WriteJSON writes one JSON document followed by a newline. It does not close w.
func (r *Report) WriteJSON(w io.Writer) error {
	data, err := r.MarshalJSON()
	if err != nil {
		return err
	}
	return write(w, append(data, '\n'))
}

// WriteSummary rounds monetary values to two places and quantity to eight.
// It does not close w or change the exact report values.
func (r *Report) WriteSummary(w io.Writer) error {
	if r == nil || r.data == nil {
		return fmt.Errorf("results: invalid report")
	}
	var b strings.Builder
	b.WriteString("Backtest result: success\nFinal account, rounded display values\n")
	a := r.data.Account
	for _, field := range []struct {
		name, value string
		places      int
	}{
		{"Initial cash", a.InitialCash, 2}, {"Cash", a.Cash, 2}, {"Quantity", a.Quantity, 8},
		{"Entry price", a.EntryPrice, 2}, {"Entry basis", a.Basis, 2}, {"Final mark", a.Mark, 2},
		{"Holdings value", a.Holdings, 2}, {"Realized P&L", a.Realized, 2}, {"Unrealized P&L", a.Unrealized, 2}, {"Equity", a.Equity, 2},
	} {
		v, _ := new(big.Rat).SetString(field.value)
		fmt.Fprintf(&b, "%s: %s\n", field.name, v.FloatString(field.places))
	}
	fmt.Fprintf(&b, "Orders: %d\nFills: %d\n%s\n", len(r.data.Orders), len(r.data.Fills), r.data.Limitation)
	return write(w, []byte(b.String()))
}

func write(w io.Writer, data []byte) error {
	if w == nil {
		return fmt.Errorf("results: nil writer")
	}
	n, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("results: write: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("results: write: %w", io.ErrShortWrite)
	}
	return nil
}

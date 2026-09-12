// Package simulation executes a long-only backtest over eligible source minutes
// and completed strategy bars. Callers own session selection, aggregation and
// fresh strategy state. Run owns event ordering, orders and exact accounting.
package simulation

package simulation

import (
	"fmt"
	"math/big"
	"regexp"
)

// Config parses decimal values exactly. Empty fields use the documented defaults.
type Config struct {
	InitialCash string
	BuyTarget   string
	SlippageBPS string
}

var decimalPattern = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

func parseConfig(c Config) (cash, target, bps *big.Rat, err error) {
	values := []*big.Rat{nil, nil, nil}
	for i, field := range []struct{ name, text, fallback string }{
		{"initial cash", c.InitialCash, "1000"},
		{"buy target", c.BuyTarget, "1000"},
		{"slippage bps", c.SlippageBPS, "0"},
	} {
		value := field.text
		if value == "" {
			value = field.fallback
		}
		if !decimalPattern.MatchString(value) {
			return nil, nil, nil, fmt.Errorf("simulation: %s must be a finite decimal", field.name)
		}
		parsed, ok := new(big.Rat).SetString(value)
		if !ok {
			return nil, nil, nil, fmt.Errorf("simulation: invalid %s", field.name)
		}
		values[i] = parsed
	}
	cash, target, bps = values[0], values[1], values[2]
	if cash.Sign() < 0 || target.Sign() <= 0 || bps.Sign() < 0 || bps.Cmp(big.NewRat(10000, 1)) >= 0 {
		return nil, nil, nil, fmt.Errorf("simulation: require initial cash >= 0, buy target > 0 and 0 <= slippage bps < 10000")
	}
	return cash, target, bps, nil
}

package strategy_test

import (
	"backtest_engine/engine/internal/strategy"
	"fmt"
)

func Example_builder() {
	rising := strategy.Rule(func(ctx strategy.Context) bool { return ctx.Bar.Close > ctx.Bar.Open })
	liquid := strategy.Rule(func(ctx strategy.Context) bool { return ctx.Bar.Volume >= 1000 })
	s, err := strategy.NewBuilder().
		EntryWhen(strategy.All(rising, liquid)).
		ExitWhen(strategy.Not(rising)).
		Build()
	if err != nil {
		panic(err)
	}
	bar := strategy.Bar{Open: 10, Close: 11, Volume: 2000}
	fmt.Println(s.OnCompletedBar(strategy.Context{Bar: bar}) == strategy.Buy)
	bar.Close = 9
	fmt.Println(s.OnCompletedBar(strategy.Context{Bar: bar, InPosition: true}) == strategy.Exit)
	// Output:
	// true
	// true
}

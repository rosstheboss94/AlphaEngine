package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"backtest_engine/engine/internal/marketdata"
	"backtest_engine/engine/internal/results"
	"backtest_engine/engine/internal/simulation"
	"backtest_engine/engine/internal/strategy"
)

func main() { os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr)) }

func execute(args []string, stdout, stderr io.Writer) int {
	if err := run(args, stdout, stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "backtest-engine: %v\n", err)
		return 1
	}
	return 0
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("backtest-engine", flag.ContinueOnError)
	// execute owns diagnostics; usage is printed explicitly for help.
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "one prefiltered .csv or .parquet file, required")
	symbol := flags.String("symbol", "", "input stock symbol, required")
	cash := flags.String("cash", "1000", "initial cash as an exact decimal")
	target := flags.String("buy-target", "1000", "per-buy cash target as an exact decimal")
	slippage := flags.String("slippage-bps", "0", "adverse slippage in basis points")
	output := flags.String("output", "summary", "output format: summary or json")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: backtest-engine -input FILE -symbol SYMBOL [options]")
		fmt.Fprintln(stderr, "Input must already contain eligible session minutes. No calendar filtering or resampling runs.")
		fmt.Fprintln(stderr, "Example strategy: buy on an up candle, exit on a down candle, hold on an equal candle.")
		fmt.Fprintln(stderr, "Each source minute is a strategy bar. Orders fill at a subsequent eligible minute close.")
		flags.SetOutput(stderr)
		flags.PrintDefaults()
		flags.SetOutput(io.Discard)
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if strings.TrimSpace(*input) == "" || strings.TrimSpace(*symbol) == "" {
		return fmt.Errorf("-input and -symbol are required; use -help for usage")
	}
	if *output != "summary" && *output != "json" {
		return fmt.Errorf("unsupported -output %q: use summary or json", *output)
	}
	var source marketdata.MinuteSource
	var err error
	switch strings.ToLower(filepath.Ext(*input)) {
	case ".csv":
		source, err = marketdata.OpenCSV(*input)
	case ".parquet":
		source, err = marketdata.OpenParquet(*input)
	default:
		return fmt.Errorf("unsupported input extension: use .csv or .parquet")
	}
	if err != nil {
		return err
	}
	minutes, err := marketdata.Load(source, *symbol)
	if err != nil {
		return err
	}
	bars := make([]strategy.Bar, len(minutes))
	for i, m := range minutes {
		bars[i] = strategy.Bar{Symbol: m.Symbol, IntervalStart: m.IntervalStart, IntervalEnd: m.IntervalEnd, AvailableAt: m.AvailableAt, Open: m.Open, High: m.High, Low: m.Low, Close: m.Close, Volume: m.Volume}
	}
	candleStrategy, err := strategy.NewBuilder().
		EntryWhen(func(ctx strategy.Context) bool { return ctx.Bar.Close > ctx.Bar.Open }).
		ExitWhen(func(ctx strategy.Context) bool { return ctx.Bar.Close < ctx.Bar.Open }).Build()
	if err != nil {
		return err
	}
	result, err := simulation.Run(simulation.Config{InitialCash: *cash, BuyTarget: *target, SlippageBPS: *slippage}, minutes, bars, candleStrategy.OnCompletedBar)
	if err != nil {
		return err
	}
	report, err := results.Build(result)
	if err != nil {
		return err
	}
	if *output == "json" {
		return report.WriteJSON(stdout)
	}
	return report.WriteSummary(stdout)
}

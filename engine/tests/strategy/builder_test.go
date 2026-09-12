package strategy_test

import (
	"backtest_engine/engine/internal/strategy"
	"testing"
)

func TestSignals(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		position, pending, entry, exit bool
		want                           strategy.Signal
		entryCalls, exitCalls          int
	}{
		{"flat match", false, false, true, true, strategy.Buy, 1, 0},
		{"flat miss", false, false, false, true, strategy.Hold, 1, 0},
		{"long match", true, false, true, true, strategy.Exit, 0, 1},
		{"long miss", true, false, true, false, strategy.Hold, 0, 1},
		{"entry pending", false, true, true, true, strategy.Hold, 0, 0},
		{"exit pending", true, true, true, true, strategy.Hold, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entryCalls, exitCalls := 0, 0
			s, err := strategy.NewBuilder().EntryWhen(func(ctx strategy.Context) bool {
				entryCalls++
				if ctx.Bar.Close != 42 {
					t.Fatal("bar lost")
				}
				ctx.Pending = true
				return tc.entry
			}).ExitWhen(func(strategy.Context) bool { exitCalls++; return tc.exit }).Build()
			if err != nil {
				t.Fatal(err)
			}
			ctx := strategy.Context{Bar: strategy.Bar{Close: 42}, InPosition: tc.position, Pending: tc.pending}
			if got := s.OnCompletedBar(ctx); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			if entryCalls != tc.entryCalls || exitCalls != tc.exitCalls {
				t.Fatalf("calls = %d/%d", entryCalls, exitCalls)
			}
			if ctx.Pending != tc.pending {
				t.Fatal("caller snapshot changed")
			}
		})
	}
}

func TestComposition(t *testing.T) {
	for _, a := range []bool{false, true} {
		for _, b := range []bool{false, true} {
			x, y := strategy.Rule(func(strategy.Context) bool { return a }), strategy.Rule(func(strategy.Context) bool { return b })
			if strategy.All(x, y)(strategy.Context{}) != (a && b) {
				t.Fatal("All truth table")
			}
			if strategy.Any(x, y)(strategy.Context{}) != (a || b) {
				t.Fatal("Any truth table")
			}
			if strategy.Not(x)(strategy.Context{}) != !a {
				t.Fatal("Not truth table")
			}
			if strategy.All(strategy.Any(x, y), strategy.Not(x))(strategy.Context{}) != ((a || b) && !a) {
				t.Fatal("nested expression")
			}
		}
	}
	never := func(strategy.Context) bool { t.Fatal("short-circuited rule called"); return false }
	strategy.All(func(strategy.Context) bool { return false }, never)(strategy.Context{})
	strategy.Any(func(strategy.Context) bool { return true }, never)(strategy.Context{})
}

func TestInvalidRules(t *testing.T) {
	yes := strategy.Rule(func(strategy.Context) bool { return true })
	for _, rule := range []strategy.Rule{nil, strategy.All(), strategy.Any(), strategy.All(yes, nil), strategy.Any(nil, yes), strategy.Not(nil), strategy.Not(strategy.All())} {
		if s, err := strategy.NewBuilder().EntryWhen(rule).ExitWhen(yes).Build(); err == nil || s != nil {
			t.Fatal("invalid entry accepted")
		}
		if s, err := strategy.NewBuilder().EntryWhen(yes).ExitWhen(rule).Build(); err == nil || s != nil {
			t.Fatal("invalid exit accepted")
		}
	}
	if s, err := (strategy.Builder{}).Build(); err == nil || s != nil {
		t.Fatal("empty builder accepted")
	}
	if (strategy.Strategy{}).OnCompletedBar(strategy.Context{}) != strategy.Hold {
		t.Fatal("zero strategy must hold")
	}
}

func TestBuilderAndGroupIsolation(t *testing.T) {
	yes := strategy.Rule(func(strategy.Context) bool { return true })
	no := strategy.Rule(func(strategy.Context) bool { return false })
	for _, combine := range []func(...strategy.Rule) strategy.Rule{strategy.All, strategy.Any} {
		rules := []strategy.Rule{yes}
		group := combine(rules...)
		rules[0] = no
		if !group(strategy.Context{}) {
			t.Fatal("group retained caller slice")
		}
	}
	b := strategy.NewBuilder().EntryWhen(yes).ExitWhen(no)
	s, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	b = b.EntryWhen(no)
	other, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if s.OnCompletedBar(strategy.Context{}) != strategy.Buy || other.OnCompletedBar(strategy.Context{}) != strategy.Hold {
		t.Fatal("builder results not independent")
	}
}

package strategy

import (
	"errors"
)

// Builder configures one entry expression and one exit expression. Its zero
// value is ready to configure. Methods return a copy; repeated setters replace
// the previous expression. Use All or Any to combine conditions explicitly.
type Builder struct{ entry, exit Rule }

func NewBuilder() Builder { return Builder{} }

func (b Builder) EntryWhen(rule Rule) Builder { b.entry = rule; return b }
func (b Builder) ExitWhen(rule Rule) Builder  { b.exit = rule; return b }

// Build requires both expressions. Use a predicate returning false to disable
// one side deliberately. Later builder changes do not affect the result.
func (b Builder) Build() (*Strategy, error) {
	if b.entry == nil {
		return nil, errors.New("strategy: entry rule is required")
	}
	if b.exit == nil {
		return nil, errors.New("strategy: exit rule is required")
	}
	return &Strategy{entry: b.entry, exit: b.exit}, nil
}

// Strategy owns rule expressions, but no account or indicator state. Captured
// mutable state belongs to the caller and must not be shared across runs.
type Strategy struct{ entry, exit Rule }

// OnCompletedBar returns Hold while pending, Buy on an entry match while flat,
// or Exit on an exit match while invested. A zero-value Strategy always holds.
// It does not catch rule panics or implement the engine's failure lifecycle.
func (s Strategy) OnCompletedBar(ctx Context) Signal {
	if ctx.Pending {
		return Hold
	}
	if ctx.InPosition {
		if s.exit != nil && s.exit(ctx) {
			return Exit
		}
	} else if s.entry != nil && s.entry(ctx) {
		return Buy
	}
	return Hold
}

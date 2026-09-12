package strategy

// Rule tests the current snapshot. Rules should be pure predicates. Maintain
// indicators before evaluation because short-circuiting can skip rules.
type Rule func(Context) bool

// All evaluates rules in order until one is false. Empty or nil-containing
// groups return nil and are rejected by Build.
func All(rules ...Rule) Rule {
	if !validRules(rules) {
		return nil
	}
	rules = append([]Rule(nil), rules...)
	return func(ctx Context) bool {
		for _, rule := range rules {
			if !rule(ctx) {
				return false
			}
		}
		return true
	}
}

// Any evaluates rules in order until one is true. Empty or nil-containing
// groups return nil and are rejected by Build.
func Any(rules ...Rule) Rule {
	if !validRules(rules) {
		return nil
	}
	rules = append([]Rule(nil), rules...)
	return func(ctx Context) bool {
		for _, rule := range rules {
			if rule(ctx) {
				return true
			}
		}
		return false
	}
}

// Not negates a rule. A nil rule produces nil.
func Not(rule Rule) Rule {
	if rule == nil {
		return nil
	}
	return func(ctx Context) bool { return !rule(ctx) }
}

func validRules(rules []Rule) bool {
	if len(rules) == 0 {
		return false
	}
	for _, rule := range rules {
		if rule == nil {
			return false
		}
	}
	return true
}

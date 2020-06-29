package reveald

type callchained struct {
	fn   func(*QueryBuilder, FeatureFunc) (*Result, error)
	next *callchained
}

func (cc *callchained) add(f Feature) *callchained {
	n := &callchained{
		f.Process,
		cc,
	}

	return n
}

type callchain struct {
	root *callchained
}

func (cc *callchain) add(f Feature) {
	if cc.root == nil {
		cc.root = &callchained{}
	}

	cc.root = cc.root.add(f)
}

func (cc *callchain) exec(qb *QueryBuilder, fn FeatureFunc) (*Result, error) {
	n := cc.root
	for n.fn != nil {
		fn = func(ff FeatureFunc, c *callchained) FeatureFunc {
			return func(qb *QueryBuilder) (*Result, error) {
				return c.fn(qb, ff)
			}
		}(fn, n)
		n = n.next
	}

	return fn(qb)
}

package reveald

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeF struct {
	called bool
}

func (f *fakeF) Process(qb *QueryBuilder, next FeatureFunc) (*Result, error) {
	f.called = true
	return next(qb)
}

func Test_Chain(t *testing.T) {
	a := &fakeF{}
	b := &fakeF{}
	c := &fakeF{}

	cc := &callchain{}
	cc.add(a)
	cc.add(b)
	cc.add(c)

	called := false
	_, err := cc.exec(NewQueryBuilder(nil), func(_ *QueryBuilder) (*Result, error) {
		called = true
		return nil, nil
	})
	assert.NoError(t, err)

	assert.True(t, a.called)
	assert.True(t, b.called)
	assert.True(t, c.called)
	assert.True(t, called)
}

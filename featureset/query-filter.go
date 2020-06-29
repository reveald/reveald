package featureset

import (
	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type QueryFilterFeature struct {
	name   string
	fields []string
}

type QueryFilterOption func(*QueryFilterFeature)

func WithQueryParam(name string) QueryFilterOption {
	return func(qff *QueryFilterFeature) {
		qff.name = name
	}
}

func WithFields(fields ...string) QueryFilterOption {
	return func(qff *QueryFilterFeature) {
		qff.fields = fields
	}
}

func NewQueryFilterFeature(opts ...QueryFilterOption) *QueryFilterFeature {
	qff := &QueryFilterFeature{
		name:   "q",
		fields: []string{},
	}

	for _, opt := range opts {
		opt(qff)
	}

	return qff
}

func (qff *QueryFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	if !builder.Request().Has(qff.name) {
		return next(builder)
	}

	v, err := builder.Request().Get(qff.name)
	if err != nil || v.Value() == "" {
		return next(builder)
	}

	builder.With(elastic.NewQueryStringQuery(v.Value()).Lenient(true))
	return next(builder)
}

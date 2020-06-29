package featureset

import (
	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type StaticFilterFeature struct {
	query elastic.Query
}

type StaticFilterOption func(*elastic.BoolQuery)

func WithRequiredProperty(property string) StaticFilterOption {
	return func(query *elastic.BoolQuery) {
		query.Must(elastic.NewExistsQuery(property))
	}
}

func WithRequiredValue(property string, value interface{}) StaticFilterOption {
	return func(query *elastic.BoolQuery) {
		query.Must(elastic.NewTermQuery(property, value))
	}
}

func NewStaticFilterFeature(opts ...StaticFilterOption) *StaticFilterFeature {
	if len(opts) == 0 {
		return &StaticFilterFeature{nil}
	}

	query := elastic.NewBoolQuery()

	for _, opt := range opts {
		opt(query)
	}

	return &StaticFilterFeature{query}
}

func (sff *StaticFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	if sff.query != nil {
		builder.With(sff.query)
	}

	return next(builder)
}

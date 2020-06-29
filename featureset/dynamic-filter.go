package featureset

import (
	"fmt"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type DynamicFilterFeature struct {
	property string
}

func NewDynamicFilterFeature(property string) *DynamicFilterFeature {
	return &DynamicFilterFeature{
		property,
	}
}

func (dff *DynamicFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	dff.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return dff.handle(r)
}

func (dff *DynamicFilterFeature) build(builder *reveald.QueryBuilder) {
	keyword := fmt.Sprintf("%s.keyword", dff.property)

	builder.Aggregation(dff.property,
		elastic.NewTermsAggregation().Field(keyword))

	if builder.Request().Has(dff.property) {
		p, err := builder.Request().Get(dff.property)
		if err != nil {
			return
		}

		bq := elastic.NewBoolQuery()
		for _, v := range p.Values() {
			bq = bq.Should(elastic.NewTermQuery(keyword, v))
		}

		builder.With(bq)
	}
}

func (dff *DynamicFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawResult().Aggregations.Terms(dff.property)
	if !ok {
		return result, nil
	}

	var buckets []*reveald.ResultBucket
	for _, bucket := range agg.Buckets {
		if bucket == nil {
			continue
		}

		buckets = append(buckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	result.Aggregations[dff.property] = buckets
	return result, nil
}

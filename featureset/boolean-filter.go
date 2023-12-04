package featureset

import (
	"fmt"
	"strconv"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type BooleanFilterFeature struct {
	property string
	agg      AggregationFeature
}

func NewBooleanFilterFeature(property string, opts ...AggregationOption) *BooleanFilterFeature {
	return &BooleanFilterFeature{
		property: property,
		agg:      buildAggregationFeature(opts...),
	}
}

func (bff *BooleanFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	bff.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return bff.handle(r)
}

func (bff *BooleanFilterFeature) build(builder *reveald.QueryBuilder) {
	keyword := fmt.Sprintf("%s.keyword", bff.property)

	builder.Aggregation(bff.property,
		elastic.NewTermsAggregation().Field(keyword).Size(bff.agg.size))

	if !builder.Request().Has(bff.property) {
		return
	}

	v, err := builder.Request().Get(bff.property)
	if err != nil {
		return
	}

	bl, err := strconv.ParseBool(v.Value())
	if err != nil {
		return
	}

	builder.With(elastic.NewTermQuery(bff.property, bl))
}

func (bff *BooleanFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawResult().Aggregations.Terms(bff.property)
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

	result.Aggregations[bff.property] = buckets
	return result, nil
}

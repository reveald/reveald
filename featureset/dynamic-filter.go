package featureset

import (
	"fmt"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type DynamicFilterFeature struct {
	property string
	nested   bool
	agg      AggregationFeature
}

func NewDynamicFilterFeature(property string, opts ...AggregationOption) *DynamicFilterFeature {
	return &DynamicFilterFeature{
		property: property,
		nested:   false,
		agg:      buildAggregationFeature(opts...),
	}
}

func NewNestedDocumentFilterFeature(property string, opts ...AggregationOption) *DynamicFilterFeature {
	return &DynamicFilterFeature{
		property: property,
		nested:   true,
		agg:      buildAggregationFeature(opts...),
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

	if !dff.nested {
		builder.Aggregation(dff.property,
			elastic.NewTermsAggregation().Field(keyword).Size(dff.agg.size))
	} else {
		path := strings.Split(dff.property, ".")[0]
		builder.Aggregation(dff.property,
			elastic.NewNestedAggregation().
				Path(path).
				SubAggregation(dff.property, elastic.NewTermsAggregation().Field(keyword).Size(dff.agg.size)))
	}

	if builder.Request().Has(dff.property) {
		p, err := builder.Request().Get(dff.property)
		if err != nil {
			return
		}

		bq := elastic.NewBoolQuery()
		for _, v := range p.Values() {
			bq = bq.Should(elastic.NewTermQuery(keyword, v))
		}

		if !dff.nested {
			builder.With(bq)
		} else {
			path := strings.Split(dff.property, ".")[0]
			builder.With(elastic.NewNestedQuery(path, bq))
		}
	}
}

func (dff *DynamicFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	var agg *elastic.AggregationBucketKeyItems

	if !dff.nested {
		items, ok := result.RawResult().Aggregations.Terms(dff.property)
		if !ok {
			return result, nil
		}

		agg = items
	} else {
		bucket, ok := result.RawResult().Aggregations.Children(dff.property)
		if !ok {
			return result, nil
		}

		items, ok := bucket.Aggregations.Terms(dff.property)
		if !ok {
			return result, nil
		}

		agg = items
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

package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald"
)

const defaultAggregationSize = 10

// AggregationFeature provides a generic terms aggregation feature that can be configured
// for different field aggregations. It creates a terms aggregation on a specified field
// and can be used as a standalone feature or as a component in other features.
//
// Example:
//
//	// Create a terms aggregation feature for the "category" field
//	aggFeature := featureset.NewAggregationFeature("category",
//	    featureset.WithAggregationSize(20),
//	)
//
//	// Use the aggregation feature in a feature chain
//	result, err := aggFeature.Process(builder, nextFeature)
type AggregationFeature struct {
	field string
	size  int
}

// AggregationOption is a functional option for configuring an AggregationFeature.
type AggregationOption func(*AggregationFeature)

// WithAggregationSize sets the size limit for aggregations.
//
// This controls how many buckets/terms are returned in aggregation results.
//
// Example:
//
//	// Create an aggregation feature that returns up to 50 terms
//	aggFeature := featureset.NewAggregationFeature(
//	    featureset.WithAggregationSize(50),
//	)
func WithAggregationSize(size int) AggregationOption {
	return func(af *AggregationFeature) {
		af.size = size
	}
}

// NewAggregationFeature creates a new terms aggregation feature for the specified field.
//
// By default, it uses a size of 10 for aggregations.
//
// Example:
//
//	// Create a basic terms aggregation feature for the "category" field
//	aggFeature := featureset.NewAggregationFeature("category")
//
//	// Create an aggregation feature with custom size
//	aggFeature := featureset.NewAggregationFeature("category",
//	    featureset.WithAggregationSize(25),
//	)
func NewAggregationFeature(field string, opts ...AggregationOption) *AggregationFeature {
	af := &AggregationFeature{
		field: field,
		size:  defaultAggregationSize,
	}

	for _, opt := range opts {
		opt(af)
	}

	return af
}

// Process implements the Feature interface for AggregationFeature.
//
// It creates a terms aggregation on the specified field and processes the results.
//
// Example:
//
//	// Use the aggregation feature in a feature chain
//	result, err := aggFeature.Process(builder, nextFeature)
func (af *AggregationFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	af.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return af.handle(r)
}

// build adds the terms aggregation to the query builder.
func (af *AggregationFeature) build(builder *reveald.QueryBuilder) {
	// Use keyword field for terms aggregation to avoid text field analysis
	keyword := af.field + ".keyword"
	field := keyword
	size := af.size

	termAgg := types.Aggregations{
		Terms: &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		},
	}

	builder.Aggregation(af.field, termAgg)
}

// handle processes the terms aggregation results.
func (af *AggregationFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	// Extract terms aggregate from aggregations
	agg, ok := result.RawAggregations()[af.field]
	if !ok {
		return result, nil
	}

	terms, ok := agg.(*types.StringTermsAggregate)
	if !ok {
		return result, nil
	}

	buckets := terms.Buckets.([]types.StringTermsBucket)

	var resultBuckets []*reveald.ResultBucket
	for _, bucket := range buckets {
		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	result.Aggregations[af.field] = resultBuckets
	return result, nil
}

// buildAggregationFeature is a helper function for building aggregation features.
// This is kept for backward compatibility for use as a component in other features.
// For standalone usage, use NewAggregationFeature instead.
func buildAggregationFeature(opts ...AggregationOption) AggregationFeature {
	agg := AggregationFeature{
		field: "", // Field will be set by the parent feature
		size:  defaultAggregationSize,
	}

	for _, opt := range opts {
		opt(&agg)
	}

	return agg
}

package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald"
)

// HistogramFeature creates a histogram aggregation for numeric fields.
//
// It groups documents based on numeric field values into buckets and
// can also apply range filters based on request parameters.
//
// Example:
//
//	// Create a basic histogram feature for the "price" field
//	histogramFeature := featureset.NewHistogramFeature("price")
//
//	// Use the histogram feature in a feature chain
//	result, err := histogramFeature.Process(builder, nextFeature)
type HistogramFeature struct {
	property    string
	neg         bool
	zeroBucket  bool
	interval    float64
	minDocCount int64
}

// HistogramOption is a functional option for configuring a HistogramFeature.
type HistogramOption func(*HistogramFeature)

// WithNegativeValuesAllowed enables support for negative values in the histogram.
//
// By default, negative values are filtered out.
//
// Example:
//
//	// Create a histogram feature that allows negative values
//	histogramFeature := featureset.NewHistogramFeature("price",
//	    featureset.WithNegativeValuesAllowed(),
//	)
func WithNegativeValuesAllowed() HistogramOption {
	return func(hf *HistogramFeature) {
		hf.neg = true
	}
}

// WithoutZeroBucket disables the creation of a bucket for zero values.
//
// By default, a bucket for zero values is created.
//
// Example:
//
//	// Create a histogram feature without a zero bucket
//	histogramFeature := featureset.NewHistogramFeature("price",
//	    featureset.WithoutZeroBucket(),
//	)
func WithoutZeroBucket() HistogramOption {
	return func(hf *HistogramFeature) {
		hf.zeroBucket = false
	}
}

// WithInterval sets the interval size for the histogram buckets.
//
// The interval determines the size of each bucket in the histogram.
//
// Example:
//
//	// Create a histogram feature with an interval of 50
//	histogramFeature := featureset.NewHistogramFeature("price",
//	    featureset.WithInterval(50),
//	)
func WithInterval(interval float64) HistogramOption {
	return func(hf *HistogramFeature) {
		hf.interval = interval
	}
}

// WithMinimumDocumentCount sets the minimum number of documents required for a bucket to be included.
//
// Buckets with fewer documents than this threshold will be excluded from the results.
//
// Example:
//
//	// Create a histogram feature that only includes buckets with at least 5 documents
//	histogramFeature := featureset.NewHistogramFeature("price",
//	    featureset.WithMinimumDocumentCount(5),
//	)
func WithMinimumDocumentCount(minDocCount int64) HistogramOption {
	return func(hf *HistogramFeature) {
		hf.minDocCount = minDocCount
	}
}

// NewHistogramFeature creates a new histogram feature for the specified property.
//
// Example:
//
//	// Create a histogram feature with custom settings
//	histogramFeature := featureset.NewHistogramFeature("price",
//	    featureset.WithInterval(50),
//	    featureset.WithMinimumDocumentCount(5),
//	    featureset.WithNegativeValuesAllowed(),
//	)
func NewHistogramFeature(property string, opts ...HistogramOption) *HistogramFeature {
	hf := &HistogramFeature{
		property:    property,
		neg:         false,
		zeroBucket:  true,
		interval:    100,
		minDocCount: 0,
	}

	for _, opt := range opts {
		opt(hf)
	}

	return hf
}

// Process applies the histogram aggregation to the query builder and processes the result.
//
// It adds a histogram aggregation to the query and processes any range filters
// from the request parameters.
//
// Example:
//
//	// Use the histogram feature in a feature chain
//	result, err := histogramFeature.Process(builder, nextFeature)
func (hf *HistogramFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	hf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return hf.handle(r)
}

// build adds the histogram aggregation to the query builder.
func (hf *HistogramFeature) build(builder *reveald.QueryBuilder) {
	// Create histogram aggregation directly with typed objects
	field := hf.property
	interval := types.Float64(float64(hf.interval))
	minDocCount := int(hf.minDocCount)

	histAgg := types.Aggregations{
		Histogram: &types.HistogramAggregation{
			Field:       &field,
			Interval:    &interval,
			MinDocCount: &minDocCount,
		},
	}

	builder.Aggregation(hf.property, histAgg)

	// Check if we need to add a range query
	p, err := builder.Request().Get(hf.property)
	if err != nil || !p.IsRangeValue() {
		return
	}

	// Create a range query directly with typed objects
	var numRangeQuery types.NumberRangeQuery

	// Get max value if available
	maxVal, hasMax := p.Max()
	if hasMax && (maxVal >= 0 || hf.neg) {
		lteValue := types.Float64(maxVal)
		numRangeQuery.Lte = &lteValue
	}

	// Get min value if available
	minVal, hasMin := p.Min()
	if hasMin && (!hasMax || minVal <= maxVal) && (minVal >= 0 || hf.neg) {
		gteValue := types.Float64(minVal)
		numRangeQuery.Gte = &gteValue
	}

	// Only add the range query if we have constraints
	if numRangeQuery.Lte != nil || numRangeQuery.Gte != nil {
		rangeQuery := types.Query{
			Range: map[string]types.RangeQuery{
				hf.property: &numRangeQuery,
			},
		}

		builder.With(rangeQuery)
	}
}

// handle processes the histogram aggregation results.
func (hf *HistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	// Extract histogram aggregate from aggregations
	agg, ok := result.RawAggregations()[hf.property]
	if !ok {
		return result, nil
	}

	histogram, ok := agg.(types.HistogramAggregate)

	buckets := histogram.Buckets.([]types.HistogramBucket)
	if !ok {
		return result, nil
	}

	var resultBuckets []*reveald.ResultBucket
	zeroOut := len(buckets) > 0

	for _, bucket := range buckets {
		if bucket.Key <= 0 {
			zeroOut = false
		}

		if bucket.Key == 0 && !hf.zeroBucket && bucket.DocCount == 0 {
			continue
		}

		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	// Add zero bucket if needed
	if hf.zeroBucket && zeroOut {
		bucket := &reveald.ResultBucket{
			Value:    0,
			HitCount: 0,
		}
		resultBuckets = append(resultBuckets, nil)
		copy(resultBuckets[1:], resultBuckets)
		resultBuckets[0] = bucket
	}

	result.Aggregations[hf.property] = resultBuckets
	return result, nil
}

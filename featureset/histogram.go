package featureset

import (
	"fmt"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type HistogramFeature struct {
	property    string
	neg         bool
	interval    float64
	minDocCount int64
}

type HistogramOption func(*HistogramFeature)

func WithNegativeValuesAllowed() HistogramOption {
	return func(hf *HistogramFeature) {
		hf.neg = true
	}
}

func WithInterval(interval float64) HistogramOption {
	return func(hf *HistogramFeature) {
		hf.interval = interval
	}
}

func WithMinimumDocumentCount(minDocCount int64) HistogramOption {
	return func(hf *HistogramFeature) {
		hf.minDocCount = minDocCount
	}
}

func NewHistogramFeature(property string, opts ...HistogramOption) *HistogramFeature {
	hf := &HistogramFeature{
		property:    property,
		neg:         false,
		interval:    100,
		minDocCount: 0,
	}

	for _, opt := range opts {
		opt(hf)
	}

	return hf
}

func (hf *HistogramFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	hf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return hf.handle(r)
}

func (hf *HistogramFeature) build(builder *reveald.QueryBuilder) {
	builder.Aggregation(hf.property,
		elastic.NewHistogramAggregation().
			Field(hf.property).
			Interval(hf.interval).
			MinDocCount(hf.minDocCount))

	p, err := builder.Request().Get(hf.property)
	if err != nil || !p.IsRangeValue() {
		return
	}

	q := elastic.NewRangeQuery(hf.property)
	max, wmax := p.Max()
	if wmax && (max >= 0 || hf.neg) {
		q.Lte(max)
	}

	min, wmin := p.Min()
	if wmin && (!wmax || min <= max) && (min >= 0 || hf.neg) {
		q.Gte(min)
	}

	builder.With(q)
}

func (hf *HistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawResult().Aggregations.Histogram(hf.property)
	if !ok {
		return result, nil
	}

	var buckets []*reveald.ResultBucket
	zeroOut := len(agg.Buckets) > 0
	for _, bucket := range agg.Buckets {
		if bucket == nil {
			continue
		}

		if bucket.Key <= 0 {
			zeroOut = false
		}

		buckets = append(buckets, &reveald.ResultBucket{
			Value:    fmt.Sprintf("%0.f", bucket.Key),
			HitCount: bucket.DocCount,
		})
	}

	if zeroOut {
		bucket := &reveald.ResultBucket{
			Value:    0,
			HitCount: 0,
		}
		buckets = append(buckets, nil)
		copy(buckets[1:], buckets)
		buckets[0] = bucket
	}

	result.Aggregations[hf.property] = buckets
	return result, nil
}

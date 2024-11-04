package featureset

import (
	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type DateRangeHistogramFeature struct {
	property   string
	zerobucket bool
	rangeFrom  *string
	rangeTo    *string
	key        string
}

type DateRangeHistogramOption func(*DateRangeHistogramFeature)

func WithoutDateRangeHistogramZeroBucket() DateRangeHistogramOption {
	return func(dhf *DateRangeHistogramFeature) {
		dhf.zerobucket = false
	}
}

func RangeFrom(from string) DateRangeHistogramOption {
	return func(dhf *DateRangeHistogramFeature) {
		dhf.rangeFrom = &from
	}
}

func RangeTo(value string) DateRangeHistogramOption {
	return func(dhf *DateRangeHistogramFeature) {
		dhf.rangeTo = &value
	}
}

func NewDateRangeHistogramFeature(property string, rangeKey string, opts ...DateRangeHistogramOption) *DateRangeHistogramFeature {
	dhf := &DateRangeHistogramFeature{
		property:   property,
		zerobucket: true,
		key:        rangeKey,
	}

	for _, opt := range opts {
		opt(dhf)
	}

	return dhf
}

func (dhf *DateRangeHistogramFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	dhf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return dhf.handle(r)
}

func (dhf *DateRangeHistogramFeature) build(builder *reveald.QueryBuilder) {
	agg := elastic.NewDateRangeAggregation().
		Field(dhf.property).
		Format("MM-yyyy").
		Keyed(true)
	if dhf.rangeFrom != nil && dhf.rangeTo != nil {
		agg.AddRangeWithKey(dhf.key, *dhf.rangeFrom, *dhf.rangeTo)
	} else if dhf.rangeFrom != nil {
		agg.AddUnboundedToWithKey(dhf.key, *dhf.rangeFrom)
	} else if dhf.rangeTo != nil {
		agg.AddUnboundedFromWithKey(dhf.key, *dhf.rangeTo)
	}

	builder.Aggregation(dhf.property, agg)

	if !builder.Request().Has(dhf.property) {
		return
	}

	q := elastic.NewRangeQuery(dhf.property)

	if dhf.rangeFrom != nil {
		q.Gte(*dhf.rangeFrom)
	}
	if dhf.rangeTo != nil {
		q.Lte(*dhf.rangeTo)
	}

	builder.With(q)
}

func (dhf *DateRangeHistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawResult().Aggregations.KeyedRange(dhf.property)
	if !ok {
		return result, nil
	}

	buckets, ok := agg.Buckets[dhf.key]
	if !ok {
		return result, nil
	}

	if buckets.DocCount == 0 && !dhf.zerobucket {
		return result, nil
	}

	result.Aggregations[dhf.property] = []*reveald.ResultBucket{
		{
			Value:    dhf.key,
			HitCount: buckets.DocCount,
		},
	}

	return result, nil
}

package featureset

import (
	"errors"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type DateHistogramInterval string

const (
	DateIntervalYearly  DateHistogramInterval = "year"
	DateIntervalMonthly DateHistogramInterval = "month"
	DateIntervalDaily   DateHistogramInterval = "day"
)

type DateHistogramFeature struct {
	property   string
	interval   DateHistogramInterval
	dateFormat string
}

func NewDateHistogramFeature(property string, interval DateHistogramInterval) *DateHistogramFeature {
	dateFormat := "2006"

	switch interval {
	case DateIntervalYearly:
		dateFormat = "2006"
	case DateIntervalMonthly:
		dateFormat = "2006-01"
	case DateIntervalDaily:
		dateFormat = "2006-01-02"
	}

	return &DateHistogramFeature{
		property,
		interval,
		dateFormat,
	}
}

func (dhf *DateHistogramFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	dhf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return dhf.handle(r)
}

func (dhf *DateHistogramFeature) build(builder *reveald.QueryBuilder) {
	builder.Aggregation(dhf.property,
		elastic.NewDateHistogramAggregation().
			Field(dhf.property).
			FixedInterval(string(dhf.interval)).
			MinDocCount(0))

	p, err := builder.Request().Get(dhf.property)
	if err != nil || !p.IsRangeValue() {
		return
	}

	q := elastic.NewRangeQuery(dhf.property)
	mx, ok := p.Max()
	max, err := asTime(mx, dhf.dateFormat)
	if ok && err == nil {
		q.Lte(max)
	}

	mn, ok := p.Min()
	min, err := asTime(mn, dhf.dateFormat)
	if ok && err == nil && min.Before(max) {
		q.Gte(min)
	}

	builder.With(q)
}

func (dhf *DateHistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawResult().Aggregations.DateHistogram(dhf.property)
	if !ok {
		return result, nil
	}

	var buckets []*reveald.ResultBucket
	for _, bucket := range agg.Buckets {
		buckets = append(buckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	result.Aggregations[dhf.property] = buckets
	return result, nil
}

func asTime(raw float64, format string) (time.Time, error) {
	if raw < 0 {
		return time.Time{}, errors.New("time value not available")
	}

	return time.Parse(format, fmt.Sprintf("%.0f", raw))
}

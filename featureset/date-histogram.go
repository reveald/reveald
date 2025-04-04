package featureset

import (
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type (
	DateCalendarHistogramInterval string
	DateFixedHistogramInterval    string
)

const (
	DateCalendarIntervalYearly  DateCalendarHistogramInterval = "year"
	DateCalendarIntervalMonthly DateCalendarHistogramInterval = "month"
	DateCalendarIntervalDaily   DateCalendarHistogramInterval = "day"

	DateFixedIntervalDaily        DateFixedHistogramInterval = "1d"
	DateFixedIntervalHours        DateFixedHistogramInterval = "1h"
	DateFixedIntervalMinutes      DateFixedHistogramInterval = "1m"
	DateFixedIntervalSeconds      DateFixedHistogramInterval = "1s"
	DateFixedIntervalMilliseconds DateFixedHistogramInterval = "1ms"
)

const (
	IntervalFixed    = "fixed"
	IntervalCalendar = "calendar"
)

type DateHistogramFeature struct {
	property      string
	interval      string
	dateFormat    string
	zerobucket    bool
	applyInterval func(*elastic.DateHistogramAggregation) *elastic.DateHistogramAggregation
}

type DateHistogramOption func(*DateHistogramFeature)

func WithoutDateHistogramZeroBucket() DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.zerobucket = false
	}
}

func WithFixedInterval(interval DateFixedHistogramInterval) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.interval = string(interval)
		switch interval {
		case DateFixedIntervalDaily:
			dhf.dateFormat = "yyyy-MM-dd"
		case DateFixedIntervalHours:
			dhf.dateFormat = "yyyy-MM-dd HH"
		case DateFixedIntervalMinutes:
			dhf.dateFormat = "yyyy-MM-dd HH:mm"
		case DateFixedIntervalSeconds:
			dhf.dateFormat = "yyyy-MM-dd HH:mm:ss"
		case DateFixedIntervalMilliseconds:
			dhf.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
		}
		dhf.applyInterval = func(agg *elastic.DateHistogramAggregation) *elastic.DateHistogramAggregation {
			return agg.FixedInterval(string(interval))
		}
	}
}

func WithCalendarInterval(interval DateCalendarHistogramInterval) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.interval = string(interval)
		switch interval {
		case DateCalendarIntervalYearly:
			dhf.dateFormat = "yyyy"
		case DateCalendarIntervalMonthly:
			dhf.dateFormat = "yyyy-MM"
		case DateCalendarIntervalDaily:
			dhf.dateFormat = "yyyy-MM-dd"
		}
		dhf.applyInterval = func(agg *elastic.DateHistogramAggregation) *elastic.DateHistogramAggregation {
			return agg.CalendarInterval(string(interval))
		}
	}
}

func WithRangeDateFormat(dateFormat string) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.dateFormat = dateFormat
	}
}

func NewDateHistogramFeature(property string, opts ...DateHistogramOption) *DateHistogramFeature {
	dhf := &DateHistogramFeature{
		property:   property,
		zerobucket: true,
	}

	WithCalendarInterval(DateCalendarIntervalDaily)(dhf)

	for _, opt := range opts {
		opt(dhf)
	}

	return dhf
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
		dhf.applyInterval(
			elastic.NewDateHistogramAggregation().
				Field(dhf.property).
				Format(dhf.dateFormat).
				MinDocCount(0),
		))

	p, err := builder.Request().Get(dhf.property)
	if err != nil {
		return
	}

	// Handle exact value filters
	if len(p.Values()) > 0 && !p.IsDateRangeValue() {
		bq := elastic.NewBoolQuery()

		for _, v := range p.Values() {
			startValue, err := ParseTimeFrom(v, dhf.interval)
			if err != nil {
				return
			}
			endValue := IntervalEnd(startValue, dhf.interval)

			q := elastic.NewRangeQuery(dhf.property)
			q.Gte(startValue)
			q.Lte(endValue)

			bq = bq.Should(q)
		}

		bq = bq.MinimumShouldMatch("1")
		builder.With(bq)
		return
	}

	if p.IsDateRangeValue() {
		bq := elastic.NewBoolQuery()

		// Handle min/max range filters
		q := elastic.NewRangeQuery(dhf.property)

		max, hasMax := p.MaxDate(getDateTimeFormat(dhf.interval))
		if hasMax {
			q.Lte(max)
		}

		min, hasMin := p.MinDate(getDateTimeFormat(dhf.interval))
		if hasMin && (!hasMax || min.Before(max)) {
			q.Gte(min)
		}

		bq = bq.Must(q)
		builder.With(bq)
	}
}

func (dhf *DateHistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawResult().Aggregations.DateHistogram(dhf.property)
	if !ok {
		return result, nil
	}

	var buckets []*reveald.ResultBucket
	for _, bucket := range agg.Buckets {
		if bucket.DocCount == 0 && !dhf.zerobucket {
			continue
		}
		buckets = append(buckets, &reveald.ResultBucket{
			Value: *bucket.KeyAsString,

			HitCount: bucket.DocCount,
		})
	}

	result.Aggregations[dhf.property] = buckets
	return result, nil
}

func IntervalEnd(t time.Time, interval string) time.Time {
	switch interval {
	case string(DateCalendarIntervalYearly):
		return t.AddDate(1, 0, 0)
	case string(DateCalendarIntervalMonthly):
		return t.AddDate(0, 1, 0)
	case string(DateCalendarIntervalDaily):
		return t.AddDate(0, 0, 1)
	case string(DateFixedIntervalDaily):
		return t.AddDate(0, 0, 1)
	case string(DateFixedIntervalHours):
		return t.Add(time.Hour)
	case string(DateFixedIntervalMinutes):
		return t.Add(time.Minute)
	case string(DateFixedIntervalSeconds):
		return t.Add(time.Second)
	case string(DateFixedIntervalMilliseconds):
		return t.Add(time.Millisecond)
	}

	return t
}

func ParseTimeFrom(d string, interval string) (time.Time, error) {
	format := getDateTimeFormat(interval)
	return time.Parse(format, d)
}

func getDateTimeFormat(interval string) string {
	switch interval {
	case string(DateCalendarIntervalYearly):
		return "2006"
	case string(DateCalendarIntervalMonthly):
		return "2006-01"
	case string(DateCalendarIntervalDaily):
		return "2006-01-02"
	case string(DateFixedIntervalDaily):
		return "2006-01-02"
	case string(DateFixedIntervalHours):
		return "2006-01-02 15"
	case string(DateFixedIntervalMinutes):
		return "2006-01-02 15:04"
	case string(DateFixedIntervalSeconds):
		return "2006-01-02 15:04:05"
	case string(DateFixedIntervalMilliseconds):
		return "2006-01-02 15:04:05.000"
	}

	return time.RFC3339
}

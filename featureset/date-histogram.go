package featureset

import (
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/calendarinterval"
	"github.com/reveald/reveald"
)

// DateHistogramInterval specifies the date interval size
type DateHistogramInterval string

// Common intervals
const (
	Second     DateHistogramInterval = "second"
	Minute     DateHistogramInterval = "minute"
	Hour       DateHistogramInterval = "hour"
	Day        DateHistogramInterval = "day"
	Week       DateHistogramInterval = "week"
	Month      DateHistogramInterval = "month"
	Quarter    DateHistogramInterval = "quarter"
	Year       DateHistogramInterval = "year"
	Minute5    DateHistogramInterval = "5m"
	Minute10   DateHistogramInterval = "10m"
	Minute30   DateHistogramInterval = "30m"
	MinuteHalf DateHistogramInterval = "30m"
	Hour12     DateHistogramInterval = "12h"
)

// DateHistogramFeature creates a date histogram aggregation for date fields.
//
// It groups documents based on date field values into buckets and
// can also apply date range filters based on request parameters.
//
// Example:
//
//	// Create a basic date histogram feature for the "created_at" field
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd")
//
//	// Use the date histogram feature in a feature chain
//	result, err := dateHistogram.Process(builder, nextFeature)
type DateHistogramFeature struct {
	property                string
	interval                DateHistogramInterval
	format                  string
	minDocCount             int64
	lowerThreshold          *time.Time
	upperThreshold          *time.Time
	extendedBounds          bool
	keepDescendingOrder     bool
	defaultUpperThreshold   *time.Time
	defaultLowerThreshold   *time.Time
	timezone                string
	calendarIntervalInstead bool
	zerobucket              bool
}

// DateHistogramOption is a functional option for configuring a DateHistogramFeature.
type DateHistogramOption func(*DateHistogramFeature)

// WithDateFormat defines the date format used when returning
// the date buckets in an aggregation
func WithDateFormat(format string) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.format = format
	}
}

// WithDateTimeZone sets a time zone for the date histogram.
//
// Example:
//
//	// Create a date histogram using UTC time zone
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithDateTimeZone("UTC"),
//	)
func WithDateTimeZone(timezone string) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.timezone = timezone
	}
}

// WithMinDateDocumentCount specifies how many documents must match
// a date range for it to be included in the search result.
//
// Buckets with fewer documents than this threshold will be excluded from the results.
//
// Example:
//
//	// Create a date histogram that only includes buckets with at least 5 documents
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithMinDateDocumentCount(5),
//	)
func WithMinDateDocumentCount(minDocCount int64) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.minDocCount = minDocCount
	}
}

// WithMinimumDate specifies a minimum date threshold for
// a property to use in a search
func WithMinimumDate(t *time.Time) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.defaultLowerThreshold = t
	}
}

// WithMaximumDate specifies a maximum date threshold for
// a property to use in a search
func WithMaximumDate(t *time.Time) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.defaultUpperThreshold = t
	}
}

// WithExtendedBounds generates empty buckets for a date range
// even if no value exists in the range.
//
// Extended bounds ensure that buckets are created for the entire range,
// even if there are no documents in some buckets.
//
// Example:
//
//	// Create a date histogram with extended bounds
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithExtendedBounds(),
//	    featureset.WithDefaultBounds("2020-01-01", "2020-12-31"),
//	)
func WithExtendedBounds() DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.extendedBounds = true
	}
}

// WithDescendingOrder specifies that the bucket order starts with
// the newest date, rather than the oldest
func WithDescendingOrder() DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.keepDescendingOrder = true
	}
}

// WithCalendarIntervalInstead specifies that we use "calendar_interval" instead of "interval" field
func WithCalendarIntervalInstead() DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.calendarIntervalInstead = true
	}
}

// WithCalendarInterval sets the calendar interval for the date histogram.
//
// Calendar intervals are specified as 'year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', or 'second'.
//
// Example:
//
//	// Create a date histogram with monthly buckets
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithCalendarInterval("month"),
//	)
func WithCalendarInterval(interval string) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.interval = DateHistogramInterval(interval)
	}
}

// WithFixedInterval sets the fixed interval for the date histogram.
//
// Fixed intervals are specified as a number followed by a time unit, e.g., '1h', '1d', '7d'.
//
// Example:
//
//	// Create a date histogram with 7-day buckets
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithFixedInterval("7d"),
//	)
func WithFixedInterval(interval string) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.interval = DateHistogramInterval(interval)
	}
}

// WithDefaultBounds sets the default lower and upper bounds for the date histogram.
//
// These bounds are used when extended bounds are enabled and no range parameters are provided.
//
// Example:
//
//	// Create a date histogram with default bounds for the entire year 2020
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithExtendedBounds(),
//	    featureset.WithDefaultBounds("2020-01-01", "2020-12-31"),
//	)
func WithDefaultBounds(lower, upper string) DateHistogramOption {
	return func(dhf *DateHistogramFeature) {
		dhf.defaultLowerThreshold = &time.Time{}
		dhf.defaultUpperThreshold = &time.Time{}
		dhf.lowerThreshold = &time.Time{}
		dhf.upperThreshold = &time.Time{}

		if err := dhf.lowerThreshold.UnmarshalText([]byte(lower)); err != nil {
			// If there's an error, we'll just use the zero time
			dhf.lowerThreshold = &time.Time{}
		}

		if err := dhf.upperThreshold.UnmarshalText([]byte(upper)); err != nil {
			// If there's an error, we'll just use the zero time
			dhf.upperThreshold = &time.Time{}
		}
	}
}

// NewDateHistogramFeature creates a new date histogram feature for the specified property and format.
//
// Example:
//
//	// Create a date histogram with custom settings
//	dateHistogram := featureset.NewDateHistogramFeature("created_at", "yyyy-MM-dd",
//	    featureset.WithCalendarInterval("month"),
//	    featureset.WithDateTimeZone("UTC"),
//	    featureset.WithMinDateDocumentCount(5),
//	)
func NewDateHistogramFeature(property string, interval DateHistogramInterval, opts ...DateHistogramOption) *DateHistogramFeature {
	dhf := &DateHistogramFeature{
		property:                property,
		interval:                interval,
		format:                  "yyyy-MM-dd HH:mm:ss",
		minDocCount:             0,
		extendedBounds:          false,
		keepDescendingOrder:     false,
		calendarIntervalInstead: false,
		defaultLowerThreshold:   nil,
		defaultUpperThreshold:   nil,
		timezone:                "",
	}

	for _, opt := range opts {
		opt(dhf)
	}

	return dhf
}

// Process applies the date histogram aggregation to the query builder and processes the result.
//
// It adds a date histogram aggregation to the query and processes any date range filters
// from the request parameters.
//
// Example:
//
//	// Use the date histogram feature in a feature chain
//	result, err := dateHistogram.Process(builder, nextFeature)
func (dhf *DateHistogramFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	dhf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return dhf.handle(r)
}

func (dhf *DateHistogramFeature) build(builder *reveald.QueryBuilder) {
	dhf.lowerThreshold = dhf.defaultLowerThreshold
	dhf.upperThreshold = dhf.defaultUpperThreshold

	if builder.Request().Has(dhf.property) {
		p, err := builder.Request().Get(dhf.property)
		if err == nil && p.IsRangeValue() {
			// Create a date range query directly with typed objects
			var dateRangeQuery types.DateRangeQuery

			max, wmax := p.Max()
			if wmax {
				// Convert float64 to string for date range
				dateMax := fmt.Sprintf("%v", max)
				dateRangeQuery.Lte = &dateMax
			}

			min, wmin := p.Min()
			if wmin {
				// Convert float64 to string for date range
				dateMin := fmt.Sprintf("%v", min)
				dateRangeQuery.Gte = &dateMin
			}

			// Create the full range query
			rangeQuery := types.Query{
				Range: map[string]types.RangeQuery{
					dhf.property: &dateRangeQuery,
				},
			}

			builder.With(rangeQuery)
		}
	}

	// Create a date histogram aggregation directly with typed objects
	field := dhf.property
	format := dhf.format
	minDocCount := int(dhf.minDocCount)

	// Create the base date histogram aggregation
	dateHistogramAgg := types.Aggregations{}

	// Initialize the appropriate date histogram type
	if dhf.calendarIntervalInstead {
		// Create a CalendarInterval with the name from the interval string
		calendarIntervalObj := &calendarinterval.CalendarInterval{Name: string(dhf.interval)}
		dateHistogramAgg.DateHistogram = &types.DateHistogramAggregation{
			CalendarInterval: calendarIntervalObj,
			Field:            &field,
			Format:           &format,
		}

		if dhf.minDocCount > 0 {
			dateHistogramAgg.DateHistogram.MinDocCount = &minDocCount
		}

		if dhf.timezone != "" {
			dateHistogramAgg.DateHistogram.TimeZone = &dhf.timezone
		}
	} else {
		fixedInterval := string(dhf.interval)
		dateHistogramAgg.DateHistogram = &types.DateHistogramAggregation{
			Interval: fixedInterval,
			Field:    &field,
			Format:   &format,
		}

		if dhf.minDocCount > 0 {
			dateHistogramAgg.DateHistogram.MinDocCount = &minDocCount
		}

		if dhf.timezone != "" {
			dateHistogramAgg.DateHistogram.TimeZone = &dhf.timezone
		}
	}

	// Add extended bounds if needed
	if dhf.extendedBounds && (dhf.lowerThreshold != nil || dhf.upperThreshold != nil) {
		extendedBounds := &types.ExtendedBoundsFieldDateMath{}

		if dhf.lowerThreshold != nil {
			// Format the time as a string and use as Min
			min := dhf.lowerThreshold.Format(time.RFC3339)
			extendedBounds.Min = min
		}

		if dhf.upperThreshold != nil {
			// Format the time as a string and use as Max
			max := dhf.upperThreshold.Format(time.RFC3339)
			extendedBounds.Max = max
		}

		dateHistogramAgg.DateHistogram.ExtendedBounds = extendedBounds
	}

	builder.Aggregation(dhf.property, dateHistogramAgg)
}

func (dhf *DateHistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawAggregations()[dhf.property]
	if !ok {
		return result, nil
	}

	histogram, ok := agg.(types.DateHistogramAggregate)
	if !ok {
		return result, nil
	}

	buckets, ok := histogram.Buckets.([]types.DateHistogramBucket)
	if !ok {
		return result, nil
	}

	var resultBuckets []*reveald.ResultBucket
	for _, bucket := range buckets {
		if bucket.DocCount == 0 && !dhf.zerobucket {
			continue
		}
		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	result.Aggregations[dhf.property] = resultBuckets
	return result, nil
}

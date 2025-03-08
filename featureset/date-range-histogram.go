package featureset

import (
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald"
)

// DateRangeHistogramFeature is a feature that groups date values into specified ranges
type DateRangeHistogramFeature struct {
	property     string
	format       string
	ranges       []DateRange
	keyed        bool
	minDocCount  int64
	timezone     string
	nestedPath   string
	nestedFilter map[string]any
}

// DateRange defines a range between two dates
type DateRange struct {
	Key       string
	From      *time.Time
	To        *time.Time
	FromStr   string
	ToStr     string
	DocCount  int64
	Timestamp int64
}

// DateRangeHistogramOption configures a date range histogram
type DateRangeHistogramOption func(*DateRangeHistogramFeature)

// WithRanges defines the date ranges to use for grouping
func WithRanges(ranges []DateRange) DateRangeHistogramOption {
	return func(drhf *DateRangeHistogramFeature) {
		drhf.ranges = ranges
	}
}

// WithKeyed specifies whether to generate a hash of buckets
// rather than an array
func WithKeyed(keyed bool) DateRangeHistogramOption {
	return func(drhf *DateRangeHistogramFeature) {
		drhf.keyed = keyed
	}
}

// WithTimeZone specifies the time zone to use for the buckets
func WithTimeZone(tz string) DateRangeHistogramOption {
	return func(drhf *DateRangeHistogramFeature) {
		drhf.timezone = tz
	}
}

// WithNestedField specifies that the date field is inside a nested document
func WithNestedField(path string, filter map[string]any) DateRangeHistogramOption {
	return func(drhf *DateRangeHistogramFeature) {
		drhf.nestedPath = path
		drhf.nestedFilter = filter
	}
}

// NewDateRangeHistogramFeature creates a feature for grouping dates into
// discrete ranges
func NewDateRangeHistogramFeature(property, format string, opts ...DateRangeHistogramOption) *DateRangeHistogramFeature {
	drhf := &DateRangeHistogramFeature{
		property:    property,
		format:      format,
		ranges:      []DateRange{},
		keyed:       false,
		minDocCount: 0,
	}

	for _, opt := range opts {
		opt(drhf)
	}

	return drhf
}

// Process implements the Feature interface
func (drhf *DateRangeHistogramFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	drhf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return drhf.handle(r)
}

func (drhf *DateRangeHistogramFeature) build(builder *reveald.QueryBuilder) {
	// Create the date range aggregation directly with typed objects
	field := drhf.property
	format := drhf.format
	keyed := drhf.keyed

	// Create array of date ranges
	dateRanges := make([]types.DateRangeExpression, 0, len(drhf.ranges))
	for _, r := range drhf.ranges {
		dateRange := types.DateRangeExpression{}

		if r.Key != "" {
			dateRange.Key = &r.Key
		}

		if r.From != nil {
			fromStr := r.From.Format(time.RFC3339)
			dateRange.From = fromStr
		} else if r.FromStr != "" {
			dateRange.From = r.FromStr
		}

		if r.To != nil {
			toStr := r.To.Format(time.RFC3339)
			dateRange.To = toStr
		} else if r.ToStr != "" {
			dateRange.To = r.ToStr
		}

		dateRanges = append(dateRanges, dateRange)
	}

	// Create the date range aggregation
	dateRangeAgg := types.Aggregations{
		DateRange: &types.DateRangeAggregation{
			Field:  &field,
			Format: &format,
			Keyed:  &keyed,
			Ranges: dateRanges,
		},
	}

	// Add timezone if specified
	if drhf.timezone != "" {
		dateRangeAgg.DateRange.TimeZone = &drhf.timezone
	}

	// If this is a nested field, wrap it in a nested aggregation
	if drhf.nestedPath != "" {
		path := drhf.nestedPath

		// Create the nested aggregation
		nestedAgg := types.Aggregations{
			Nested: &types.NestedAggregation{
				Path: &path,
			},
			Aggregations: map[string]types.Aggregations{
				drhf.property: dateRangeAgg,
			},
		}

		// Add filter if provided (would need to be converted to typed filter)
		if drhf.nestedFilter != nil {
			// Note: This would require converting the map[string]any filter
			// to a typed Query structure. For now, we'll leave this commented out
			// as it needs custom handling based on the specific filter types used.
			// nestedAgg.Nested.Filter = ...
		}

		builder.Aggregation(drhf.property, nestedAgg)
	} else {
		builder.Aggregation(drhf.property, dateRangeAgg)
	}
}

func (drhf *DateRangeHistogramFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	// Buckets are already processed in the Result struct
	return result, nil
}

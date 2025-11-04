package featureset

const defaultAggregationSize = 10

type AggregationFeature struct {
	size         int
	missingValue string
}

type AggregationOption func(*AggregationFeature)

func WithAggregationSize(size int) AggregationOption {
	return func(af *AggregationFeature) {
		af.size = size
	}
}

// WithMissingValueAs configures filter features to include missing values in aggregations
// and allow filtering by a custom label.
//
// When called with a label, the feature will:
//   - Add a missing aggregation alongside the main terms aggregation
//   - Merge the missing count into the aggregation results with the provided label
//   - Support filtering by the label via request parameters (e.g., ?category=no-category)
//   - Match documents where the field is either completely missing OR explicitly null
//
// What counts as "missing":
//   - Field completely absent from the document
//   - Field with a null value
//   - Empty array []
//   - Array containing only nulls [null]
//
// What does NOT count as "missing":
//   - Empty string "" (this is a valid value)
//   - Zero 0
//   - False false
//
// Parameters:
//   - value: The label to use for missing values in aggregations and filters
//
// Returns:
//   An AggregationOption that can be passed to any filter feature constructor
//
// Example:
//
//	// Create a category filter with missing value support using custom label
//	categoryFilter := featureset.NewDynamicFilterFeature(
//	    "category",
//	    featureset.WithAggregationSize(20),
//	    featureset.WithMissingValueAs("no-category"),  // Custom label
//	)
//
//	// Aggregation results will include custom label bucket:
//	// Electronics (45), Clothing (30), Books (10), no-category (15)
//
//	// Users can filter by custom label: ?category=no-category
//	// Or combine: ?category=Electronics,no-category
func WithMissingValueAs(value string) AggregationOption {
	return func(af *AggregationFeature) {
		af.missingValue = value
	}
}

func buildAggregationFeature(opts ...AggregationOption) AggregationFeature {
	agg := AggregationFeature{
		size: defaultAggregationSize,
	}

	for _, opt := range opts {
		opt(&agg)
	}

	return agg
}

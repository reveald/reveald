package featureset

import (
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/calendarinterval"
	"github.com/reveald/reveald/v2"
)

// NestedDocumentWrapper wraps a set of features to support nested document queries.
// It automatically detects DynamicFilterFeature, HistogramFeature, and DateHistogramFeature
// children and wraps their queries and aggregations in nested structures.
//
// Example:
//
//	wrapper := featureset.NewNestedDocumentWrapper("items",
//	    featureset.NewDynamicFilterFeature("items.category"),
//	    featureset.NewDynamicFilterFeature("items.tags"),
//	)
//
// This will generate nested queries and aggregations that properly handle
// the nested document structure.
type NestedDocumentWrapper struct {
	path        string
	features    []reveald.Feature
	disjunctive bool
}

// NewNestedDocumentWrapper creates a new nested document wrapper for the specified path.
//
// The path parameter should be the nested field path (e.g., "items").
// The features parameter should be a list of features that operate on nested fields.
//
// Example:
//
//	wrapper := featureset.NewNestedDocumentWrapper("items",
//	    featureset.NewDynamicFilterFeature("items.category"),
//	    featureset.NewDynamicFilterFeature("items.tags"),
//	)
func NewNestedDocumentWrapper(path string, features ...reveald.Feature) *NestedDocumentWrapper {
	return &NestedDocumentWrapper{
		path:        path,
		features:    features,
		disjunctive: false,
	}
}

// Disjunctive enables disjunctive (OR) mode for faceted search aggregations.
//
// # Understanding Conjunctive vs Disjunctive Filtering
//
// Conjunctive Mode (default, Disjunctive=false):
//   - Hit queries: All filters combined with AND logic
//   - Aggregations: Each aggregation filtered by ALL active filters (including its own)
//   - Result: As you select more options, available choices narrow down progressively
//   - Use case: When you want to drill down and find items matching ALL selected criteria
//
// Disjunctive Mode (Disjunctive=true):
//   - Hit queries: All filters still combined with AND logic (for accurate results)
//   - Aggregations: Each facet's aggregation excludes its own filter but includes others
//   - Result: You can always see all available options for each facet independently
//   - Use case: When you want users to explore different combinations without losing visibility
//
// # Example Scenario
//
// Given nested items with category and tags fields, suppose the data contains:
//   - Item A: category="Electronics", tags="New"
//   - Item B: category="Electronics", tags="Sale"
//   - Item C: category="Books", tags="New"
//
// User selects: category="Electronics" AND tags="New"
//
// Conjunctive Mode Output:
//
//	Hits: [Item A] (only items matching both filters)
//	Category aggregation: {Electronics: 1}     // Only shows categories for filtered items
//	Tags aggregation: {New: 1}                 // Only shows tags for filtered items
//
// Disjunctive Mode Output:
//
//	Hits: [Item A] (only items matching both filters)
//	Category aggregation: {Electronics: 2, Books: 1}  // Shows all categories (tags filter excluded)
//	Tags aggregation: {New: 2, Sale: 1}              // Shows all tags (category filter excluded)
//
// The key difference: In disjunctive mode, users can see what OTHER options are available
// for each facet, even when filters are active. This prevents "dead ends" where applying
// too many filters results in empty aggregation buckets.
//
// Example:
//
//	// Conjunctive mode (default) - narrow down progressively
//	wrapper := featureset.NewNestedDocumentWrapper("items",
//	    featureset.NewDynamicFilterFeature("items.category"),
//	    featureset.NewDynamicFilterFeature("items.tags"),
//	)
//
//	// Disjunctive mode - explore freely
//	wrapper := featureset.NewNestedDocumentWrapper("items",
//	    featureset.NewDynamicFilterFeature("items.category"),
//	    featureset.NewDynamicFilterFeature("items.tags"),
//	).Disjunctive(true)
func (ndw *NestedDocumentWrapper) Disjunctive(enable bool) *NestedDocumentWrapper {
	ndw.disjunctive = enable
	return ndw
}

// Process implements the Feature interface.
// It wraps child features and transforms their queries/aggregations to work with nested documents.
func (ndw *NestedDocumentWrapper) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	// Step 1: Build nested query filters (for hit filtering)
	ndw.buildNestedQueryFilter(builder)

	// Step 2: Build nested aggregations (for faceting)
	ndw.buildNestedAggregations(builder)

	// Step 3: Continue the feature chain
	res, err := next(builder)
	if err != nil {
		return nil, err
	}

	// Step 4: Handle aggregation results
	return ndw.handleAggregations(res)
}

// buildNestedQueryFilter creates a single nested query that combines all child feature filters.
func (ndw *NestedDocumentWrapper) buildNestedQueryFilter(builder *reveald.QueryBuilder) {
	var must []types.Query

	// Collect filter clauses from each child feature
	for _, feature := range ndw.features {
		switch f := feature.(type) {
		case *DynamicFilterFeature:
			if q := ndw.buildDynamicFilterClause(f, builder); q != nil {
				must = append(must, *q)
			}
		case *HistogramFeature:
			if q := ndw.buildHistogramFilterClause(f, builder); q != nil {
				must = append(must, *q)
			}
		case *DateHistogramFeature:
			if q := ndw.buildDateHistogramFilterClause(f, builder); q != nil {
				must = append(must, *q)
			}
		}
	}

	if len(must) == 0 {
		return
	}

	// Wrap all filters in a single nested query
	nestedQuery := types.Query{
		Nested: &types.NestedQuery{
			Path: ndw.path,
			Query: types.Query{
				Bool: &types.BoolQuery{Must: must},
			},
		},
	}

	builder.With(nestedQuery)
}

// buildDynamicFilterClause builds a filter clause for a DynamicFilterFeature.
func (ndw *NestedDocumentWrapper) buildDynamicFilterClause(f *DynamicFilterFeature, builder *reveald.QueryBuilder) *types.Query {
	if !builder.Request().Has(f.property) {
		return nil
	}

	p, err := builder.Request().Get(f.property)
	if err != nil {
		return nil
	}

	keyword := fmt.Sprintf("%s.keyword", f.property)
	shouldClauses := make([]types.Query, 0, len(p.Values()))

	for _, v := range p.Values() {
		if f.agg.missingValue != "" && v == f.agg.missingValue {
			// Handle missing values
			missingQuery := types.Query{
				Bool: &types.BoolQuery{
					MustNot: []types.Query{
						{Exists: &types.ExistsQuery{Field: keyword}},
					},
				},
			}
			shouldClauses = append(shouldClauses, missingQuery)
		} else {
			termQuery := types.Query{
				Term: map[string]types.TermQuery{
					keyword: {Value: v},
				},
			}
			shouldClauses = append(shouldClauses, termQuery)
		}
	}

	if len(shouldClauses) == 0 {
		return nil
	}

	if len(shouldClauses) == 1 {
		return &shouldClauses[0]
	}

	return &types.Query{
		Bool: &types.BoolQuery{
			Should:             shouldClauses,
			MinimumShouldMatch: 1,
		},
	}
}

// buildHistogramFilterClause builds a filter clause for a HistogramFeature.
func (ndw *NestedDocumentWrapper) buildHistogramFilterClause(f *HistogramFeature, builder *reveald.QueryBuilder) *types.Query {
	p, err := builder.Request().Get(f.property)
	if err != nil || !p.IsRangeValue() {
		return nil
	}

	var numRangeQuery types.NumberRangeQuery

	maxVal, hasMax := p.Max()
	if hasMax && (maxVal >= 0 || f.neg) {
		lteValue := types.Float64(maxVal)
		numRangeQuery.Lte = &lteValue
	}

	minVal, hasMin := p.Min()
	if hasMin && (!hasMax || minVal <= maxVal) && (minVal >= 0 || f.neg) {
		gteValue := types.Float64(minVal)
		numRangeQuery.Gte = &gteValue
	}

	if numRangeQuery.Lte == nil && numRangeQuery.Gte == nil {
		return nil
	}

	return &types.Query{
		Range: map[string]types.RangeQuery{
			f.property: &numRangeQuery,
		},
	}
}

// buildDateHistogramFilterClause builds a filter clause for a DateHistogramFeature.
func (ndw *NestedDocumentWrapper) buildDateHistogramFilterClause(f *DateHistogramFeature, builder *reveald.QueryBuilder) *types.Query {
	if !builder.Request().Has(f.property) {
		return nil
	}

	p, err := builder.Request().Get(f.property)
	if err != nil || !p.IsRangeValue() {
		return nil
	}

	var dateRangeQuery types.DateRangeQuery

	max, wmax := p.Max()
	if wmax {
		dateMax := fmt.Sprintf("%v", max)
		dateRangeQuery.Lte = &dateMax
	}

	min, wmin := p.Min()
	if wmin {
		dateMin := fmt.Sprintf("%v", min)
		dateRangeQuery.Gte = &dateMin
	}

	if dateRangeQuery.Lte == nil && dateRangeQuery.Gte == nil {
		return nil
	}

	return &types.Query{
		Range: map[string]types.RangeQuery{
			f.property: &dateRangeQuery,
		},
	}
}

// buildNestedAggregations creates nested aggregations for each child feature.
func (ndw *NestedDocumentWrapper) buildNestedAggregations(builder *reveald.QueryBuilder) {
	// Collect all filter clauses by property
	allClauses := make([]types.Query, 0, len(ndw.features))
	perProperty := make(map[string]*types.Query, len(ndw.features))

	for _, feature := range ndw.features {
		var property string
		var clause *types.Query

		switch f := feature.(type) {
		case *DynamicFilterFeature:
			property = f.property
			clause = ndw.buildDynamicFilterClause(f, builder)
		case *HistogramFeature:
			property = f.property
			clause = ndw.buildHistogramFilterClause(f, builder)
		case *DateHistogramFeature:
			property = f.property
			clause = ndw.buildDateHistogramFilterClause(f, builder)
		}

		if clause != nil {
			perProperty[property] = clause
			allClauses = append(allClauses, *clause)
		}
	}

	// Build aggregation for each feature
	for _, feature := range ndw.features {
		switch f := feature.(type) {
		case *DynamicFilterFeature:
			ndw.buildDynamicFilterAggregation(f, builder, allClauses, perProperty)
		case *HistogramFeature:
			ndw.buildHistogramAggregation(f, builder, allClauses, perProperty)
		case *DateHistogramFeature:
			ndw.buildDateHistogramAggregation(f, builder, allClauses, perProperty)
		}
	}
}

// buildDynamicFilterAggregation creates a nested aggregation for a DynamicFilterFeature.
func (ndw *NestedDocumentWrapper) buildDynamicFilterAggregation(
	f *DynamicFilterFeature,
	builder *reveald.QueryBuilder,
	allClauses []types.Query,
	perProperty map[string]*types.Query,
) {
	// Build filter must clauses (conjunctive vs disjunctive)
	filterMust := ndw.buildFilterMustClauses(f.property, allClauses, perProperty)

	// Create the inner terms aggregation
	keyword := fmt.Sprintf("%s.keyword", f.property)
	field := keyword
	size := f.agg.size

	termsAgg := &types.TermsAggregation{
		Field: &field,
		Size:  &size,
	}

	if f.agg.missingValue != "" {
		termsAgg.Missing = types.Missing(f.agg.missingValue)
	}

	// Build: nested -> filter -> terms
	nestedPath := ndw.path
	builder.Aggregation(f.property, types.Aggregations{
		Nested: &types.NestedAggregation{Path: &nestedPath},
		Aggregations: map[string]types.Aggregations{
			f.property + "._filter": {
				Filter: &types.Query{Bool: &types.BoolQuery{Must: filterMust}},
				Aggregations: map[string]types.Aggregations{
					f.property: {Terms: termsAgg},
				},
			},
		},
	})
}

// buildHistogramAggregation creates a nested aggregation for a HistogramFeature.
func (ndw *NestedDocumentWrapper) buildHistogramAggregation(
	f *HistogramFeature,
	builder *reveald.QueryBuilder,
	allClauses []types.Query,
	perProperty map[string]*types.Query,
) {
	// Build filter must clauses (conjunctive vs disjunctive)
	filterMust := ndw.buildFilterMustClauses(f.property, allClauses, perProperty)

	// Create the inner histogram aggregation
	field := f.property
	interval := types.Float64(float64(f.interval))
	minDocCount := int(f.minDocCount)

	histAgg := &types.HistogramAggregation{
		Field:       &field,
		Interval:    &interval,
		MinDocCount: &minDocCount,
	}

	// Build: nested -> filter -> histogram
	nestedPath := ndw.path
	builder.Aggregation(f.property, types.Aggregations{
		Nested: &types.NestedAggregation{Path: &nestedPath},
		Aggregations: map[string]types.Aggregations{
			f.property + "._filter": {
				Filter: &types.Query{Bool: &types.BoolQuery{Must: filterMust}},
				Aggregations: map[string]types.Aggregations{
					f.property: {Histogram: histAgg},
				},
			},
		},
	})
}

// buildDateHistogramAggregation creates a nested aggregation for a DateHistogramFeature.
func (ndw *NestedDocumentWrapper) buildDateHistogramAggregation(
	f *DateHistogramFeature,
	builder *reveald.QueryBuilder,
	allClauses []types.Query,
	perProperty map[string]*types.Query,
) {
	// Build filter must clauses (conjunctive vs disjunctive)
	filterMust := ndw.buildFilterMustClauses(f.property, allClauses, perProperty)

	// Create the inner date histogram aggregation
	field := f.property
	format := f.format
	minDocCount := int(f.minDocCount)

	dateHistAgg := &types.DateHistogramAggregation{
		Field:  &field,
		Format: &format,
	}

	if f.calendarIntervalInstead {
		// Use calendar interval
		dateHistAgg.CalendarInterval = &calendarinterval.CalendarInterval{Name: string(f.interval)}
	} else {
		// Use fixed interval
		dateHistAgg.Interval = string(f.interval)
	}

	if f.minDocCount > 0 {
		dateHistAgg.MinDocCount = &minDocCount
	}

	if f.timezone != "" {
		dateHistAgg.TimeZone = &f.timezone
	}

	// Add extended bounds if needed
	if f.extendedBounds && (f.lowerThreshold != nil || f.upperThreshold != nil) {
		extendedBounds := &types.ExtendedBoundsFieldDateMath{}
		if f.lowerThreshold != nil {
			min := f.lowerThreshold.Format("2006-01-02T15:04:05Z07:00")
			extendedBounds.Min = min
		}
		if f.upperThreshold != nil {
			max := f.upperThreshold.Format("2006-01-02T15:04:05Z07:00")
			extendedBounds.Max = max
		}
		dateHistAgg.ExtendedBounds = extendedBounds
	}

	// Build: nested -> filter -> date_histogram
	nestedPath := ndw.path
	builder.Aggregation(f.property, types.Aggregations{
		Nested: &types.NestedAggregation{Path: &nestedPath},
		Aggregations: map[string]types.Aggregations{
			f.property + "._filter": {
				Filter: &types.Query{Bool: &types.BoolQuery{Must: filterMust}},
				Aggregations: map[string]types.Aggregations{
					f.property: {DateHistogram: dateHistAgg},
				},
			},
		},
	})
}

// buildFilterMustClauses builds the filter must clauses for conjunctive or disjunctive mode.
func (ndw *NestedDocumentWrapper) buildFilterMustClauses(
	property string,
	allClauses []types.Query,
	perProperty map[string]*types.Query,
) []types.Query {
	if ndw.disjunctive {
		// Disjunctive: exclude this property's filter
		filterMust := make([]types.Query, 0, len(allClauses))
		for prop, clause := range perProperty {
			if prop != property && clause != nil {
				filterMust = append(filterMust, *clause)
			}
		}
		return filterMust
	}

	// Conjunctive: include all filters
	return append([]types.Query{}, allClauses...)
}

// handleAggregations processes the nested aggregation results.
func (ndw *NestedDocumentWrapper) handleAggregations(res *reveald.Result) (*reveald.Result, error) {
	for _, feature := range ndw.features {
		var err error
		switch f := feature.(type) {
		case *DynamicFilterFeature:
			err = ndw.handleDynamicFilterResult(f, res)
		case *HistogramFeature:
			err = ndw.handleHistogramResult(f, res)
		case *DateHistogramFeature:
			err = ndw.handleDateHistogramResult(f, res)
		}
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// handleDynamicFilterResult extracts and processes the nested aggregation result for a DynamicFilterFeature.
func (ndw *NestedDocumentWrapper) handleDynamicFilterResult(f *DynamicFilterFeature, res *reveald.Result) error {
	// Navigate: raw[property] -> nested -> filter -> terms
	raw, ok := res.RawAggregations()[f.property]
	if !ok || raw == nil {
		return nil
	}

	nestedAgg, ok := raw.(*types.NestedAggregate)
	if !ok || nestedAgg == nil {
		return nil
	}

	filterNode, ok := nestedAgg.Aggregations[f.property+"._filter"]
	if !ok || filterNode == nil {
		return nil
	}

	filterAgg, ok := filterNode.(*types.FilterAggregate)
	if !ok || filterAgg == nil {
		return nil
	}

	inner, ok := filterAgg.Aggregations[f.property]
	if !ok || inner == nil {
		return nil
	}

	terms, ok := inner.(*types.StringTermsAggregate)
	if !ok {
		return nil
	}

	buckets := terms.Buckets.([]types.StringTermsBucket)
	var resultBuckets []*reveald.ResultBucket
	for _, bucket := range buckets {
		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	res.Aggregations[f.property] = resultBuckets
	return nil
}

// handleHistogramResult extracts and processes the nested aggregation result for a HistogramFeature.
func (ndw *NestedDocumentWrapper) handleHistogramResult(f *HistogramFeature, res *reveald.Result) error {
	// Navigate: raw[property] -> nested -> filter -> histogram
	raw, ok := res.RawAggregations()[f.property]
	if !ok || raw == nil {
		return nil
	}

	nestedAgg, ok := raw.(*types.NestedAggregate)
	if !ok || nestedAgg == nil {
		return nil
	}

	filterNode, ok := nestedAgg.Aggregations[f.property+"._filter"]
	if !ok || filterNode == nil {
		return nil
	}

	filterAgg, ok := filterNode.(*types.FilterAggregate)
	if !ok || filterAgg == nil {
		return nil
	}

	inner, ok := filterAgg.Aggregations[f.property]
	if !ok || inner == nil {
		return nil
	}

	histogram, ok := inner.(*types.HistogramAggregate)
	if !ok {
		return nil
	}

	buckets, ok := histogram.Buckets.([]types.HistogramBucket)
	if !ok {
		return nil
	}

	var resultBuckets []*reveald.ResultBucket
	zeroOut := len(buckets) > 0

	for _, bucket := range buckets {
		if bucket.Key <= 0 {
			zeroOut = false
		}

		if bucket.Key == 0 && !f.zeroBucket && bucket.DocCount == 0 {
			continue
		}

		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	// Add zero bucket if needed
	if f.zeroBucket && zeroOut {
		bucket := &reveald.ResultBucket{
			Value:    0,
			HitCount: 0,
		}
		resultBuckets = append(resultBuckets, nil)
		copy(resultBuckets[1:], resultBuckets)
		resultBuckets[0] = bucket
	}

	res.Aggregations[f.property] = resultBuckets
	return nil
}

// handleDateHistogramResult extracts and processes the nested aggregation result for a DateHistogramFeature.
func (ndw *NestedDocumentWrapper) handleDateHistogramResult(f *DateHistogramFeature, res *reveald.Result) error {
	// Navigate: raw[property] -> nested -> filter -> date_histogram
	raw, ok := res.RawAggregations()[f.property]
	if !ok || raw == nil {
		return nil
	}

	nestedAgg, ok := raw.(*types.NestedAggregate)
	if !ok || nestedAgg == nil {
		return nil
	}

	filterNode, ok := nestedAgg.Aggregations[f.property+"._filter"]
	if !ok || filterNode == nil {
		return nil
	}

	filterAgg, ok := filterNode.(*types.FilterAggregate)
	if !ok || filterAgg == nil {
		return nil
	}

	inner, ok := filterAgg.Aggregations[f.property]
	if !ok || inner == nil {
		return nil
	}

	histogram, ok := inner.(types.DateHistogramAggregate)
	if !ok {
		return nil
	}

	buckets, ok := histogram.Buckets.([]types.DateHistogramBucket)
	if !ok {
		return nil
	}

	var resultBuckets []*reveald.ResultBucket
	for _, bucket := range buckets {
		if bucket.DocCount == 0 && !f.zerobucket {
			continue
		}
		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	res.Aggregations[f.property] = resultBuckets
	return nil
}

// Property returns the nested path for this wrapper.
func (ndw *NestedDocumentWrapper) Property() string {
	return ndw.path
}

// Features returns the child features wrapped by this wrapper.
func (ndw *NestedDocumentWrapper) Features() []reveald.Feature {
	return ndw.features
}

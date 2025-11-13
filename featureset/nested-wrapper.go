package featureset

import (
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
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
	innerQueryBuilder := reveald.NewQueryBuilder(builder.Request(), builder.Indices()...)
	for _, feature := range ndw.features {
		_, err := feature.Process(innerQueryBuilder, func(b *reveald.QueryBuilder) (*reveald.Result, error) {

			// return nil here to continue the chain
			// we only do this to build up the inner query/aggregations
			// then we will wrap them in nested structures
			// and then we will run them again and feed them with their data
			fmt.Println(b)

			return nil, nil
		})
		if err != nil {
			return nil, err
		}
	}

	ndw.wrapAndApplyToMainBuilder(innerQueryBuilder.BuildRequest(), builder)

	res, err := next(builder)
	if err != nil {
		return nil, err
	}

	return ndw.handleAggregations(res)
}

func (ndw *NestedDocumentWrapper) wrapAndApplyToMainBuilder(builtReq *search.Request, mainBuilder *reveald.QueryBuilder) {
	// Wrap query in nested
	if builtReq.Query != nil && builtReq.Query.Bool != nil && len(builtReq.Query.Bool.Must) > 0 {
		nestedQuery := types.Query{
			Nested: &types.NestedQuery{
				Path:  ndw.path,
				Query: *builtReq.Query,
			},
		}
		mainBuilder.With(nestedQuery)
	}

	// Wrap each aggregation
	for aggName, agg := range builtReq.Aggregations {
		// Build filter clauses (conjunctive vs disjunctive)
		filterClauses := ndw.buildFilterClausesForAgg(aggName, builtReq.Query)

		wrappedAgg := types.Aggregations{
			Nested: &types.NestedAggregation{Path: &ndw.path},
			Aggregations: map[string]types.Aggregations{
				aggName + "._filter": {
					Filter: &types.Query{Bool: &types.BoolQuery{Must: filterClauses}},
					Aggregations: map[string]types.Aggregations{
						aggName: agg,
					},
				},
			},
		}
		mainBuilder.Aggregation(aggName, wrappedAgg)
	}
}

// handleAggregations unwraps nested aggregation results for sub-features only
func (ndw *NestedDocumentWrapper) handleAggregations(res *reveald.Result) (*reveald.Result, error) {
	// Get the property names that this wrapper is responsible for
	wrappedProperties := ndw.getWrappedPropertyNames()

	// Unwrap only the aggregations that belong to our nested features
	rawAggs := res.RawAggregations()
	unwrappedAggs := make(map[string]types.Aggregate)

	// Copy all aggregations, but unwrap the ones we're responsible for
	for aggName, rawAgg := range rawAggs {
		if wrappedProperties[aggName] {
			// This is one of our nested aggregations - unwrap it
			innerAgg := ndw.unwrapNestedAggregation(aggName, rawAggs)
			if innerAgg != nil {
				unwrappedAggs[aggName] = innerAgg
			}
		} else {
			// Not our aggregation - leave it unchanged
			unwrappedAggs[aggName] = rawAgg
		}
	}

	// Replace the response's aggregations with the modified map
	rawResponse := res.RawResult()
	rawResponse.Aggregations = unwrappedAggs

	// Now let each sub-feature handle its aggregation normally
	builder := reveald.NewQueryBuilder(res.Request(), "dummy")
	currentResult := res

	for _, feature := range ndw.features {
		var err error
		currentResult, err = feature.Process(builder, func(b *reveald.QueryBuilder) (*reveald.Result, error) {
			return currentResult, nil
		})
		if err != nil {
			return nil, err
		}
	}

	return currentResult, nil
}

// getWrappedPropertyNames returns a set of property names this wrapper manages
func (ndw *NestedDocumentWrapper) getWrappedPropertyNames() map[string]bool {
	properties := make(map[string]bool)

	for _, feature := range ndw.features {
		var propertyName string

		switch f := feature.(type) {
		case *DynamicFilterFeature:
			propertyName = f.property
		case *HistogramFeature:
			propertyName = f.property
		case *DateHistogramFeature:
			propertyName = f.property
		}

		if propertyName != "" {
			properties[propertyName] = true
		}
	}

	return properties
}

// unwrapNestedAggregation extracts inner aggregation from: nested -> filter -> innerAgg
func (ndw *NestedDocumentWrapper) unwrapNestedAggregation(aggName string, rawAggs map[string]types.Aggregate) types.Aggregate {
	raw, ok := rawAggs[aggName]
	if !ok {
		return nil
	}

	// Step 1: Unwrap nested aggregation
	nestedAgg, ok := raw.(*types.NestedAggregate)
	if !ok || nestedAgg == nil {
		return nil
	}

	// Step 2: Get filter aggregation
	filterNode, ok := nestedAgg.Aggregations[aggName+"._filter"]
	if !ok || filterNode == nil {
		return nil
	}

	filterAgg, ok := filterNode.(*types.FilterAggregate)
	if !ok || filterAgg == nil {
		return nil
	}

	// Step 3: Get the actual inner aggregation
	innerAgg, ok := filterAgg.Aggregations[aggName]
	if !ok || innerAgg == nil {
		return nil
	}

	return innerAgg
}

// unwrapSingleAggregation extracts the inner aggregation from: nested -> filter -> innerAgg
func (ndw *NestedDocumentWrapper) unwrapSingleAggregation(aggName string, rawAgg types.Aggregate) types.Aggregate {
	// Step 1: Unwrap nested aggregation
	nestedAgg, ok := rawAgg.(*types.NestedAggregate)
	if !ok || nestedAgg == nil {
		return nil
	}

	// Step 2: Get the filter aggregation
	filterNode, ok := nestedAgg.Aggregations[aggName+"._filter"]
	if !ok || filterNode == nil {
		return nil
	}

	filterAgg, ok := filterNode.(*types.FilterAggregate)
	if !ok || filterAgg == nil {
		return nil
	}

	// Step 3: Get the actual inner aggregation
	innerAgg, ok := filterAgg.Aggregations[aggName]
	if !ok || innerAgg == nil {
		return nil
	}

	// Return the unwrapped aggregation
	return innerAgg
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

// Property returns the nested path for this wrapper.
func (ndw *NestedDocumentWrapper) Property() string {
	return ndw.path
}

// Features returns the child features wrapped by this wrapper.
func (ndw *NestedDocumentWrapper) Features() []reveald.Feature {
	return ndw.features
}

// buildFilterClausesForAgg determines which filter clauses to apply for a given aggregation.
// In conjunctive mode, includes all filters. In disjunctive mode, excludes the filter for this property.
func (ndw *NestedDocumentWrapper) buildFilterClausesForAgg(aggName string, query *types.Query) []types.Query {
	if query == nil || query.Bool == nil {
		return nil
	}

	allClauses := query.Bool.Must

	if !ndw.disjunctive {
		// Conjunctive mode: include all filters
		return append([]types.Query{}, allClauses...)
	}

	// Disjunctive mode: exclude the filter for this specific aggregation
	var result []types.Query
	for _, mustClause := range allClauses {
		property := ndw.extractPropertyFromQuery(mustClause)
		// Exclude this clause if it's for the current aggregation property
		if property != aggName {
			result = append(result, mustClause)
		}
	}

	return result
}

// extractPropertyFromQuery attempts to extract the property name from a query clause.
// This is used to determine which filter belongs to which property for disjunctive mode.
func (ndw *NestedDocumentWrapper) extractPropertyFromQuery(query types.Query) string {
	// Handle term queries (from DynamicFilterFeature)
	if query.Term != nil {
		for field := range query.Term {
			// Remove .keyword suffix to get base property name
			if strings.HasSuffix(field, ".keyword") {
				return field[:len(field)-len(".keyword")]
			}
			return field
		}
	}

	// Handle range queries (from HistogramFeature and DateHistogramFeature)
	if query.Range != nil {
		for field := range query.Range {
			return field
		}
	}

	// Handle bool queries with should clauses (from DynamicFilterFeature with multiple values)
	if query.Bool != nil {
		if len(query.Bool.Should) > 0 {
			// Recursively extract from first should clause
			return ndw.extractPropertyFromQuery(query.Bool.Should[0])
		}

		// Handle missing value queries (bool with must_not exists)
		if len(query.Bool.MustNot) > 0 {
			for _, mustNot := range query.Bool.MustNot {
				if mustNot.Exists != nil {
					field := mustNot.Exists.Field
					if strings.HasSuffix(field, ".keyword") {
						return field[:len(field)-len(".keyword")]
					}
					return field
				}
			}
		}
	}

	// If we can't determine the property, return empty string
	return ""
}

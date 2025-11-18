package featureset

import (
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
)

// NestedDocumentWrapper wraps features to support nested document queries.
// It automatically wraps child feature queries and aggregations in nested structures
// for the specified path.
//
// Example:
//
//	wrapper := featureset.NewNestedDocumentWrapper("items",
//	    featureset.NewDynamicFilterFeature("items.category"),
//	    featureset.NewDynamicFilterFeature("items.tags"),
//	)
type NestedDocumentWrapper struct {
	path        string
	features    []reveald.Feature
	disjunctive bool
}

// NewNestedDocumentWrapper creates a nested document wrapper for the specified path.
// The path should be the nested field path (e.g., "items"), and features should
// operate on fields within that nested path.
func NewNestedDocumentWrapper(path string, features ...reveald.Feature) *NestedDocumentWrapper {
	return &NestedDocumentWrapper{
		path:        path,
		features:    features,
		disjunctive: false,
	}
}

// Disjunctive enables disjunctive mode for faceted search aggregations.
//
// In conjunctive mode (default), aggregations include all active filters, narrowing
// results progressively as filters are applied.
//
// In disjunctive mode, each facet's aggregation excludes its own filter but includes
// others, allowing users to see all available options for each facet independently.
// This prevents empty aggregation buckets when multiple filters are active.
//
// Example:
//
//	wrapper := featureset.NewNestedDocumentWrapper("items",
//	    featureset.NewDynamicFilterFeature("items.category"),
//	    featureset.NewDynamicFilterFeature("items.tags"),
//	).Disjunctive(true)
func (ndw *NestedDocumentWrapper) Disjunctive(enable bool) *NestedDocumentWrapper {
	ndw.disjunctive = enable
	return ndw
}

// Process implements the Feature interface, wrapping child feature queries and
// aggregations in nested structures.
func (ndw *NestedDocumentWrapper) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	innerQueryBuilder := reveald.NewQueryBuilder(builder.Request(), builder.Indices()...)
	for _, feature := range ndw.features {
		_, err := feature.Process(innerQueryBuilder, func(b *reveald.QueryBuilder) (*reveald.Result, error) {
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

// handleAggregations unwraps nested aggregation results for child features
func (ndw *NestedDocumentWrapper) handleAggregations(res *reveald.Result) (*reveald.Result, error) {
	rawAggs := res.RawAggregations()
	unwrappedAggs := make(map[string]types.Aggregate)

	// Copy all aggregations, but unwrap the ones that belong to our nested path
	for aggName, rawAgg := range rawAggs {
		if ndw.isOurAggregation(aggName) {
			// This aggregation belongs to our nested path - unwrap it
			innerAgg := ndw.unwrapNestedAggregation(aggName, rawAggs)
			if innerAgg != nil {
				unwrappedAggs[aggName] = innerAgg
			} else {
				// Unwrapping failed, keep original
				unwrappedAggs[aggName] = rawAgg
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

// isOurAggregation checks if an aggregation belongs to this nested path
func (ndw *NestedDocumentWrapper) isOurAggregation(aggName string) bool {
	// Check if the aggregation name starts with our nested path
	// e.g., "reviews.author" starts with "reviews"
	return strings.HasPrefix(aggName, ndw.path+".")
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

// buildFilterClausesForAgg builds filter clauses for an aggregation based on mode.
// Conjunctive: includes all filters. Disjunctive: excludes the aggregation's own filter.
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

// extractPropertyFromQuery extracts the property name from a query clause for
// determining filter ownership in disjunctive mode.
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

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
//	    featureset.WithInnerHits(),
//	    featureset.WithFeature(featureset.NewDynamicFilterFeature("items.category")),
//	    featureset.WithFeature(featureset.NewDynamicFilterFeature("items.tags")),
//	)
type NestedDocumentWrapper struct {
	path            string
	features        []reveald.Feature
	innerHitsConfig *types.InnerHits
	disjunctive     bool
}

// NestedDocumentWrapperOption is a functional option for configuring NestedDocumentWrapper
type NestedDocumentWrapperOption func(*NestedDocumentWrapper)

// WithFeature adds a feature to the nested document wrapper.
func WithFeatures(features ...reveald.Feature) NestedDocumentWrapperOption {
	return func(ndw *NestedDocumentWrapper) {
		ndw.features = append(ndw.features, features...)
	}
}

// WithInnerHits enables inner hits for the nested query, allowing access to the nested
// documents that matched the query.
func WithInnerHits() NestedDocumentWrapperOption {
	return func(ndw *NestedDocumentWrapper) {
		ndw.innerHitsConfig = &types.InnerHits{}
	}
}

// WithInnerHitsConfig enables inner hits with custom configuration.
func WithInnerHitsConfig(config *types.InnerHits) NestedDocumentWrapperOption {
	return func(ndw *NestedDocumentWrapper) {
		ndw.innerHitsConfig = config
	}
}

// WithDisjunctiveAggregations enables disjunctive aggregations mode.
// In disjunctive mode, each aggregation excludes its own filter but includes all other filters.
// This allows users to see what options are available even when a filter is active.
//
// Example:
//   If filtering by reviews.author="John" AND reviews.verified=true:
//   - The reviews.author aggregation will show all authors with verified reviews (not just John)
//   - The reviews.verified aggregation will show verified/unverified counts for John's reviews
//
// This is commonly used in faceted search UIs (like e-commerce filters).
func WithDisjunctiveAggregations() NestedDocumentWrapperOption {
	return func(ndw *NestedDocumentWrapper) {
		ndw.disjunctive = true
	}
}

// NewNestedDocumentWrapper creates a nested document wrapper for the specified path.
// The path should be the nested field path (e.g., "items"), and features should
// operate on fields within that nested path.
// Options can be passed to configure inner hits and other settings.
func NewNestedDocumentWrapper(path string, opts ...NestedDocumentWrapperOption) *NestedDocumentWrapper {
	ndw := &NestedDocumentWrapper{
		path:     path,
		features: []reveald.Feature{},
	}

	// Apply options
	for _, opt := range opts {
		opt(ndw)
	}

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
	var must, mustNot []types.Query
	if builtReq.Query != nil && builtReq.Query.Bool != nil {
		must = builtReq.Query.Bool.Must
		mustNot = builtReq.Query.Bool.MustNot
	}

	// Wrap positive clauses together in nested, so a single nested document must match all of them
	if len(must) > 0 {
		nestedQuery := types.Query{
			Nested: &types.NestedQuery{
				Path:      ndw.path,
				Query:     types.Query{Bool: &types.BoolQuery{Must: must}},
				InnerHits: ndw.innerHitsConfig,
			},
		}
		mainBuilder.With(nestedQuery)
	}

	// Exclusions apply to the parent document: exclude it if any nested document matches
	for _, clause := range mustNot {
		mainBuilder.Without(ndw.wrapNested(clause))
	}

	// Wrap each aggregation
	for aggName, agg := range builtReq.Aggregations {
		filter := &types.Query{Bool: &types.BoolQuery{
			Must: ndw.buildFilterClausesForAgg(aggName, must),
		}}

		var wrappedAgg types.Aggregations

		if ndw.disjunctive {
			// In disjunctive mode, use global aggregation to include all documents,
			// then apply nested and filter scopes
			nested := types.Aggregations{
				Nested: &types.NestedAggregation{Path: &ndw.path},
				Aggregations: map[string]types.Aggregations{
					aggName + "._filter": {
						Filter: filter,
						Aggregations: map[string]types.Aggregations{
							aggName: agg,
						},
					},
				},
			}

			// The global scope ignores the main query, so exclusions are applied
			// to parent documents before entering the nested scope
			if exclusions := ndw.buildFilterClausesForAgg(aggName, mustNot); len(exclusions) > 0 {
				parentMustNot := make([]types.Query, 0, len(exclusions))
				for _, clause := range exclusions {
					parentMustNot = append(parentMustNot, ndw.wrapNested(clause))
				}

				nested = types.Aggregations{
					Filter: &types.Query{Bool: &types.BoolQuery{MustNot: parentMustNot}},
					Aggregations: map[string]types.Aggregations{
						aggName + "._nested": nested,
					},
				}
				wrappedAgg = types.Aggregations{
					Global: &types.GlobalAggregation{},
					Aggregations: map[string]types.Aggregations{
						aggName + "._parent": nested,
					},
				}
			} else {
				wrappedAgg = types.Aggregations{
					Global: &types.GlobalAggregation{},
					Aggregations: map[string]types.Aggregations{
						aggName + "._nested": nested,
					},
				}
			}
		} else {
			// In conjunctive mode, just use nested and filter. Exclusions are already
			// applied to parent documents by the main query.
			wrappedAgg = types.Aggregations{
				Nested: &types.NestedAggregation{Path: &ndw.path},
				Aggregations: map[string]types.Aggregations{
					aggName + "._filter": {
						Filter: filter,
						Aggregations: map[string]types.Aggregations{
							aggName: agg,
						},
					},
				},
			}
		}

		mainBuilder.Aggregation(aggName, wrappedAgg)
	}
}

// wrapNested wraps a query in a nested query for the wrapper's path.
func (ndw *NestedDocumentWrapper) wrapNested(query types.Query) types.Query {
	return types.Query{
		Nested: &types.NestedQuery{
			Path:  ndw.path,
			Query: query,
		},
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

// unwrapNestedAggregation extracts inner aggregation from:
// - Disjunctive mode: global -> [parent filter] -> nested -> filter -> innerAgg
// - Conjunctive mode: nested -> filter -> innerAgg
func (ndw *NestedDocumentWrapper) unwrapNestedAggregation(aggName string, rawAggs map[string]types.Aggregate) types.Aggregate {
	raw, ok := rawAggs[aggName]
	if !ok {
		return nil
	}

	var nestedAgg *types.NestedAggregate

	if ndw.disjunctive {
		// Step 1a: Unwrap global aggregation
		globalAgg, ok := raw.(*types.GlobalAggregate)
		if !ok || globalAgg == nil {
			return nil
		}

		// Step 1b: Get nested aggregation from within global, through the
		// parent exclusion filter if present
		scope := globalAgg.Aggregations
		if parentNode, ok := scope[aggName+"._parent"]; ok {
			parentAgg, ok := parentNode.(*types.FilterAggregate)
			if !ok || parentAgg == nil {
				return nil
			}
			scope = parentAgg.Aggregations
		}

		nestedNode, ok := scope[aggName+"._nested"]
		if !ok || nestedNode == nil {
			return nil
		}

		nestedAgg, ok = nestedNode.(*types.NestedAggregate)
		if !ok || nestedAgg == nil {
			return nil
		}
	} else {
		// Step 1: Unwrap nested aggregation directly
		var ok bool
		nestedAgg, ok = raw.(*types.NestedAggregate)
		if !ok || nestedAgg == nil {
			return nil
		}
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

// buildFilterClausesForAgg builds filter clauses for an aggregation.
// In conjunctive mode: all clauses are included.
// In disjunctive mode: all clauses except the ones for this specific aggregation are included.
func (ndw *NestedDocumentWrapper) buildFilterClausesForAgg(aggName string, clauses []types.Query) []types.Query {
	// In conjunctive mode, include all filters
	if !ndw.disjunctive {
		return append([]types.Query{}, clauses...)
	}

	// In disjunctive mode, exclude the filter for this specific aggregation
	var filterClauses []types.Query
	for _, clause := range clauses {
		// Check if this filter is for the current aggregation
		isForThisAgg := ndw.isFilterForAggregation(aggName, clause)
		if !isForThisAgg {
			filterClauses = append(filterClauses, clause)
		}
	}

	return filterClauses
}

// isFilterForAggregation checks if a query clause is a filter for the given aggregation name.
// It inspects nested queries and term queries to determine if they match the aggregation field.
func (ndw *NestedDocumentWrapper) isFilterForAggregation(aggName string, query types.Query) bool {
	// Check if it's a nested query
	if query.Nested != nil {
		// For nested queries, we need to check the inner query
		innerQuery := query.Nested.Query
		if innerQuery.Bool != nil {
			// Check should clauses (used for multi-value filters)
			for _, shouldClause := range innerQuery.Bool.Should {
				if ndw.matchesAggregationField(aggName, shouldClause) {
					return true
				}
			}
			// Check must clauses
			for _, mustClause := range innerQuery.Bool.Must {
				if ndw.matchesAggregationField(aggName, mustClause) {
					return true
				}
			}
		} else if ndw.matchesAggregationField(aggName, innerQuery) {
			return true
		}
	}

	// Check if it's a direct term query
	return ndw.matchesAggregationField(aggName, query)
}

// matchesAggregationField checks if a query matches the aggregation field.
func (ndw *NestedDocumentWrapper) matchesAggregationField(aggName string, query types.Query) bool {
	// Check term queries
	if query.Term != nil {
		for field := range query.Term {
			// Check both with and without .keyword suffix
			if field == aggName || field == aggName+".keyword" {
				return true
			}
		}
	}

	// Check bool queries with should clauses (used for multi-value filters)
	if query.Bool != nil && len(query.Bool.Should) > 0 {
		// Check if any should clause matches the aggregation field
		for _, shouldClause := range query.Bool.Should {
			if ndw.matchesAggregationField(aggName, shouldClause) {
				return true
			}
		}
	}

	// Check bool queries with must_not (used for missing values)
	if query.Bool != nil && len(query.Bool.MustNot) > 0 {
		for _, mustNotClause := range query.Bool.MustNot {
			if mustNotClause.Exists != nil {
				field := mustNotClause.Exists.Field
				if field == aggName || field == aggName+".keyword" {
					return true
				}
			}
		}
	}

	// Check range queries (for numeric/date fields)
	if query.Range != nil {
		for field := range query.Range {
			if field == aggName {
				return true
			}
		}
	}

	return false
}

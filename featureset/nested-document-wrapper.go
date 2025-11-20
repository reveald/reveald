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
	// Wrap query in nested
	if builtReq.Query != nil && builtReq.Query.Bool != nil && len(builtReq.Query.Bool.Must) > 0 {
		nestedQuery := types.Query{
			Nested: &types.NestedQuery{
				Path:      ndw.path,
				Query:     *builtReq.Query,
				InnerHits: ndw.innerHitsConfig,
			},
		}
		mainBuilder.With(nestedQuery)
	}

	// Wrap each aggregation
	for aggName, agg := range builtReq.Aggregations {
		filterClauses := ndw.buildFilterClausesForAgg(builtReq.Query)

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

// buildFilterClausesForAgg builds filter clauses for an aggregation.
// All filters are included (conjunctive mode).
func (ndw *NestedDocumentWrapper) buildFilterClausesForAgg(query *types.Query) []types.Query {
	if query == nil || query.Bool == nil {
		return nil
	}

	return append([]types.Query{}, query.Bool.Must...)
}

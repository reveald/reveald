package featureset

import (
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
)

type DynamicFilterFeature struct {
	property string
	nested   bool
	agg      AggregationFeature
}

func NewDynamicFilterFeature(property string, opts ...AggregationOption) *DynamicFilterFeature {
	return &DynamicFilterFeature{
		property: property,
		nested:   false,
		agg:      buildAggregationFeature(opts...),
	}
}

func NewNestedDocumentFilterFeature(property string, opts ...AggregationOption) *DynamicFilterFeature {
	return &DynamicFilterFeature{
		property: property,
		nested:   true,
		agg:      buildAggregationFeature(opts...),
	}
}

func (dff *DynamicFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	dff.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return dff.handle(r)
}

func (dff *DynamicFilterFeature) build(builder *reveald.QueryBuilder) {
	keyword := fmt.Sprintf("%s.keyword", dff.property)

	if !dff.nested {
		// Create terms aggregation directly with typed objects
		field := keyword
		size := dff.agg.size

		termsAgg := &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		}

		// Use built-in Missing parameter if missingValue is configured
		if dff.agg.missingValue != "" {
			termsAgg.Missing = types.Missing(dff.agg.missingValue)
		}

		termAgg := types.Aggregations{
			Terms: termsAgg,
		}

		builder.Aggregation(dff.property, termAgg)
	} else {
		// Create nested aggregation with term sub-aggregation
		path := strings.Split(dff.property, ".")[0]

		// First create the inner terms aggregation
		field := keyword
		size := dff.agg.size

		innerTermsAgg := &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		}

		// Use built-in Missing parameter if missingValue is configured
		if dff.agg.missingValue != "" {
			innerTermsAgg.Missing = types.Missing(dff.agg.missingValue)
		}

		termsAgg := types.Aggregations{
			Terms: innerTermsAgg,
		}

		// Create the nested aggregation
		nestedPath := path
		nestedAgg := types.Aggregations{
			Nested: &types.NestedAggregation{
				Path: &nestedPath,
			},
			Aggregations: map[string]types.Aggregations{
				dff.property: termsAgg,
			},
		}

		builder.Aggregation(dff.property, nestedAgg)
	}

	if builder.Request().Has(dff.property) {
		p, err := builder.Request().Get(dff.property)
		if err != nil {
			return
		}

		if len(p.Values()) > 0 {
			if !dff.nested && len(p.Values()) == 1 {
				// Single value - simple term query (or missing query for the missing label)
				builder.With(dff.valueQuery(keyword, p.Values()[0]))
			} else {
				builder.With(dff.wrapNested(dff.anyOf(keyword, p.Values())))
			}
		}

		if len(p.Excludes()) > 0 {
			// Exclude documents matching any of the excluded values
			builder.Without(dff.wrapNested(dff.anyOf(keyword, p.Excludes())))
		}
	}
}

// valueQuery builds a query matching a single value, or documents missing
// the property if the value equals the configured missing label.
func (dff *DynamicFilterFeature) valueQuery(keyword, value string) types.Query {
	if dff.agg.missingValue != "" && value == dff.agg.missingValue {
		// Build missing filter query (must_not exists covers both null and missing)
		return types.Query{
			Bool: &types.BoolQuery{
				MustNot: []types.Query{
					{Exists: &types.ExistsQuery{Field: keyword}},
				},
			},
		}
	}

	return types.Query{
		Term: map[string]types.TermQuery{
			keyword: {Value: value},
		},
	}
}

// anyOf builds a bool query with 'should' clauses matching any of the values.
func (dff *DynamicFilterFeature) anyOf(keyword string, values []string) types.Query {
	shouldClauses := make([]types.Query, 0, len(values))
	for _, v := range values {
		shouldClauses = append(shouldClauses, dff.valueQuery(keyword, v))
	}

	return types.Query{
		Bool: &types.BoolQuery{
			Should: shouldClauses,
		},
	}
}

// wrapNested wraps the query in a nested query for nested fields.
func (dff *DynamicFilterFeature) wrapNested(query types.Query) types.Query {
	if !dff.nested {
		return query
	}

	return types.Query{
		Nested: &types.NestedQuery{
			Path:  strings.Split(dff.property, ".")[0],
			Query: query,
		},
	}
}

func (dff *DynamicFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawAggregations()[dff.property]
	if !ok {
		return result, nil
	}

	// Handle nested aggregations - extract inner terms from nested aggregate
	if dff.nested {
		nestedAgg, ok := agg.(*types.NestedAggregate)
		if !ok {
			return result, nil
		}

		innerAgg, ok := nestedAgg.Aggregations[dff.property]
		if !ok {
			return result, nil
		}

		agg = innerAgg
	}

	terms, ok := agg.(*types.StringTermsAggregate)
	if !ok {
		return result, nil
	}

	buckets := terms.Buckets.([]types.StringTermsBucket)

	// Missing values are automatically included in buckets when Missing parameter is set
	var resultBuckets []*reveald.ResultBucket
	for _, bucket := range buckets {
		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
			Value:    bucket.Key,
			HitCount: bucket.DocCount,
		})
	}

	result.Aggregations[dff.property] = resultBuckets

	return result, nil
}

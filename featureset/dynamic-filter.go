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

		if !dff.nested {
			// Term query with 'should' clauses for non-nested fields
			if len(p.Values()) == 1 && (dff.agg.missingValue == "" || p.Values()[0] != dff.agg.missingValue) {
				// Single value (not missing label) - simple term query
				termQuery := types.Query{
					Term: map[string]types.TermQuery{
						keyword: {Value: p.Values()[0]},
					},
				}

				builder.With(termQuery)
			} else {
				// Multiple values or contains missing label - bool query with should clauses
				shouldClauses := make([]types.Query, 0, len(p.Values()))
				for _, v := range p.Values() {
					if dff.agg.missingValue != "" && v == dff.agg.missingValue {
						// Build missing filter query (must_not exists covers both null and missing)
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

				if len(shouldClauses) > 0 {
					boolQuery := types.Query{
						Bool: &types.BoolQuery{
							Should: shouldClauses,
						},
					}

					builder.With(boolQuery)
				}
			}
		} else {
			// Nested query for nested fields
			path := strings.Split(dff.property, ".")[0]

			// Create should clauses for the nested query
			shouldClauses := make([]types.Query, 0, len(p.Values()))
			for _, v := range p.Values() {
				if dff.agg.missingValue != "" && v == dff.agg.missingValue {
					// Build missing filter query (must_not exists covers both null and missing)
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

			if len(shouldClauses) > 0 {
				// Create the inner bool query
				innerBoolQuery := types.BoolQuery{
					Should: shouldClauses,
				}

				// Create the nested query with the inner bool query
				nestedQuery := types.Query{
					Nested: &types.NestedQuery{
						Path:  path,
						Query: types.Query{Bool: &innerBoolQuery},
					},
				}

				builder.With(nestedQuery)
			}
		}
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

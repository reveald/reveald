package featureset

import (
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
)

type DynamicBooleanFilterFeature struct {
	property string
	nested   bool
	agg      AggregationFeature
}

func NewDynamicBooleanFilterFeature(property string, opts ...AggregationOption) *DynamicBooleanFilterFeature {
	return &DynamicBooleanFilterFeature{
		property: property,
		nested:   false,
		agg:      buildAggregationFeature(opts...),
	}
}

func NewNestedDocumentBooleanFilterFeature(property string, opts ...AggregationOption) *DynamicBooleanFilterFeature {
	return &DynamicBooleanFilterFeature{
		property: property,
		nested:   true,
		agg:      buildAggregationFeature(opts...),
	}
}

func (dbff *DynamicBooleanFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	dbff.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return dbff.handle(r)
}

func (dbff *DynamicBooleanFilterFeature) build(builder *reveald.QueryBuilder) {
	if !dbff.nested {
		// Create filters based on missingValue configuration
		filters := map[string]types.Query{
			"true": {
				Term: map[string]types.TermQuery{
					dbff.property: {Value: true},
				},
			},
		}

		if dbff.agg.missingValue != "" {
			// Split false and missing into separate filters with custom label
			filters["false"] = types.Query{
				Term: map[string]types.TermQuery{
					dbff.property: {Value: false},
				},
			}
			filters[dbff.agg.missingValue] = types.Query{
				Bool: &types.BoolQuery{
					MustNot: []types.Query{
						{
							Exists: &types.ExistsQuery{
								Field: dbff.property,
							},
						},
					},
				},
			}
		} else {
			// Combine false and missing into "false" filter (current behavior)
			filters["false"] = types.Query{
				Bool: &types.BoolQuery{
					Should: []types.Query{
						{
							Bool: &types.BoolQuery{
								MustNot: []types.Query{
									{
										Exists: &types.ExistsQuery{
											Field: dbff.property,
										},
									},
								},
							},
						},
						{
							Term: map[string]types.TermQuery{
								dbff.property: {Value: false},
							},
						},
					},
					MinimumShouldMatch: 1,
				},
			}
		}

		termAgg := types.Aggregations{
			Filters: &types.FiltersAggregation{
				Filters: filters,
			},
		}

		builder.Aggregation(dbff.property, termAgg)
	} else {
		// Create nested aggregation with term sub-aggregation
		path := strings.Split(dbff.property, ".")[0]

		// First create the inner terms aggregation
		field := dbff.property
		size := dbff.agg.size

		innerTermsAgg := &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		}

		// Use built-in Missing parameter if missingValue is configured
		if dbff.agg.missingValue != "" {
			innerTermsAgg.Missing = types.Missing(dbff.agg.missingValue)
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
				dbff.property: termsAgg,
			},
		}

		builder.Aggregation(dbff.property, nestedAgg)
	}

	if builder.Request().Has(dbff.property) {
		p, err := builder.Request().Get(dbff.property)
		if err != nil {
			return
		}

		if !dbff.nested {
			// Term query with 'should' clauses for non-nested fields
			if len(p.Values()) == 1 && (dbff.agg.missingValue == "" || p.Values()[0] != dbff.agg.missingValue) {
				// Single value (not missing label) - simple term query
				boolValue, err := strconv.ParseBool(p.Values()[0])
				if err != nil {
					return
				}

				termQuery := types.Query{
					Term: map[string]types.TermQuery{
						dbff.property: {Value: boolValue},
					},
				}

				builder.With(termQuery)
			} else {
				// Multiple values or contains missing label - bool query with should clauses
				shouldClauses := make([]types.Query, 0, len(p.Values()))
				for _, v := range p.Values() {
					if dbff.agg.missingValue != "" && v == dbff.agg.missingValue {
						// Build missing filter query
						missingQuery := types.Query{
							Bool: &types.BoolQuery{
								MustNot: []types.Query{
									{Exists: &types.ExistsQuery{Field: dbff.property}},
								},
							},
						}
						shouldClauses = append(shouldClauses, missingQuery)
					} else {
						boolValue, err := strconv.ParseBool(v)
						if err != nil {
							continue // Skip invalid boolean values
						}

						termQuery := types.Query{
							Term: map[string]types.TermQuery{
								dbff.property: {Value: boolValue},
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
			path := strings.Split(dbff.property, ".")[0]

			// Create should clauses for the nested query
			shouldClauses := make([]types.Query, 0, len(p.Values()))
			for _, v := range p.Values() {
				if dbff.agg.missingValue != "" && v == dbff.agg.missingValue {
					// Build missing filter query
					missingQuery := types.Query{
						Bool: &types.BoolQuery{
							MustNot: []types.Query{
								{Exists: &types.ExistsQuery{Field: dbff.property}},
							},
						},
					}
					shouldClauses = append(shouldClauses, missingQuery)
				} else {
					boolValue, err := strconv.ParseBool(v)
					if err != nil {
						continue // Skip invalid boolean values
					}

					termQuery := types.Query{
						Term: map[string]types.TermQuery{
							dbff.property: {Value: boolValue},
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

func (dbff *DynamicBooleanFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawAggregations()[dbff.property]
	if !ok {
		return result, nil
	}

	if !dbff.nested {
		// Handle non-nested FiltersAggregate
		filters, ok := agg.(*types.FiltersAggregate)
		if !ok {
			return result, nil
		}

		buckets, ok := filters.Buckets.(map[string]types.FiltersBucket)
		if !ok {
			return result, nil
		}

		resultBuckets := make([]*reveald.ResultBucket, 0, len(buckets))
		for key, bucket := range buckets {
			resultBuckets = append(resultBuckets, &reveald.ResultBucket{
				Value:    key,
				HitCount: bucket.DocCount,
			})
		}

		result.Aggregations[dbff.property] = resultBuckets
	} else {
		// Handle nested aggregation
		nested, ok := agg.(*types.NestedAggregate)
		if !ok {
			return result, nil
		}

		// Extract the inner terms aggregation from the nested aggregation
		innerAgg, ok := nested.Aggregations[dbff.property]
		if !ok {
			return result, nil
		}

		terms, ok := innerAgg.(*types.StringTermsAggregate)
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

		result.Aggregations[dbff.property] = resultBuckets
	}

	return result, nil
}

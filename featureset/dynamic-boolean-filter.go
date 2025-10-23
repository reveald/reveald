package featureset

import (
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v1"
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

		termAgg := types.Aggregations{
			Filters: &types.FiltersAggregation{
				Filters: map[string]types.Query{
					"true": {
						Term: map[string]types.TermQuery{
							dbff.property: {Value: true},
						},
					},
					"false": {
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
					},
				},
			},
		}

		builder.Aggregation(dbff.property, termAgg)
	} else {
		// Create nested aggregation with term sub-aggregation
		path := strings.Split(dbff.property, ".")[0]

		// First create the inner terms aggregation
		field := dbff.property
		size := dbff.agg.size

		termsAgg := types.Aggregations{
			Terms: &types.TermsAggregation{
				Field: &field,
				Size:  &size,
			},
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
			if len(p.Values()) == 1 {
				// Single value - simple term query
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
				// Multiple values - bool query with should clauses
				shouldClauses := make([]types.Query, 0, len(p.Values()))
				for _, v := range p.Values() {
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

	return result, nil

	// if !dbff.nested {
	// 	aggregate, ok := result.Aggregations[dbff.property]
	// 	if !ok {
	// 		return result, nil
	// 	}

	// 	agg, ok := aggregate.(types.FiltersAggregate)
	// 	if !ok {
	// 		return result, nil
	// 	}

	// 	buckets := agg.Buckets.([]types.FiltersBucket)

	// 	var resultBuckets []*reveald.ResultBucket
	// 	for _, bucket := range buckets {
	// 		resultBuckets = append(resultBuckets, &reveald.ResultBucket{
	// 			Value:    bucket.Key,
	// 			HitCount: bucket.DocCount,
	// 		})
	// 	}

	// items, ok := ragg.Filters.Filters[dbff.property]
	// if !ok {
	// 	return result, nil
	// }

	// agg = items.(map[string]any)
}

// } else {
// 	// bucket, ok := result.RawResult().Aggregations.Children(dbff.property)
// 	// if !ok {
// 	// 	return result, nil
// 	// }

// 	// items, ok := bucket.Aggregations.Filters(dbff.property)
// 	// if !ok {
// 	// 	return result, nil
// 	// }

// 	// agg = items
// }

// var buckets []*reveald.ResultBucket

// for key, bucket := range agg.NamedBuckets {
// 	if bucket == nil {
// 		continue
// 	}

// 	buckets = append(buckets, &reveald.ResultBucket{
// 		Value:    key,
// 		HitCount: bucket.DocCount,
// 	})
// }

// result.Aggregations[dbff.property] = buckets
// return result, nil

package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
)

type BooleanFilterFeature struct {
	property   string
	nested     bool
	nestedPath string
	agg        AggregationFeature
}

type BooleanFilterOption func(*BooleanFilterFeature)

func WithNestedPath(path string) BooleanFilterOption {
	return func(bff *BooleanFilterFeature) {
		bff.nested = true
		bff.nestedPath = path
	}
}

func NewBooleanFilterFeature(property string, opts ...AggregationOption) *BooleanFilterFeature {
	bff := &BooleanFilterFeature{
		property: property,
		nested:   false,
		agg:      buildAggregationFeature(opts...),
	}

	return bff
}

func NewNestedBooleanFilterFeature(property string, nestedPath string, opts ...AggregationOption) *BooleanFilterFeature {
	bff := &BooleanFilterFeature{
		property:   property,
		nested:     true,
		nestedPath: nestedPath,
		agg:        buildAggregationFeature(opts...),
	}

	return bff
}

func (bff *BooleanFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	bff.build(builder)

	result, err := next(builder)
	if err != nil {
		return nil, err
	}

	return bff.handle(result)
}

func (bff *BooleanFilterFeature) build(builder *reveald.QueryBuilder) {
	if !bff.nested {
		// Create terms aggregation for the boolean field
		field := bff.property
		size := bff.agg.size

		termsAgg := &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		}

		// Use built-in Missing parameter if missingValue is configured
		if bff.agg.missingValue != "" {
			termsAgg.Missing = types.Missing(bff.agg.missingValue)
		}

		termAgg := types.Aggregations{
			Terms: termsAgg,
		}

		builder.Aggregation(bff.property, termAgg)
	} else {
		// Create nested aggregation with term sub-aggregation
		path := bff.nestedPath

		// First create the inner terms aggregation
		field := bff.property
		size := bff.agg.size

		innerTermsAgg := &types.TermsAggregation{
			Field: &field,
			Size:  &size,
		}

		// Use built-in Missing parameter if missingValue is configured
		if bff.agg.missingValue != "" {
			innerTermsAgg.Missing = types.Missing(bff.agg.missingValue)
		}

		termsAgg := types.Aggregations{
			Terms: innerTermsAgg,
		}

		// Create the nested aggregation
		nestedAgg := types.Aggregations{
			Nested: &types.NestedAggregation{
				Path: &path,
			},
			Aggregations: map[string]types.Aggregations{
				bff.property: termsAgg,
			},
		}

		builder.Aggregation(bff.property, nestedAgg)
	}

	// Add filtering if parameter exists
	param, err := builder.Request().Get(bff.property)
	if err == nil {
		// Check if filtering by custom missing label
		if bff.agg.missingValue != "" && param.Value() == bff.agg.missingValue {
			// Build missing filter query
			missingQuery := types.Query{
				Bool: &types.BoolQuery{
					MustNot: []types.Query{
						{Exists: &types.ExistsQuery{Field: bff.property}},
					},
				},
			}

			if !bff.nested {
				builder.With(missingQuery)
			} else {
				// Wrap in nested query if needed
				nestedQuery := types.Query{
					Nested: &types.NestedQuery{
						Path:  bff.nestedPath,
						Query: missingQuery,
					},
				}
				builder.With(nestedQuery)
			}
		} else if param.IsTruthy() {
			// Create a term query for the boolean field (true)
			termQuery := types.Query{
				Term: map[string]types.TermQuery{
					bff.property: {Value: true},
				},
			}

			if !bff.nested {
				builder.With(termQuery)
			} else {
				// Wrap in nested query if needed
				nestedQuery := types.Query{
					Nested: &types.NestedQuery{
						Path:  bff.nestedPath,
						Query: termQuery,
					},
				}
				builder.With(nestedQuery)
			}
		}
	}
}

func (bff *BooleanFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	agg, ok := result.RawAggregations()[bff.property]
	if !ok {
		return result, nil
	}

	// Handle nested aggregations - extract inner terms from nested aggregate
	if bff.nested {
		nestedAgg, ok := agg.(*types.NestedAggregate)
		if !ok {
			return result, nil
		}

		innerAgg, ok := nestedAgg.Aggregations[bff.property]
		if !ok {
			return result, nil
		}

		agg = innerAgg
	}

	// Handle direct term aggregation
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

	result.Aggregations[bff.property] = resultBuckets

	return result, nil
}

package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v1"
)

// StaticFilterFeature adds a static filter to the query.
//
// It applies a predefined filter to all queries, regardless of request parameters.
// This is useful for implementing global filters like tenant isolation or access control.
//
// Example:
//
//	// Create a static filter that only shows active products
//	staticFilter := featureset.NewStaticFilterFeature(
//	    types.Query{
//	        Term: map[string]types.TermQuery{
//	            "active": {Value: true},
//	        },
//	    },
//	)
//
//	// Use the static filter in a feature chain
//	result, err := staticFilter.Process(builder, nextFeature)
type StaticFilterFeature struct {
	query *types.Query
}

// StaticFilterOption is a functional option for configuring a StaticFilterFeature.
type StaticFilterOption func(*StaticFilterFeature)

// WithQuery sets the query to use for the static filter.
//
// Example:
//
//	// Create a static filter with a range query
//	minPrice := types.Float64(10)
//	staticFilter := featureset.NewStaticFilterFeature(
//	    featureset.WithQuery(types.Query{
//	        Range: map[string]types.RangeQuery{
//	            "price": &types.NumberRangeQuery{
//	                Gte: &minPrice,
//	            },
//	        },
//	    }),
//	)
func WithQuery(query types.Query) StaticFilterOption {
	return func(sff *StaticFilterFeature) {
		queryCopy := query // Create a copy to get a pointer to
		sff.query = &queryCopy
	}
}

// NewStaticFilterFeature creates a new static filter feature.
//
// You can either provide the query directly as the first parameter,
// or use the WithQuery option.
//
// Example:
//
//	// Create a static filter directly with a term query
//	staticFilter := featureset.NewStaticFilterFeature(
//	    types.Query{
//	        Term: map[string]types.TermQuery{
//	            "active": {Value: true},
//	        },
//	    },
//	)
//
//	// Or use the WithQuery option
//	staticFilter := featureset.NewStaticFilterFeature(
//	    featureset.WithQuery(types.Query{
//	        Term: map[string]types.TermQuery{
//	            "active": {Value: true},
//	        },
//	    }),
//	)
func NewStaticFilterFeature(options ...StaticFilterOption) *StaticFilterFeature {
	sff := &StaticFilterFeature{}

	for _, opt := range options {
		opt(sff)
	}

	return sff
}

// NewStaticFilterFeatureWithQuery creates a new StaticFilterFeature with a query.
// This is a convenience function for backward compatibility.
func NewStaticFilterFeatureWithQuery(query types.Query) *StaticFilterFeature {
	queryCopy := query // Create a copy to get a pointer to
	return &StaticFilterFeature{
		query: &queryCopy,
	}
}

// Process applies the static filter to the query builder and processes the result.
//
// It adds the predefined query to the query builder's "must" clause.
//
// Example:
//
//	// Use the static filter in a feature chain
//	result, err := staticFilter.Process(builder, nextFeature)
func (sff *StaticFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	if sff.query != nil {
		builder.With(*sff.query)
	}
	return next(builder)
}

// WithRequiredProperty adds a filter that requires the specified property to exist.
//
// Example:
//
//	// Create a static filter that requires the "email" field to exist
//	staticFilter := featureset.NewStaticFilterFeature(
//	    featureset.WithRequiredProperty("email"),
//	)
func WithRequiredProperty(property string) StaticFilterOption {
	return func(sff *StaticFilterFeature) {
		existsQuery := types.Query{
			Exists: &types.ExistsQuery{
				Field: property,
			},
		}

		if sff.query == nil {
			sff.query = &types.Query{
				Bool: &types.BoolQuery{
					Must: []types.Query{existsQuery},
				},
			}
		} else {
			if sff.query.Bool.Must == nil {
				sff.query.Bool.Must = []types.Query{existsQuery}
			} else {
				sff.query.Bool.Must = append(sff.query.Bool.Must, existsQuery)
			}
		}
	}
}

// WithRequiredValue adds a filter that requires the specified property to have the specified value.
//
// Example:
//
//	// Create a static filter that requires the "status" field to be "active"
//	staticFilter := featureset.NewStaticFilterFeature(
//	    featureset.WithRequiredValue("status", "active"),
//	)
func WithRequiredValue(property string, value any) StaticFilterOption {
	return func(sff *StaticFilterFeature) {
		termQuery := types.Query{
			Term: map[string]types.TermQuery{
				property: {Value: value},
			},
		}

		if sff.query == nil {
			sff.query = &types.Query{
				Bool: &types.BoolQuery{
					Must: []types.Query{termQuery},
				},
			}
		} else {
			if sff.query.Bool.Must == nil {
				sff.query.Bool.Must = []types.Query{termQuery}
			} else {
				sff.query.Bool.Must = append(sff.query.Bool.Must, termQuery)
			}
		}
	}
}

package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald"
)

type BooleanFilterFeature struct {
	property   string
	nested     bool
	nestedPath string
}

type BooleanFilterOption func(*BooleanFilterFeature)

func WithNestedPath(path string) BooleanFilterOption {
	return func(bff *BooleanFilterFeature) {
		bff.nested = true
		bff.nestedPath = path
	}
}

func NewBooleanFilterFeature(property string, opts ...BooleanFilterOption) *BooleanFilterFeature {
	bff := &BooleanFilterFeature{
		property: property,
		nested:   false,
	}

	for _, opt := range opts {
		opt(bff)
	}

	return bff
}

func (bff *BooleanFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	param, err := builder.Request().Get(bff.property)
	if err == nil && param.IsTruthy() {
		bff.build(builder)
	}

	result, err := next(builder)
	if err != nil {
		return nil, err
	}

	return bff.handle(result)
}

func (bff *BooleanFilterFeature) build(builder *reveald.QueryBuilder) {
	// Create a term query for the boolean field
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
				Query: &termQuery,
			},
		}
		builder.With(nestedQuery)
	}
}

func (bff *BooleanFilterFeature) handle(result *reveald.Result) (*reveald.Result, error) {
	// With the official client, we no longer need to process the raw result
	// as the aggregations are already parsed and available in Result.Aggregations
	return result, nil
}

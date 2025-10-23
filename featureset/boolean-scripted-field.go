package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/runtimefieldtype"
	"github.com/reveald/reveald/v1"
)

// BooleanScriptedFieldFeature creates a boolean scripted field and optionally filters on it.
//
// This feature allows you to create scripted fields that return boolean values and
// automatically filter on them based on request parameters. This is particularly useful
// for creating dynamic boolean conditions like "is_expensive" (price > 100) or
// "is_recent" (created_date > now - 30d).
//
// Example usage:
//
//	// Create a feature that adds an "is_expensive" scripted field
//	feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100")
//
//	// Create a feature that also filters when the parameter is provided
//	featureWithFilter := NewBooleanScriptedFieldFeature(
//		"is_expensive",
//		"doc['price'].value > 100",
//		WithFiltering(),
//	)
//
// When WithFiltering() is enabled, the feature will automatically add a script query
// filter when the request contains a parameter matching the field name.
// For example:
// - "is_expensive=true" filters to show only documents where the script condition is true
// - "is_expensive=false" filters to show only documents where the script condition is false
type BooleanScriptedFieldFeature struct {
	fieldName string
	script    string
	filter    bool // Whether to also apply filtering based on request parameters
}

type BooleanScriptedFieldOption func(*BooleanScriptedFieldFeature)

// WithFiltering enables automatic filtering based on request parameters
func WithFiltering() BooleanScriptedFieldOption {
	return func(bsff *BooleanScriptedFieldFeature) {
		bsff.filter = true
	}
}

// NewBooleanScriptedFieldFeature creates a new boolean scripted field feature
func NewBooleanScriptedFieldFeature(fieldName, script string, opts ...BooleanScriptedFieldOption) *BooleanScriptedFieldFeature {
	bsff := &BooleanScriptedFieldFeature{
		fieldName: fieldName,
		script:    script,
		filter:    false,
	}

	for _, opt := range opts {
		opt(bsff)
	}

	return bsff
}

func (bsff *BooleanScriptedFieldFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	// Always add the scripted field
	bsff.addScriptedField(builder)

	return next(builder)
}

func (bsff *BooleanScriptedFieldFeature) addScriptedField(builder *reveald.QueryBuilder) {
	builder.WithRuntimeMappings(map[string]types.RuntimeField{
		bsff.fieldName: {
			Script: &types.Script{
				Source: &bsff.script,
			},
			Type: runtimefieldtype.Boolean,
		},
	})
}

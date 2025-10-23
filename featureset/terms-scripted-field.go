package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v1"
)

// TermsScriptedFieldFeature creates a terms-based scripted field and optionally filters on it.
//
// This feature allows you to create scripted fields that return string values and
// automatically filter on them based on request parameters. This is particularly useful
// for creating dynamic categorizations like "price_range" (low/medium/high) or
// "user_segment" (premium/standard/basic).
//
// Example usage:
//
//	// Create a feature that adds a "price_range" scripted field
//	script := `
//		if (doc['price'].value < 50) {
//			return 'low';
//		} else if (doc['price'].value <= 150) {
//			return 'medium';
//		} else {
//			return 'high';
//		}
//	`
//	feature := NewTermsScriptedFieldFeature("price_range", script)
//
//	// Create a feature that also filters when parameters are provided
//	featureWithFilter := NewTermsScriptedFieldFeature(
//		"price_range",
//		script,
//		WithTermsFiltering(),
//	)
//
// When WithTermsFiltering() is enabled, the feature will automatically add script query
// filters when the request contains parameters matching the field name.
// For example, if the request has "price_range=high" or "price_range=low,high",
// it will filter to only show documents where the script result matches those values.
type TermsScriptedFieldFeature struct {
	fieldName string
	script    string
	filter    bool // Whether to also apply filtering based on request parameters
}

type TermsScriptedFieldOption func(*TermsScriptedFieldFeature)

// WithTermsFiltering enables automatic filtering based on request parameters
func WithTermsFiltering() TermsScriptedFieldOption {
	return func(tsff *TermsScriptedFieldFeature) {
		tsff.filter = true
	}
}

// NewTermsScriptedFieldFeature creates a new terms scripted field feature
func NewTermsScriptedFieldFeature(fieldName, script string, opts ...TermsScriptedFieldOption) *TermsScriptedFieldFeature {
	tsff := &TermsScriptedFieldFeature{
		fieldName: fieldName,
		script:    script,
		filter:    false,
	}

	for _, opt := range opts {
		opt(tsff)
	}

	return tsff
}

func (tsff *TermsScriptedFieldFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	// Always add the scripted field
	tsff.addScriptedField(builder)

	// Optionally add filtering if enabled and parameter exists
	if tsff.filter {
		param, err := builder.Request().Get(tsff.fieldName)
		if err == nil && len(param.Values()) > 0 {
			tsff.addScriptFilter(builder, param.Values())
		}
	}

	return next(builder)
}

func (tsff *TermsScriptedFieldFeature) addScriptedField(builder *reveald.QueryBuilder) {
	source := tsff.script
	script := &types.Script{
		Source: &source,
	}
	builder.WithScriptedField(tsff.fieldName, script)
}

func (tsff *TermsScriptedFieldFeature) addScriptFilter(builder *reveald.QueryBuilder, values []string) {
	// For terms filtering on scripted fields, we need to create a bool query
	// with should clauses for each value
	if len(values) == 1 {
		// Single value - use script query with equality check
		filterScript := tsff.createEqualityScript(values[0])
		scriptQuery := types.Query{
			Script: &types.ScriptQuery{
				Script: types.Script{
					Source: &filterScript,
				},
			},
		}
		builder.With(scriptQuery)
	} else {
		// Multiple values - use bool query with should clauses
		shouldClauses := make([]types.Query, 0, len(values))
		for _, value := range values {
			filterScript := tsff.createEqualityScript(value)
			shouldClauses = append(shouldClauses, types.Query{
				Script: &types.ScriptQuery{
					Script: types.Script{
						Source: &filterScript,
					},
				},
			})
		}

		boolQuery := types.Query{
			Bool: &types.BoolQuery{
				Should: shouldClauses,
				MinimumShouldMatch: func() *types.MinimumShouldMatch {
					msm := types.MinimumShouldMatch("1")
					return &msm
				}(),
			},
		}
		builder.With(boolQuery)
	}
}

func (tsff *TermsScriptedFieldFeature) createEqualityScript(value string) string {
	// Create a script that checks if the scripted field result equals the given value
	// For complex scripts, we need to wrap them in a function-like structure
	return `
		def scriptResult = ` + tsff.script + `;
		return scriptResult == '` + value + `';
	`
}

package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald"
)

type ScriptedFieldFeature struct {
	fieldName string
	script    string
}

func NewScriptedFieldFeature(fieldName, script string) *ScriptedFieldFeature {
	return &ScriptedFieldFeature{fieldName, script}
}

func (sff *ScriptedFieldFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	sff.build(builder)
	return next(builder)
}

func (sff *ScriptedFieldFeature) build(builder *reveald.QueryBuilder) {
	// Create script directly with typed objects
	source := sff.script
	script := &types.Script{
		Source: &source,
	}

	builder.WithScriptedField(sff.fieldName, script)
}

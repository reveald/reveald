package featureset

import (
	"github.com/olivere/elastic/v7"
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
	builder.WithScriptedField(elastic.NewScriptField(sff.fieldName, elastic.NewScript(sff.script)))
}

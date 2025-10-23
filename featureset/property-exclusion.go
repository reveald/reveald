package featureset

import "github.com/reveald/reveald/v2"

type PropertyExclusionFeature struct {
	properties []string
}

func NewPropertyExclusionFeature(properties ...string) *PropertyExclusionFeature {
	return &PropertyExclusionFeature{properties}
}

func (pef *PropertyExclusionFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	builder.
		Selection().
		Update(reveald.WithoutProperties(pef.properties...))

	return next(builder)
}

package featureset

import "github.com/reveald/reveald"

type PropertyInclusionFeature struct {
	properties []string
}

func NewPropertyInclusionFeature(properties ...string) *PropertyInclusionFeature {
	return &PropertyInclusionFeature{properties}
}

func (pif *PropertyInclusionFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	builder.
		Selection().
		Update(reveald.WithProperties(pif.properties...))

	return next(builder)
}

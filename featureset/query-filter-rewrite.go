package featureset

import (
	"strings"

	"github.com/reveald/reveald/v2"
)

type rewriteSource struct {
	property string
	matches  []string
}

type QueryFilterRewriteFeature struct {
	name    string
	sources []*rewriteSource
}

type QueryFilterRewriteOption func(*QueryFilterRewriteFeature)

func WithRewriteQueryParam(name string) QueryFilterRewriteOption {
	return func(qfrf *QueryFilterRewriteFeature) {
		qfrf.name = name
	}
}

func WithRewriteMatcher(property string, matches ...string) QueryFilterRewriteOption {
	return func(qfrf *QueryFilterRewriteFeature) {
		qfrf.sources = append(qfrf.sources, &rewriteSource{property, matches})
	}
}

func NewQueryFilterRewriteFeature(opts ...QueryFilterRewriteOption) *QueryFilterRewriteFeature {
	var sources []*rewriteSource
	qfrf := &QueryFilterRewriteFeature{
		name:    "q",
		sources: sources,
	}

	for _, opt := range opts {
		opt(qfrf)
	}

	return qfrf
}

func (qfrf *QueryFilterRewriteFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	if !builder.Request().Has(qfrf.name) {
		return next(builder)
	}

	p, err := builder.Request().Get(qfrf.name)
	if err != nil {
		return next(builder)
	}

	builder.Request().Del(qfrf.name)

	var values []string
	for _, v := range p.Values() {
		normalized := strings.ReplaceAll(v, "  ", " ")
		parts := strings.Split(normalized, " ")
		values = append(values, parts...)
	}

	var output []string
	completed := make(map[string]bool)
	for _, v := range values {
		for _, s := range qfrf.sources {
			for _, m := range s.matches {
				e, ok := completed[v]
				if e && ok {
					continue
				}
				completed[v] = true

				if v == m {
					builder.Request().Append(
						reveald.NewParameter(s.property, v))
					break
				} else {
					output = append(output, v)
				}
			}
		}
	}

	builder.Request().Set(qfrf.name, output...)
	return next(builder)
}

package featureset

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/operator"
	"github.com/reveald/reveald/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewQueryFilterFeature(t *testing.T) {
	t.Run("defaults to any-term matching", func(t *testing.T) {
		feature := NewQueryFilterFeature()

		assert.Equal(t, operator.Or, feature.operator)
	})

	t.Run("configures all-term matching", func(t *testing.T) {
		feature := NewQueryFilterFeature(WithMatchingOperator(operator.And))

		assert.Equal(t, operator.And, feature.operator)
	})
}

func TestQueryFilterFeatureProcess(t *testing.T) {
	feature := NewQueryFilterFeature(
		WithFields("title", "description"),
		WithMatchingOperator(operator.And),
	)
	builder := reveald.NewQueryBuilder(
		reveald.NewRequest(reveald.NewParameter("q", "quick brown")),
		"-",
	)

	_, err := feature.Process(builder, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return nil, nil
	})

	assert.NoError(t, err)
	query := builder.RawQuery()
	if assert.NotNil(t, query.Bool) && assert.Len(t, query.Bool.Must, 1) {
		queryString := query.Bool.Must[0].QueryString
		if assert.NotNil(t, queryString) {
			assert.Equal(t, "quick brown", queryString.Query)
			assert.Equal(t, operator.And, *queryString.DefaultOperator)
			assert.Equal(t, []string{"title", "description"}, queryString.Fields)
		}
	}
}
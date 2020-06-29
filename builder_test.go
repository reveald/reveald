package reveald

import (
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

func Test_That_With_Adds_Query_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")
	q := elastic.NewTermQuery("property", "value")
	builder.With(q)

	actual := builder.Build()
	expected := elastic.NewSearchSource().
		Query(elastic.NewBoolQuery().Must(q))

	assert.Equal(t, expected, actual)
}

func Test_That_Without_Adds_Query_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")
	q := elastic.NewTermQuery("property", "value")
	builder.Without(q)

	actual := builder.Build()
	expected := elastic.NewSearchSource().
		Query(elastic.NewBoolQuery().MustNot(q))

	assert.Equal(t, expected, actual)
}

func Test_That_Boost_Adds_Query_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")
	q := elastic.NewTermQuery("property", "value")
	builder.Boost(q)

	actual := builder.Build()
	expected := elastic.NewSearchSource().
		Query(elastic.NewBoolQuery().Should(q))

	assert.Equal(t, expected, actual)
}

func Test_That_Aggregation_Adds_Aggregation_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")
	agg := elastic.NewTermsAggregation().Field("property")
	builder.Aggregation("property", agg)

	actual := builder.Build()
	expected := elastic.NewSearchSource().
		Query(elastic.NewBoolQuery()).Aggregation("property", agg)

	assert.Equal(t, expected, actual)
}

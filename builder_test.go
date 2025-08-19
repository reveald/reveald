package reveald

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/stretchr/testify/assert"
)

func Test_That_With_Adds_Query_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")

	// Create typed query
	q := types.Query{
		Term: map[string]types.TermQuery{
			"property": {Value: "value"},
		},
	}
	builder.With(q)

	actual := builder.Build()

	// For comparison, convert to map
	qMap := termQueryToMap("property", "value", true)
	expected := map[string]any{
		"from": float64(0),
		"size": float64(24),
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{qMap},
			},
		},
	}

	assert.Equal(t, expected, actual)
}

func Test_That_Without_Adds_Query_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")

	// Create typed query
	q := types.Query{
		Term: map[string]types.TermQuery{
			"property": {Value: "value"},
		},
	}
	builder.Without(q)

	actual := builder.Build()

	// For comparison, convert to map
	qMap := termQueryToMap("property", "value", true)
	expected := map[string]any{
		"from": float64(0),
		"size": float64(24),
		"query": map[string]any{
			"bool": map[string]any{
				"must_not": []any{qMap},
			},
		},
	}

	assert.Equal(t, expected, actual)
}

func Test_That_Boost_Adds_Query_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")

	// Create typed query
	q := types.Query{
		Term: map[string]types.TermQuery{
			"property": {Value: "value"},
		},
	}
	builder.Boost(q)

	actual := builder.Build()

	// For comparison, convert to map
	qMap := termQueryToMap("property", "value", true)
	expected := map[string]any{
		"from": float64(0),
		"size": float64(24),
		"query": map[string]any{
			"bool": map[string]any{
				"should": []any{qMap},
			},
		},
	}

	assert.Equal(t, expected, actual)
}

func Test_That_Aggregation_Adds_Aggregation_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")

	// Create typed aggregation
	field := "property"
	agg := types.Aggregations{
		Terms: &types.TermsAggregation{
			Field: &field,
		},
	}
	builder.Aggregation("property", agg)

	actual := builder.Build()

	// For comparison, convert to map
	aggMap := termsAggregationToMap("property")
	expected := map[string]any{
		"from": float64(0),
		"size": float64(24),
		"query": map[string]any{
			"bool": map[string]any{},
		},
		"aggregations": map[string]any{
			"property": aggMap,
		},
	}

	assert.Equal(t, expected, actual)
}

func Test_That_PostFilter_Adds_To_Source(t *testing.T) {
	builder := NewQueryBuilder(nil, "idx")

	// Create typed query
	q := types.Query{
		Term: map[string]types.TermQuery{
			"property": {Value: "value"},
		},
	}

	builder.PostFilterWith(q)
	builder.PostFilterWithout(q)
	builder.PostFilterBoost(q)

	actual := builder.Build()

	// For comparison, convert to map
	qMap := termQueryToMap("property", "value", true)
	expected := map[string]any{
		"from": float64(0),
		"size": float64(24),
		"query": map[string]any{
			"bool": map[string]any{},
		},
		"post_filter": map[string]any{
			"bool": map[string]any{
				"must":     []any{qMap},
				"must_not": []any{qMap},
				"should":   []any{qMap},
			},
		},
	}

	assert.Equal(t, expected, actual)
}

// Helper functions to create map representations of queries and aggregations for testing
func termQueryToMap(property string, value any, typed bool) map[string]any {
	if typed {
		return map[string]any{
			"term": map[string]any{
				property: map[string]any{
					"value": value,
				},
			},
		}
	}

	return map[string]any{
		"term": map[string]any{
			property: value,
		},
	}
}

func termsAggregationToMap(field string) map[string]any {
	return map[string]any{
		"terms": map[string]any{
			"field": field,
		},
	}
}

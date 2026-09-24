package featureset

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
	"github.com/stretchr/testify/assert"
)

// Note: Full Process() flow tests including aggregation handling are in integration tests
// Unit tests focus on construction and configuration as Result type has private response field

func TestNewDynamicFilterFeature(t *testing.T) {
	t.Run("without options", func(t *testing.T) {
		feature := NewDynamicFilterFeature("category")
		assert.Equal(t, "category", feature.property)
		assert.False(t, feature.nested)
		assert.Equal(t, defaultAggregationSize, feature.agg.size)
		assert.Empty(t, feature.agg.missingValue)
	})

	t.Run("with custom size", func(t *testing.T) {
		feature := NewDynamicFilterFeature("category", WithAggregationSize(20))
		assert.Equal(t, 20, feature.agg.size)
		assert.Empty(t, feature.agg.missingValue)
	})

	t.Run("with include missing", func(t *testing.T) {
		feature := NewDynamicFilterFeature("category", WithMissingValueAs("no-category"))
		assert.Equal(t, "no-category", feature.agg.missingValue)
		assert.Equal(t, defaultAggregationSize, feature.agg.size)
	})

	t.Run("with both options", func(t *testing.T) {
		feature := NewDynamicFilterFeature("category",
			WithAggregationSize(50),
			WithMissingValueAs("missing-category"),
		)
		assert.Equal(t, 50, feature.agg.size)
		assert.Equal(t, "missing-category", feature.agg.missingValue)
	})
}

func TestNewNestedDocumentFilterFeature(t *testing.T) {
	t.Run("without options", func(t *testing.T) {
		feature := NewNestedDocumentFilterFeature("tags.name")
		assert.Equal(t, "tags.name", feature.property)
		assert.True(t, feature.nested)
		assert.Equal(t, defaultAggregationSize, feature.agg.size)
		assert.Empty(t, feature.agg.missingValue)
	})

	t.Run("with include missing", func(t *testing.T) {
		feature := NewNestedDocumentFilterFeature("tags.name", WithMissingValueAs("no-tags"))
		assert.Equal(t, "no-tags", feature.agg.missingValue)
		assert.True(t, feature.nested)
	})

	t.Run("with both options", func(t *testing.T) {
		feature := NewNestedDocumentFilterFeature("tags.name",
			WithAggregationSize(25),
			WithMissingValueAs("missing-tags"),
		)
		assert.Equal(t, 25, feature.agg.size)
		assert.Equal(t, "missing-tags", feature.agg.missingValue)
		assert.True(t, feature.nested)
	})
}

// Note: Full Process() flow tests with query building and aggregation handling
// are covered in integration tests as they require proper Elasticsearch responses

func TestDynamicFilterFeature_Build(t *testing.T) {
	build := func(feature *DynamicFilterFeature, params ...reveald.Parameter) *types.BoolQuery {
		builder := reveald.NewQueryBuilder(reveald.NewRequest(params...), "index")
		feature.build(builder)
		return builder.RawQuery().Bool
	}

	t.Run("single value", func(t *testing.T) {
		q := build(NewDynamicFilterFeature("category"),
			reveald.NewParameter("category", "news"))

		assert.Len(t, q.Must, 1)
		assert.Equal(t, "news", q.Must[0].Term["category.keyword"].Value)
		assert.Empty(t, q.MustNot)
	})

	t.Run("excluded value", func(t *testing.T) {
		q := build(NewDynamicFilterFeature("category"),
			reveald.NewParameter("category.not", "feature"))

		assert.Empty(t, q.Must)
		assert.Len(t, q.MustNot, 1)
		assert.Len(t, q.MustNot[0].Bool.Should, 1)
		assert.Equal(t, "feature", q.MustNot[0].Bool.Should[0].Term["category.keyword"].Value)
	})

	t.Run("included and excluded values", func(t *testing.T) {
		q := build(NewDynamicFilterFeature("category"),
			reveald.NewParameter("category", "news", "blog"),
			reveald.NewParameter("category.not", "feature", "draft"))

		assert.Len(t, q.Must, 1)
		assert.Len(t, q.Must[0].Bool.Should, 2)
		assert.Len(t, q.MustNot, 1)
		assert.Len(t, q.MustNot[0].Bool.Should, 2)
		assert.Equal(t, "draft", q.MustNot[0].Bool.Should[1].Term["category.keyword"].Value)
	})

	t.Run("excluded missing value", func(t *testing.T) {
		q := build(NewDynamicFilterFeature("category", WithMissingValueAs("none")),
			reveald.NewParameter("category.not", "none"))

		assert.Len(t, q.MustNot, 1)
		missing := q.MustNot[0].Bool.Should[0]
		assert.Equal(t, "category.keyword", missing.Bool.MustNot[0].Exists.Field)
	})

	t.Run("excluded nested value", func(t *testing.T) {
		q := build(NewNestedDocumentFilterFeature("tags.name"),
			reveald.NewParameter("tags.name.not", "feature"))

		assert.Empty(t, q.Must)
		assert.Len(t, q.MustNot, 1)
		assert.Equal(t, "tags", q.MustNot[0].Nested.Path)
		assert.Equal(t, "feature", q.MustNot[0].Nested.Query.Bool.Should[0].Term["tags.name.keyword"].Value)
	})
}

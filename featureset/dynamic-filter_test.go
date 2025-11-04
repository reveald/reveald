package featureset

import (
	"testing"

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

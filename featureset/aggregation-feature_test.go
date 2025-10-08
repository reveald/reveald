package featureset

import (
	"testing"

	"github.com/reveald/reveald"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregationFeature(t *testing.T) {
	t.Run("NewAggregationFeature", func(t *testing.T) {
		// Test creating without options
		feature := NewAggregationFeature("category")
		assert.Equal(t, "category", feature.field)
		assert.Equal(t, defaultAggregationSize, feature.size)

		// Test creating with custom size
		featureWithSize := NewAggregationFeature("category", WithAggregationSize(25))
		assert.Equal(t, "category", featureWithSize.field)
		assert.Equal(t, 25, featureWithSize.size)
	})

	t.Run("ProcessBasicFunctionality", func(t *testing.T) {
		feature := NewAggregationFeature("category")

		// Create a query builder
		builder := reveald.NewQueryBuilder(nil, "test-index")

		// Mock next function that returns a result with mock aggregation data
		nextCalled := false
		mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			nextCalled = true
			
			// Create a mock result with aggregation data
			result := &reveald.Result{
				Aggregations: make(map[string][]*reveald.ResultBucket),
			}
			
			// Add mock raw aggregations (this would normally come from Elasticsearch)
			// For testing purposes, we'll simulate the aggregation structure
			return result, nil
		}

		// Process the feature
		result, err := feature.Process(builder, mockNext)

		// Verify results
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, nextCalled, "Next function should be called")
	})

	t.Run("WithAggregationSize", func(t *testing.T) {
		feature := NewAggregationFeature("category", WithAggregationSize(50))
		assert.Equal(t, "category", feature.field)
		assert.Equal(t, 50, feature.size)
	})

	t.Run("FeatureInterfaceCompliance", func(t *testing.T) {
		// Verify that AggregationFeature implements the Feature interface
		var _ reveald.Feature = NewAggregationFeature("category")
	})

	t.Run("BuildAggregationFeatureBackwardCompatibility", func(t *testing.T) {
		// Test that buildAggregationFeature still works for backward compatibility
		agg := buildAggregationFeature(WithAggregationSize(15))
		assert.Equal(t, "", agg.field) // Field should be empty for component usage
		assert.Equal(t, 15, agg.size)
	})
}

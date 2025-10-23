package featureset

import (
	"testing"

	"github.com/reveald/reveald/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBooleanScriptedFieldFeature(t *testing.T) {
	t.Run("NewBooleanScriptedFieldFeature", func(t *testing.T) {
		// Test creating without options
		feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100")
		assert.Equal(t, "is_expensive", feature.fieldName)
		assert.Equal(t, "doc['price'].value > 100", feature.script)
		assert.False(t, feature.filter, "Filter should be disabled by default")

		// Test creating with filtering option
		featureWithFilter := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100", WithFiltering())
		assert.Equal(t, "is_expensive", featureWithFilter.fieldName)
		assert.Equal(t, "doc['price'].value > 100", featureWithFilter.script)
		assert.True(t, featureWithFilter.filter, "Filter should be enabled")
	})

	t.Run("ProcessBasicFunctionality", func(t *testing.T) {
		feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100")

		// Create a query builder
		builder := reveald.NewQueryBuilder(nil, "test-index")

		// Mock next function
		nextCalled := false
		mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			nextCalled = true
			return &reveald.Result{}, nil
		}

		// Process the feature
		result, err := feature.Process(builder, mockNext)

		// Verify results
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, nextCalled, "Next function should be called")
	})

	t.Run("ProcessWithFilteringEnabled", func(t *testing.T) {
		feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100", WithFiltering())

		// Create a request with the parameter
		request := reveald.NewRequest(reveald.NewParameter("is_expensive", "true"))
		builder := reveald.NewQueryBuilder(request, "test-index")

		// Mock next function
		nextCalled := false
		mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			nextCalled = true
			return &reveald.Result{}, nil
		}

		// Process the feature
		result, err := feature.Process(builder, mockNext)

		// Verify results
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, nextCalled, "Next function should be called")
	})

	t.Run("ProcessWithFilteringForFalsyParameter", func(t *testing.T) {
		feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100", WithFiltering())

		// Create a request with a falsy parameter
		request := reveald.NewRequest(reveald.NewParameter("is_expensive", "false"))
		builder := reveald.NewQueryBuilder(request, "test-index")

		// Mock next function
		nextCalled := false
		mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			nextCalled = true
			return &reveald.Result{}, nil
		}

		// Process the feature
		result, err := feature.Process(builder, mockNext)

		// Verify results
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, nextCalled, "Next function should be called")
	})

	t.Run("ProcessWithFilteringForEmptyParameter", func(t *testing.T) {
		feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100", WithFiltering())

		// Create a request with an empty parameter (should not trigger filtering)
		request := reveald.NewRequest(reveald.NewParameter("is_expensive", ""))
		builder := reveald.NewQueryBuilder(request, "test-index")

		// Mock next function
		nextCalled := false
		mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			nextCalled = true
			return &reveald.Result{}, nil
		}

		// Process the feature
		result, err := feature.Process(builder, mockNext)

		// Verify results
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, nextCalled, "Next function should be called")
	})

	t.Run("ParameterHandling", func(t *testing.T) {
		// Test with truthy parameter
		request := reveald.NewRequest(reveald.NewParameter("is_expensive", "true"))
		builder := reveald.NewQueryBuilder(request, "test-index")

		param, err := builder.Request().Get("is_expensive")
		require.NoError(t, err)
		assert.True(t, param.IsTruthy(), "Parameter should be truthy")

		// Test with falsy parameter
		request2 := reveald.NewRequest(reveald.NewParameter("is_expensive", "false"))
		builder2 := reveald.NewQueryBuilder(request2, "test-index")

		param2, err := builder2.Request().Get("is_expensive")
		require.NoError(t, err)
		assert.False(t, param2.IsTruthy(), "Parameter should be falsy")

		// Test with missing parameter (empty request)
		emptyRequest := reveald.NewRequest()
		builder3 := reveald.NewQueryBuilder(emptyRequest, "test-index")

		_, err = builder3.Request().Get("is_expensive")
		assert.Error(t, err, "Should error when parameter is missing")
	})

	t.Run("ScriptFilterGeneration", func(t *testing.T) {
		feature := NewBooleanScriptedFieldFeature("is_expensive", "doc['price'].value > 100", WithFiltering())

		// Test truthy parameter - should use original script
		request1 := reveald.NewRequest(reveald.NewParameter("is_expensive", "true"))
		builder1 := reveald.NewQueryBuilder(request1, "test-index")

		// Mock next function
		mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil
		}

		// Process with truthy parameter
		_, err := feature.Process(builder1, mockNext)
		require.NoError(t, err)

		// Test falsy parameter - should use negated script
		request2 := reveald.NewRequest(reveald.NewParameter("is_expensive", "false"))
		builder2 := reveald.NewQueryBuilder(request2, "test-index")

		// Process with falsy parameter
		_, err = feature.Process(builder2, mockNext)
		require.NoError(t, err)

		// This test mainly ensures no errors occur during processing
		// The actual script verification would require access to builder internals
		// which is not exposed in the current API
	})
}

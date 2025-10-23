package featureset

import (
	"testing"

	"github.com/reveald/reveald/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTermsScriptedFieldFeature(t *testing.T) {
	t.Run("NewTermsScriptedFieldFeature", func(t *testing.T) {
		// Test creating without options
		script := `
			if (doc['price'].value < 50) {
				return 'low';
			} else if (doc['price'].value <= 150) {
				return 'medium';
			} else {
				return 'high';
			}
		`
		feature := NewTermsScriptedFieldFeature("price_range", script)
		assert.Equal(t, "price_range", feature.fieldName)
		assert.Equal(t, script, feature.script)
		assert.False(t, feature.filter, "Filter should be disabled by default")

		// Test creating with filtering option
		featureWithFilter := NewTermsScriptedFieldFeature("price_range", script, WithTermsFiltering())
		assert.Equal(t, "price_range", featureWithFilter.fieldName)
		assert.Equal(t, script, featureWithFilter.script)
		assert.True(t, featureWithFilter.filter, "Filter should be enabled")
	})

	t.Run("ProcessBasicFunctionality", func(t *testing.T) {
		script := "doc['category'].value"
		feature := NewTermsScriptedFieldFeature("category_field", script)

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
		script := "doc['category'].value"
		feature := NewTermsScriptedFieldFeature("category_field", script, WithTermsFiltering())

		// Create a request with the parameter
		request := reveald.NewRequest(reveald.NewParameter("category_field", "electronics"))
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

	t.Run("ProcessWithMultipleValues", func(t *testing.T) {
		script := "doc['category'].value"
		feature := NewTermsScriptedFieldFeature("category_field", script, WithTermsFiltering())

		// Create a request with multiple values
		request := reveald.NewRequest(
			reveald.NewParameter("category_field", "electronics"),
			reveald.NewParameter("category_field", "fashion"),
		)
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

	t.Run("CreateEqualityScript", func(t *testing.T) {
		script := "doc['category'].value"
		feature := NewTermsScriptedFieldFeature("category_field", script, WithTermsFiltering())

		// Test the equality script creation
		equalityScript := feature.createEqualityScript("electronics")
		expected := `
		def scriptResult = doc['category'].value;
		return scriptResult == 'electronics';
	`
		assert.Equal(t, expected, equalityScript)
	})

	t.Run("ParameterHandling", func(t *testing.T) {
		// Test with single parameter
		request := reveald.NewRequest(reveald.NewParameter("category_field", "electronics"))
		builder := reveald.NewQueryBuilder(request, "test-index")

		param, err := builder.Request().Get("category_field")
		require.NoError(t, err)
		assert.Equal(t, []string{"electronics"}, param.Values())

		// Test with multiple parameters
		request2 := reveald.NewRequest(
			reveald.NewParameter("category_field", "electronics"),
			reveald.NewParameter("category_field", "fashion"),
		)
		builder2 := reveald.NewQueryBuilder(request2, "test-index")

		param2, err := builder2.Request().Get("category_field")
		require.NoError(t, err)
		values := param2.Values()
		assert.Len(t, values, 2, "Should have 2 values")
		assert.Contains(t, values, "electronics", "Should contain electronics")
		assert.Contains(t, values, "fashion", "Should contain fashion")

		// Test with missing parameter (empty request)
		emptyRequest := reveald.NewRequest()
		builder3 := reveald.NewQueryBuilder(emptyRequest, "test-index")

		_, err = builder3.Request().Get("category_field")
		assert.Error(t, err, "Should error when parameter is missing")
	})
}

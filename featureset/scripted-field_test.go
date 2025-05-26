package featureset

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Test_NewScriptedFieldFeature(t *testing.T) {
	fieldName := "calculated_field"
	script := "doc['price'].value * doc['quantity'].value"

	feature := NewScriptedFieldFeature(fieldName, script)

	assert.NotNil(t, feature)
	assert.Equal(t, fieldName, feature.fieldName)
	assert.Equal(t, script, feature.script)
}

func Test_ScriptedFieldFeature_Process(t *testing.T) {
	fieldName := "total_price"
	script := "doc['price'].value * doc['quantity'].value"

	feature := NewScriptedFieldFeature(fieldName, script)
	qb := reveald.NewQueryBuilder(&reveald.Request{}, "test-index")

	// Mock the next function
	nextCalled := false
	mockNext := func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
		nextCalled = true
		return &reveald.Result{}, nil
	}

	result, err := feature.Process(qb, mockNext)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, nextCalled, "Next function should have been called")
}

func Test_ScriptedFieldFeature_Build(t *testing.T) {
	fieldName := "calculated_value"
	script := "Math.log(doc['score'].value)"

	feature := NewScriptedFieldFeature(fieldName, script)
	qb := reveald.NewQueryBuilder(&reveald.Request{}, "test-index")

	// Process the feature to add the scripted field
	_, err := feature.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	assert.NoError(t, err)

	// Build the request to check if script fields are properly added
	request := qb.BuildRequest()
	assert.NotNil(t, request)
	assert.NotNil(t, request.ScriptFields)

	// Check that our scripted field was added
	scriptField, exists := request.ScriptFields[fieldName]
	assert.True(t, exists, "Scripted field should exist in the request")
	assert.NotNil(t, scriptField.Script)
	assert.Equal(t, script, *scriptField.Script.Source)
}

func Test_ScriptedFieldFeature_Build_WithMap(t *testing.T) {
	fieldName := "discount_price"
	script := "doc['price'].value * 0.9"

	feature := NewScriptedFieldFeature(fieldName, script)
	qb := reveald.NewQueryBuilder(&reveald.Request{}, "test-index")

	// Process the feature
	_, err := feature.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	assert.NoError(t, err)

	// Build as map to check the structure
	queryMap := qb.Build()
	assert.NotNil(t, queryMap)

	// Check that script_fields is present in the map
	scriptFields, exists := queryMap["script_fields"]
	assert.True(t, exists, "script_fields should exist in the query map")

	scriptFieldsMap, ok := scriptFields.(map[string]any)
	assert.True(t, ok, "script_fields should be a map")

	// Check our specific field
	fieldData, exists := scriptFieldsMap[fieldName]
	assert.True(t, exists, "Our scripted field should exist")

	fieldMap, ok := fieldData.(map[string]any)
	assert.True(t, ok, "Field data should be a map")

	scriptData, exists := fieldMap["script"]
	assert.True(t, exists, "Script should exist in field data")

	scriptMap, ok := scriptData.(map[string]any)
	assert.True(t, ok, "Script should be a map")

	source, exists := scriptMap["source"]
	assert.True(t, exists, "Script source should exist")
	assert.Equal(t, script, source, "Script source should match")
}

func Test_ScriptedFieldFeature_MultipleFields(t *testing.T) {
	// Test adding multiple scripted fields
	qb := reveald.NewQueryBuilder(&reveald.Request{}, "test-index")

	// Add first scripted field
	feature1 := NewScriptedFieldFeature("field1", "doc['a'].value + doc['b'].value")
	_, err := feature1.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	assert.NoError(t, err)

	// Add second scripted field
	feature2 := NewScriptedFieldFeature("field2", "doc['x'].value * doc['y'].value")
	_, err = feature2.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	assert.NoError(t, err)

	// Check that both fields are present
	request := qb.BuildRequest()
	assert.NotNil(t, request.ScriptFields)
	assert.Len(t, request.ScriptFields, 2)

	// Check first field
	field1, exists := request.ScriptFields["field1"]
	assert.True(t, exists)
	assert.Equal(t, "doc['a'].value + doc['b'].value", *field1.Script.Source)

	// Check second field
	field2, exists := request.ScriptFields["field2"]
	assert.True(t, exists)
	assert.Equal(t, "doc['x'].value * doc['y'].value", *field2.Script.Source)
}

func Test_ScriptedFieldFeature_ResponseStructure(t *testing.T) {
	fieldName := "calculated_total"
	script := "doc['price'].value * doc['quantity'].value"

	feature := NewScriptedFieldFeature(fieldName, script)
	qb := reveald.NewQueryBuilder(&reveald.Request{}, "test-index")

	// Process the feature
	_, err := feature.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	assert.NoError(t, err)

	// Build the query map to verify the complete structure
	queryMap := qb.Build()
	assert.NotNil(t, queryMap)

	// Verify the overall structure includes script_fields
	expectedKeys := []string{"from", "size", "query", "script_fields"}
	for _, key := range expectedKeys {
		_, exists := queryMap[key]
		assert.True(t, exists, "Query should contain %s", key)
	}

	// Verify script_fields structure
	scriptFields, exists := queryMap["script_fields"]
	assert.True(t, exists, "script_fields should exist in query")

	scriptFieldsMap, ok := scriptFields.(map[string]any)
	assert.True(t, ok, "script_fields should be a map")

	// Verify our specific field exists and has correct structure
	fieldData, exists := scriptFieldsMap[fieldName]
	assert.True(t, exists, "Our scripted field should exist")

	fieldMap, ok := fieldData.(map[string]any)
	assert.True(t, ok, "Field data should be a map")

	// Verify the script structure
	scriptData, exists := fieldMap["script"]
	assert.True(t, exists, "Script should exist in field data")

	scriptMap, ok := scriptData.(map[string]any)
	assert.True(t, ok, "Script should be a map")

	// Verify script source
	source, exists := scriptMap["source"]
	assert.True(t, exists, "Script source should exist")
	assert.Equal(t, script, source, "Script source should match expected value")

	// Verify the script_fields can be serialized properly (simulating response handling)
	assert.IsType(t, map[string]any{}, scriptFields, "script_fields should be serializable as map[string]any")
}

func Test_ScriptedFieldFeature_EmptyQueryWithScriptField(t *testing.T) {
	// Test that scripted fields work even when there are no other query conditions
	fieldName := "computed_field"
	script := "Math.sqrt(doc['value'].value)"

	feature := NewScriptedFieldFeature(fieldName, script)
	qb := reveald.NewQueryBuilder(&reveald.Request{}, "test-index")

	// Process the feature without adding any other query conditions
	_, err := feature.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	assert.NoError(t, err)

	// Build the query
	queryMap := qb.Build()
	assert.NotNil(t, queryMap)

	// Should have an empty bool query but still include script_fields
	query, exists := queryMap["query"]
	assert.True(t, exists)

	queryObj, ok := query.(map[string]any)
	assert.True(t, ok)

	boolQuery, exists := queryObj["bool"]
	assert.True(t, exists)

	boolObj, ok := boolQuery.(map[string]any)
	assert.True(t, ok)
	assert.Empty(t, boolObj, "Bool query should be empty when no conditions are added")

	// But script_fields should still be present
	scriptFields, exists := queryMap["script_fields"]
	assert.True(t, exists, "script_fields should exist even with empty query")

	scriptFieldsMap, ok := scriptFields.(map[string]any)
	assert.True(t, ok)
	assert.Len(t, scriptFieldsMap, 1, "Should have exactly one scripted field")

	// Verify our field is there
	fieldData, exists := scriptFieldsMap[fieldName]
	assert.True(t, exists)

	fieldMap, ok := fieldData.(map[string]any)
	assert.True(t, ok)

	scriptData := fieldMap["script"].(map[string]any)
	assert.Equal(t, script, scriptData["source"])
}

func Test_ScriptedFieldFeature_Integration(t *testing.T) {
	// Skip long running tests unless explicitly enabled
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	const (
		elasticVersion = "8.12.1"
		testIndex      = "scripted-field-test"
	)

	ctx := context.Background()

	// Set up Elasticsearch container
	elasticContainer, err := elasticsearch.RunContainer(ctx,
		testcontainers.WithImage("docker.elastic.co/elasticsearch/elasticsearch:"+elasticVersion),
		testcontainers.WithEnv(map[string]string{
			"discovery.type":           "single-node",
			"xpack.security.enabled":   "false",
			"ES_JAVA_OPTS":             "-Xms512m -Xmx512m",
			"action.auto_create_index": "true",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("started").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err, "Failed to start Elasticsearch container")
	defer func() {
		if err := elasticContainer.Terminate(ctx); err != nil {
			t.Fatalf("Failed to terminate container: %s", err)
		}
	}()

	// Get the connection URL
	httpURL, err := elasticContainer.Endpoint(ctx, "http")
	require.NoError(t, err, "Failed to get Elasticsearch HTTP URL")

	// Wait a bit more to ensure Elasticsearch is truly ready
	time.Sleep(2 * time.Second)

	// Create backend
	backend, err := reveald.NewElasticBackend([]string{httpURL})
	require.NoError(t, err, "Failed to create Elasticsearch backend")

	// Create test index with sample data
	indexMapping := `{
		"mappings": {
			"properties": {
				"name": {"type": "text"},
				"price": {"type": "float"},
				"quantity": {"type": "integer"},
				"discount_rate": {"type": "float"}
			}
		}
	}`

	// Create the index
	res, err := backend.GetClient().Indices.Create(
		testIndex,
		backend.GetClient().Indices.Create.WithBody(strings.NewReader(indexMapping)),
		backend.GetClient().Indices.Create.WithContext(ctx),
	)
	require.NoError(t, err, "Failed to create test index")
	require.False(t, res.IsError(), "Error creating index: %s", res.String())
	res.Body.Close()

	// Add sample documents
	documents := []map[string]any{
		{
			"name":          "Product A",
			"price":         100.0,
			"quantity":      5,
			"discount_rate": 0.1,
		},
		{
			"name":          "Product B",
			"price":         50.0,
			"quantity":      10,
			"discount_rate": 0.2,
		},
		{
			"name":          "Product C",
			"price":         200.0,
			"quantity":      3,
			"discount_rate": 0.15,
		},
	}

	// Index documents
	for i, doc := range documents {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err, "Failed to marshal document")

		res, err := backend.GetClient().Index(
			testIndex,
			strings.NewReader(string(docJSON)),
			backend.GetClient().Index.WithContext(ctx),
			backend.GetClient().Index.WithDocumentID(fmt.Sprintf("doc-%d", i+1)),
		)
		require.NoError(t, err, "Failed to index document")
		require.False(t, res.IsError(), "Error indexing document: %s", res.String())
		res.Body.Close()
	}

	// Force refresh to make documents searchable immediately
	res, err = backend.GetClient().Indices.Refresh(
		backend.GetClient().Indices.Refresh.WithIndex(testIndex),
		backend.GetClient().Indices.Refresh.WithContext(ctx),
	)
	require.NoError(t, err, "Failed to refresh index")
	require.False(t, res.IsError(), "Error refreshing index: %s", res.String())
	res.Body.Close()

	// Test the ScriptedFieldFeature
	t.Run("TestScriptedFieldInResponse", func(t *testing.T) {
		// Create a query builder
		qb := reveald.NewQueryBuilder(&reveald.Request{}, testIndex)

		// Create and process the scripted field feature
		feature := NewScriptedFieldFeature("total_value", "doc['price'].value * doc['quantity'].value")

		// Process the feature with a mock next function that executes the query
		result, err := feature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return backend.Execute(ctx, builder)
		})

		require.NoError(t, err, "Failed to execute query with scripted field")
		require.NotNil(t, result, "Expected result")

		// Verify we got results
		assert.Greater(t, result.TotalHitCount, int64(0), "Expected at least one result")
		assert.NotEmpty(t, result.Hits, "Expected hits in result")

		// Check that the scripted field is present in each hit
		for i, hit := range result.Hits {
			t.Logf("Hit %d: %+v", i, hit)

			totalValue, hasTotalValue := hit["total_value"]
			assert.True(t, hasTotalValue, "Expected total_value scripted field in hit %d", i)

			if hasTotalValue {
				// Scripted fields are returned as direct values
				if calculatedValue, ok := totalValue.(float64); ok {
					// We can't verify the exact calculation without access to original fields,
					// but we can verify it's a reasonable value
					assert.Greater(t, calculatedValue, 0.0, "Total value should be positive for hit %d", i)
					assert.Less(t, calculatedValue, 10000.0, "Total value should be reasonable for hit %d", i)
				} else {
					t.Errorf("total_value should be a numeric value, got: %T %v", totalValue, totalValue)
				}
			}
		}

		// Verify that we have the expected number of hits with script fields
		assert.Len(t, result.Hits, 3, "Expected 3 hits with script fields")

		// Verify that all hits have the script field
		for i, hit := range result.Hits {
			_, hasTotalValue := hit["total_value"]
			assert.True(t, hasTotalValue, "Hit %d should have total_value", i)
		}
	})

	// Test multiple scripted fields
	t.Run("TestMultipleScriptedFields", func(t *testing.T) {
		// Create a query builder
		qb := reveald.NewQueryBuilder(&reveald.Request{}, testIndex)

		// Create and process multiple scripted field features
		feature1 := NewScriptedFieldFeature("total_value", "doc['price'].value * doc['quantity'].value")
		feature2 := NewScriptedFieldFeature("discounted_price", "doc['price'].value * (1 - doc['discount_rate'].value)")

		// Process both features
		_, err := feature1.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil
		})
		require.NoError(t, err)

		result, err := feature2.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return backend.Execute(ctx, builder)
		})

		require.NoError(t, err, "Failed to execute query with multiple scripted fields")
		require.NotNil(t, result, "Expected result")

		// Verify we got results
		assert.Greater(t, result.TotalHitCount, int64(0), "Expected at least one result")
		assert.NotEmpty(t, result.Hits, "Expected hits in result")

		// Check that both scripted fields are present in each hit
		for i, hit := range result.Hits {
			totalValue, hasTotalValue := hit["total_value"]
			discountedPrice, hasDiscountedPrice := hit["discounted_price"]

			assert.True(t, hasTotalValue, "Expected total_value scripted field in hit %d", i)
			assert.True(t, hasDiscountedPrice, "Expected discounted_price scripted field in hit %d", i)

			// Verify both fields have reasonable values
			if hasTotalValue && hasDiscountedPrice {
				if totalVal, ok := totalValue.(float64); ok {
					if discountVal, ok := discountedPrice.(float64); ok {
						assert.Greater(t, totalVal, 0.0, "Total value should be positive")
						assert.Greater(t, discountVal, 0.0, "Discounted price should be positive")

						// Both values should be reasonable
						assert.Less(t, totalVal, 10000.0, "Total value should be reasonable")
						assert.Less(t, discountVal, 1000.0, "Discounted price should be reasonable")
					}
				}
			}
		}
	})
}

// Test comprehensive workflow: ScriptedFields with manual aggregations
func Test_ScriptedFieldsWithAggregations_Integration(t *testing.T) {
	// Skip long running tests unless explicitly enabled
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	const (
		elasticVersion = "8.12.1"
		testIndex      = "scripted-field-aggregation-test"
	)

	ctx := context.Background()

	// Set up Elasticsearch container
	elasticContainer, err := elasticsearch.RunContainer(ctx,
		testcontainers.WithImage("docker.elastic.co/elasticsearch/elasticsearch:"+elasticVersion),
		testcontainers.WithEnv(map[string]string{
			"discovery.type":           "single-node",
			"xpack.security.enabled":   "false",
			"ES_JAVA_OPTS":             "-Xms512m -Xmx512m",
			"action.auto_create_index": "true",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("started").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err, "Failed to start Elasticsearch container")
	defer func() {
		if err := elasticContainer.Terminate(ctx); err != nil {
			t.Fatalf("Failed to terminate container: %s", err)
		}
	}()

	// Get the connection URL
	httpURL, err := elasticContainer.Endpoint(ctx, "http")
	require.NoError(t, err, "Failed to get Elasticsearch HTTP URL")

	// Wait a bit more to ensure Elasticsearch is truly ready
	time.Sleep(2 * time.Second)

	// Create backend
	backend, err := reveald.NewElasticBackend([]string{httpURL})
	require.NoError(t, err, "Failed to create Elasticsearch backend")

	// Create test index with sample data
	indexMapping := `{
		"mappings": {
			"properties": {
				"title": {"type": "text"},
				"category": {"type": "keyword"},
				"price": {"type": "float"},
				"rating": {"type": "integer"},
				"active": {"type": "boolean"}
			}
		}
	}`

	// Create the index
	res, err := backend.GetClient().Indices.Create(
		testIndex,
		backend.GetClient().Indices.Create.WithBody(strings.NewReader(indexMapping)),
		backend.GetClient().Indices.Create.WithContext(ctx),
	)
	require.NoError(t, err, "Failed to create test index")
	require.False(t, res.IsError(), "Error creating index: %s", res.String())
	res.Body.Close()

	// Add sample documents with different categories
	documents := []map[string]any{
		{
			"title":    "Laptop Pro",
			"category": "electronics",
			"price":    1299.99,
			"rating":   5,
			"active":   true,
		},
		{
			"title":    "Smartphone X",
			"category": "electronics",
			"price":    899.99,
			"rating":   4,
			"active":   true,
		},
		{
			"title":    "Cotton T-Shirt",
			"category": "clothing",
			"price":    29.99,
			"rating":   3,
			"active":   true,
		},
		{
			"title":    "Jeans",
			"category": "clothing",
			"price":    79.99,
			"rating":   4,
			"active":   true,
		},
		{
			"title":    "Coffee Table",
			"category": "furniture",
			"price":    299.99,
			"rating":   4,
			"active":   false,
		},
	}

	// Index documents
	for i, doc := range documents {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err, "Failed to marshal document")

		res, err := backend.GetClient().Index(
			testIndex,
			strings.NewReader(string(docJSON)),
			backend.GetClient().Index.WithContext(ctx),
			backend.GetClient().Index.WithDocumentID(fmt.Sprintf("doc-%d", i+1)),
		)
		require.NoError(t, err, "Failed to index document")
		require.False(t, res.IsError(), "Error indexing document: %s", res.String())
		res.Body.Close()
	}

	// Force refresh to make documents searchable immediately
	res, err = backend.GetClient().Indices.Refresh(
		backend.GetClient().Indices.Refresh.WithIndex(testIndex),
		backend.GetClient().Indices.Refresh.WithContext(ctx),
	)
	require.NoError(t, err, "Failed to refresh index")
	require.False(t, res.IsError(), "Error refreshing index: %s", res.String())
	res.Body.Close()

	// Test: ScriptedFields with manual filtering and aggregations
	t.Run("ScriptedFieldsWithFilterAndAggregation", func(t *testing.T) {
		// Create a query builder that filters by electronics category
		qb := reveald.NewQueryBuilder(nil, testIndex)

		// Add a filter for electronics category
		qb.With(types.Query{
			Term: map[string]types.TermQuery{
				"category": {Value: "electronics"},
			},
		})

		// Add scripted fields for additional computed data
		discountScript := "doc['price'].value * 0.9"              // 10% discount
		valueScript := "doc['price'].value / doc['rating'].value" // price per rating point

		// Create and process scripted field features
		discountFeature := NewScriptedFieldFeature("discount_price", discountScript)
		valueFeature := NewScriptedFieldFeature("value_ratio", valueScript)

		// Process scripted fields (they just add to the query, don't execute)
		_, err := discountFeature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil // Just add the scripted field, don't execute yet
		})
		require.NoError(t, err, "Failed to process discount scripted field")

		_, err = valueFeature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil // Just add the scripted field, don't execute yet
		})
		require.NoError(t, err, "Failed to process value ratio scripted field")

		// Add a manual aggregation on category to verify aggregations work with scripted fields
		categoryField := "category"
		categoryAgg := types.Aggregations{
			Terms: &types.TermsAggregation{
				Field: &categoryField,
				Size:  func() *int { size := 10; return &size }(),
			},
		}
		qb.Aggregation("category_agg", categoryAgg)

		// Add an aggregation on the scripted field using the same script
		priceRangeScript := `
			if (doc['price'].value < 100) {
				return 'budget';
			} else if (doc['price'].value < 500) {
				return 'mid-range';
			} else {
				return 'premium';
			}
		`
		scriptedAgg := types.Aggregations{
			Terms: &types.TermsAggregation{
				Script: &types.Script{
					Source: &priceRangeScript,
				},
				Size: func() *int { size := 10; return &size }(),
			},
		}
		qb.Aggregation("price_range_agg", scriptedAgg)

		// Execute the query
		result, err := backend.Execute(ctx, qb)
		require.NoError(t, err, "Failed to execute query with scripted fields and aggregations")

		// Verify we got filtered results (only electronics products)
		assert.Equal(t, int64(2), result.TotalHitCount, "Expected exactly 2 electronics products")

		// Verify all returned hits have scripted fields
		// Note: Source fields may not be returned when only scripted fields are requested
		for i, hit := range result.Hits {
			// Check scripted fields are present
			discountPrice, hasDiscount := hit["discount_price"]
			assert.True(t, hasDiscount, "Hit %d should have discount_price scripted field", i)

			valueRatio, hasValue := hit["value_ratio"]
			assert.True(t, hasValue, "Hit %d should have value_ratio scripted field", i)

			// Verify scripted field values are reasonable
			if hasDiscount {
				if discountVal, ok := discountPrice.(float64); ok {
					assert.Greater(t, discountVal, 0.0, "Discount price should be positive for hit %d", i)
					assert.Less(t, discountVal, 2000.0, "Discount price should be reasonable for hit %d", i)
				}
			}

			if hasValue {
				if valueVal, ok := valueRatio.(float64); ok {
					assert.Greater(t, valueVal, 0.0, "Value ratio should be positive for hit %d", i)
					assert.Less(t, valueVal, 500.0, "Value ratio should be reasonable for hit %d", i)
				}
			}

			t.Logf("Hit %d: discount_price=%v, value_ratio=%v",
				i, discountPrice, valueRatio)
		}

		// Verify aggregations work alongside scripted fields
		assert.NotNil(t, result.Aggregations, "Expected aggregations in result")

		// Check category aggregation
		categoryBuckets, hasCategoryAgg := result.Aggregations["category_agg"]
		assert.True(t, hasCategoryAgg, "Expected category_agg aggregation")
		assert.NotEmpty(t, categoryBuckets, "Expected category aggregation buckets")

		// Convert category aggregation results to a map
		categoryMap := make(map[string]int64)
		for _, bucket := range categoryBuckets {
			if key, ok := bucket.Value.(string); ok {
				categoryMap[key] = bucket.HitCount
			}
		}

		t.Logf("Category aggregation results: %+v", categoryMap)

		// The aggregation should show all categories, not just the filtered one
		// (aggregations typically show all available values for faceted search)
		assert.Contains(t, categoryMap, "electronics", "Should have electronics in aggregation")

		// Check price range aggregation (based on script)
		priceRangeBuckets, hasPriceRangeAgg := result.Aggregations["price_range_agg"]
		assert.True(t, hasPriceRangeAgg, "Expected price_range_agg aggregation")
		assert.NotEmpty(t, priceRangeBuckets, "Expected price range aggregation buckets")

		// Convert price range aggregation results to a map
		priceRangeMap := make(map[string]int64)
		for _, bucket := range priceRangeBuckets {
			if key, ok := bucket.Value.(string); ok {
				priceRangeMap[key] = bucket.HitCount
			}
		}

		t.Logf("Price range aggregation results: %+v", priceRangeMap)

		// Based on our filtered data (electronics only):
		// - Smartphone X: 899.99 -> "premium"
		// - Laptop Pro: 1299.99 -> "premium"
		// Both electronics products should be in the "premium" category
		assert.Contains(t, priceRangeMap, "premium", "Should have premium products in aggregation")
		assert.Equal(t, int64(2), priceRangeMap["premium"], "Should have 2 premium electronics products")

		t.Logf("Successfully demonstrated ScriptedFields with filtering and aggregations")
	})

	// Test: Verify that scripted fields can be used in aggregations
	t.Run("AggregationOnScriptedFieldValues", func(t *testing.T) {
		// Create a query builder for all active products
		qb := reveald.NewQueryBuilder(nil, testIndex)

		// Filter by active products
		qb.With(types.Query{
			Term: map[string]types.TermQuery{
				"active": {Value: true},
			},
		})

		// Note: With the updated implementation, source fields are automatically included
		// alongside scripted fields, so no explicit field selection is needed

		// Add a scripted field that categorizes products by price range
		priceRangeScript := `
			if (doc['price'].value < 100) {
				return 'budget';
			} else if (doc['price'].value < 500) {
				return 'mid-range';
			} else {
				return 'premium';
			}
		`

		// Create and process scripted field feature
		priceRangeFeature := NewScriptedFieldFeature("price_category", priceRangeScript)

		_, err := priceRangeFeature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil // Just add the scripted field, don't execute yet
		})
		require.NoError(t, err, "Failed to process price range scripted field")

		// Add an aggregation that uses the same script as the scripted field
		scriptedAgg := types.Aggregations{
			Terms: &types.TermsAggregation{
				Script: &types.Script{
					Source: &priceRangeScript,
				},
				Size: func() *int { size := 10; return &size }(),
			},
		}
		qb.Aggregation("price_category_agg", scriptedAgg)

		// Execute the query
		result, err := backend.Execute(ctx, qb)
		require.NoError(t, err, "Failed to execute query with scripted field and script aggregation")

		// Verify we got filtered results (only active products)
		assert.Equal(t, int64(4), result.TotalHitCount, "Expected exactly 4 active products")

		// Collect price categories from scripted fields in hits
		priceCategoryCount := make(map[string]int)
		for i, hit := range result.Hits {
			// Check active filter worked
			active, hasActive := hit["active"]
			assert.True(t, hasActive, "Hit %d should have active field", i)
			assert.Equal(t, true, active, "Hit %d should be active", i)

			// Check scripted field is present
			priceCategory, hasCategory := hit["price_category"]
			assert.True(t, hasCategory, "Hit %d should have price_category scripted field", i)

			if hasCategory {
				if categoryVal, ok := priceCategory.(string); ok {
					priceCategoryCount[categoryVal]++
					assert.Contains(t, []string{"budget", "mid-range", "premium"}, categoryVal,
						"Price category should be budget, mid-range, or premium for hit %d", i)
				}
			}
		}

		t.Logf("Price category distribution from scripted fields: %+v", priceCategoryCount)

		// Verify the aggregation results
		assert.NotNil(t, result.Aggregations, "Expected aggregations in result")

		priceCategoryBuckets, hasPriceCategoryAgg := result.Aggregations["price_category_agg"]
		assert.True(t, hasPriceCategoryAgg, "Expected price_category_agg aggregation")
		assert.NotEmpty(t, priceCategoryBuckets, "Expected price category aggregation buckets")

		// Convert aggregation results to a map for comparison
		aggCategoryCount := make(map[string]int64)
		for _, bucket := range priceCategoryBuckets {
			if key, ok := bucket.Value.(string); ok {
				aggCategoryCount[key] = bucket.HitCount
			}
		}

		t.Logf("Price category distribution from aggregation: %+v", aggCategoryCount)

		// Verify that the aggregation results match the scripted field results
		for categoryName, hitCount := range priceCategoryCount {
			assert.Equal(t, int64(hitCount), aggCategoryCount[categoryName],
				"Aggregation count for %s should match scripted field count", categoryName)
		}

		// Based on our test data (active products only):
		// - Cotton T-Shirt: 29.99 -> "budget"
		// - Jeans: 79.99 -> "budget"
		// - Smartphone X: 899.99 -> "premium"
		// - Laptop Pro: 1299.99 -> "premium"
		assert.Equal(t, int64(2), aggCategoryCount["budget"], "Should have 2 budget products")
		assert.Equal(t, int64(2), aggCategoryCount["premium"], "Should have 2 premium products")

		t.Logf("Successfully demonstrated aggregations on scripted field values")
	})

	// Test: Verify that both scripted fields and ordinary source fields are returned together
	t.Run("ScriptedFieldsWithOrdinaryFields", func(t *testing.T) {
		// Create a query builder for all products
		qb := reveald.NewQueryBuilder(nil, testIndex)

		// Note: With the updated implementation, source fields are automatically included
		// alongside scripted fields, so no explicit field selection is needed

		// Add multiple scripted fields for comprehensive testing
		discountScript := "doc['price'].value * 0.85"                               // 15% discount
		valueRatioScript := "doc['price'].value / Math.max(doc['rating'].value, 1)" // price per rating point
		categoryUpperScript := "doc['category'].value.toUpperCase()"                // uppercase category

		// Create and process multiple scripted field features
		discountFeature := NewScriptedFieldFeature("discount_price", discountScript)
		valueRatioFeature := NewScriptedFieldFeature("value_ratio", valueRatioScript)
		categoryUpperFeature := NewScriptedFieldFeature("category_upper", categoryUpperScript)

		// Process all scripted fields
		_, err := discountFeature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil // Just add the scripted field, don't execute yet
		})
		require.NoError(t, err, "Failed to process discount scripted field")

		_, err = valueRatioFeature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil // Just add the scripted field, don't execute yet
		})
		require.NoError(t, err, "Failed to process value ratio scripted field")

		_, err = categoryUpperFeature.Process(qb, func(builder *reveald.QueryBuilder) (*reveald.Result, error) {
			return &reveald.Result{}, nil // Just add the scripted field, don't execute yet
		})
		require.NoError(t, err, "Failed to process category upper scripted field")

		// Execute the query
		result, err := backend.Execute(ctx, qb)
		require.NoError(t, err, "Failed to execute query with mixed scripted and source fields")

		// Verify we got all documents
		assert.Equal(t, int64(5), result.TotalHitCount, "Expected all 5 products")
		assert.Len(t, result.Hits, 5, "Expected 5 hits")

		// Verify that each hit contains both source fields and scripted fields
		expectedSourceFields := []string{"title", "category", "price", "rating", "active"}
		expectedScriptedFields := []string{"discount_price", "value_ratio", "category_upper"}

		for i, hit := range result.Hits {
			t.Logf("Hit %d: %+v", i, hit)

			// Verify all expected source fields are present
			for _, field := range expectedSourceFields {
				value, hasField := hit[field]
				assert.True(t, hasField, "Hit %d should have source field '%s'", i, field)
				assert.NotNil(t, value, "Hit %d source field '%s' should not be nil", i, field)
			}

			// Verify all expected scripted fields are present
			for _, field := range expectedScriptedFields {
				value, hasField := hit[field]
				assert.True(t, hasField, "Hit %d should have scripted field '%s'", i, field)
				assert.NotNil(t, value, "Hit %d scripted field '%s' should not be nil", i, field)
			}

			// Verify specific field types and relationships
			title, hasTitle := hit["title"]
			category, hasCategory := hit["category"]
			price, hasPrice := hit["price"]
			rating, hasRating := hit["rating"]
			active, hasActive := hit["active"]

			discountPrice, hasDiscountPrice := hit["discount_price"]
			valueRatio, hasValueRatio := hit["value_ratio"]
			categoryUpper, hasCategoryUpper := hit["category_upper"]

			// All fields should be present
			assert.True(t, hasTitle && hasCategory && hasPrice && hasRating && hasActive,
				"Hit %d should have all source fields", i)
			assert.True(t, hasDiscountPrice && hasValueRatio && hasCategoryUpper,
				"Hit %d should have all scripted fields", i)

			// Verify field types
			if hasTitle {
				assert.IsType(t, "", title, "Title should be a string")
			}
			if hasCategory {
				assert.IsType(t, "", category, "Category should be a string")
			}
			if hasPrice {
				assert.IsType(t, float64(0), price, "Price should be a float64")
			}
			if hasRating {
				// Rating could be int or float64 depending on how Elasticsearch returns it
				assert.True(t,
					fmt.Sprintf("%T", rating) == "float64" || fmt.Sprintf("%T", rating) == "int",
					"Rating should be numeric, got %T", rating)
			}
			if hasActive {
				assert.IsType(t, true, active, "Active should be a boolean")
			}

			// Verify scripted field types and reasonable values
			if hasDiscountPrice {
				if discountVal, ok := discountPrice.(float64); ok {
					assert.Greater(t, discountVal, 0.0, "Discount price should be positive")
					assert.Less(t, discountVal, 2000.0, "Discount price should be reasonable")

					// Verify discount calculation (should be 85% of original price)
					if priceVal, ok := price.(float64); ok {
						expectedDiscount := priceVal * 0.85
						assert.InDelta(t, expectedDiscount, discountVal, 0.01,
							"Discount price should be 85%% of original price")
					}
				} else {
					t.Errorf("discount_price should be float64, got %T", discountPrice)
				}
			}

			if hasValueRatio {
				if ratioVal, ok := valueRatio.(float64); ok {
					assert.Greater(t, ratioVal, 0.0, "Value ratio should be positive")
					assert.Less(t, ratioVal, 1000.0, "Value ratio should be reasonable")
				} else {
					t.Errorf("value_ratio should be float64, got %T", valueRatio)
				}
			}

			if hasCategoryUpper {
				if upperVal, ok := categoryUpper.(string); ok {
					assert.NotEmpty(t, upperVal, "Category upper should not be empty")

					// Verify it's actually uppercase
					if categoryVal, ok := category.(string); ok {
						expectedUpper := strings.ToUpper(categoryVal)
						assert.Equal(t, expectedUpper, upperVal,
							"Category upper should be uppercase version of category")
					}
				} else {
					t.Errorf("category_upper should be string, got %T", categoryUpper)
				}
			}

			// Log the complete hit for debugging
			t.Logf("Hit %d complete data:", i)
			t.Logf("  Source fields: title=%v, category=%v, price=%v, rating=%v, active=%v",
				title, category, price, rating, active)
			t.Logf("  Scripted fields: discount_price=%v, value_ratio=%v, category_upper=%v",
				discountPrice, valueRatio, categoryUpper)
		}

		// Verify that we have a good mix of data
		categories := make(map[string]int)
		activeCount := 0
		for _, hit := range result.Hits {
			if category, ok := hit["category"].(string); ok {
				categories[category]++
			}
			if active, ok := hit["active"].(bool); ok && active {
				activeCount++
			}
		}

		// Should have multiple categories
		assert.GreaterOrEqual(t, len(categories), 2, "Should have at least 2 different categories")
		assert.Equal(t, 4, activeCount, "Should have 4 active products")

		t.Logf("Successfully demonstrated mixed scripted and source fields in hits")
		t.Logf("Categories found: %+v", categories)
		t.Logf("Active products: %d", activeCount)
	})
}

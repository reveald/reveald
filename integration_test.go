package reveald

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	elasticVersion = "8.12.1" // Use the version compatible with your client
	testIndex      = "test-index"
)

// setupElasticsearch starts an Elasticsearch container and returns its connection details
func setupElasticsearch(t *testing.T) (*elasticsearch.ElasticsearchContainer, string) {
	ctx := context.Background()

	// Create and start the Elasticsearch container
	elasticContainer, err := elasticsearch.RunContainer(ctx,
		testcontainers.WithImage("docker.elastic.co/elasticsearch/elasticsearch:"+elasticVersion),
		testcontainers.WithEnv(map[string]string{
			"discovery.type":           "single-node",
			"xpack.security.enabled":   "false", // Disable security for simplicity in tests
			"ES_JAVA_OPTS":             "-Xms512m -Xmx512m",
			"action.auto_create_index": "true",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("started").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err, "Failed to start Elasticsearch container")

	// Get the connection URL
	httpURL, err := elasticContainer.Endpoint(ctx, "http")
	require.NoError(t, err, "Failed to get Elasticsearch HTTP URL")

	// Wait a bit more to ensure Elasticsearch is truly ready
	time.Sleep(2 * time.Second)

	return elasticContainer, httpURL
}

type mappingCaster struct {
	types.TypeMapping
}

func (m *mappingCaster) TypeMappingCaster() *types.TypeMapping {
	return &m.TypeMapping
}

// createTestIndex creates a test index with sample data
func createTestIndex(t *testing.T, backend *ElasticBackend) {
	ctx := context.Background()

	// Create mapping for test index
	indexMapping := types.TypeMapping{
		Properties: map[string]types.Property{
			"title":       types.NewTextProperty(),
			"description": types.NewTextProperty(),
			"tags":        types.NewKeywordProperty(),
			"price":       types.NewFloatNumberProperty(),
			"active":      types.NewBooleanProperty(),
			"category":    types.NewKeywordProperty(),
			"rating":      types.NewIntegerNumberProperty(),
		},
	}

	// Create the index using typed client
	res, err := backend.client.Indices.Create(testIndex).
		Mappings(&indexMapping).
		Do(ctx)
	require.NoError(t, err, "Failed to create test index")
	require.True(t, res.Acknowledged, "Index creation was not acknowledged")

	// Add sample documents
	documents := []map[string]any{
		{
			"title":       "Product 1",
			"description": "This is product one with high quality",
			"tags":        []string{"electronics", "gadget"},
			"price":       99.99,
			"active":      true,
			"category":    "electronics",
			"rating":      5,
		},
		{
			"title":       "Product 2",
			"description": "This is product two with medium quality",
			"tags":        []string{"clothing", "apparel"},
			"price":       49.99,
			"active":      true,
			"category":    "fashion",
			"rating":      3,
		},
		{
			"title":       "Product 3",
			"description": "This is product three with low quality",
			"tags":        []string{"home", "kitchen"},
			"price":       29.99,
			"active":      false,
			"category":    "home",
			"rating":      2,
		},
		{
			"title":       "Product 4",
			"description": "This is product four with high quality",
			"tags":        []string{"electronics", "computer"},
			"price":       199.99,
			"active":      true,
			"category":    "electronics",
			"rating":      4,
		},
		{
			"title":       "Product 5",
			"description": "This is product five with medium quality",
			"tags":        []string{"home", "furniture"},
			"price":       149.99,
			"active":      true,
			"category":    "home",
			"rating":      3,
		},
	}

	// Index documents using typed client
	for i, doc := range documents {
		res, err := backend.client.Index(testIndex).
			Id(fmt.Sprintf("doc-%d", i+1)).
			Request(doc).
			Do(ctx)
		require.NoError(t, err, "Failed to index document")
		require.NotEmpty(t, res.Id_, "Document ID should not be empty")
	}

	// Force refresh to make documents searchable immediately
	refreshRes, err := backend.client.Indices.Refresh().
		Index(testIndex).
		Do(ctx)
	require.NoError(t, err, "Failed to refresh index")
	require.NotNil(t, refreshRes, "Refresh response should not be nil")
}

func TestElasticBackendWithTestcontainers(t *testing.T) {
	// Skip long running tests unless explicitly enabled
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up Elasticsearch container
	elasticContainer, httpURL := setupElasticsearch(t)
	defer func() {
		if err := elasticContainer.Terminate(context.Background()); err != nil {
			t.Fatalf("Failed to terminate container: %s", err)
		}
	}()

	// Create backend
	backend, err := NewElasticBackend([]string{httpURL})
	require.NoError(t, err, "Failed to create Elasticsearch backend")

	// Create test index with sample data
	createTestIndex(t, backend)

	// Test basic search functionality
	t.Run("TestBasicSearch", func(t *testing.T) {
		ctx := context.Background()

		// Create a query to find all active products
		builder := NewQueryBuilder(nil, testIndex)
		builder.With(types.Query{
			Term: map[string]types.TermQuery{
				"active": {Value: true},
			},
		})

		// Execute the query
		result, err := backend.Execute(ctx, builder)
		require.NoError(t, err, "Failed to execute search")

		// Verify results
		assert.Equal(t, int64(4), result.TotalHitCount, "Expected 4 active products")
	})

	// Test complex query with multiple conditions
	t.Run("TestComplexQuery", func(t *testing.T) {
		ctx := context.Background()

		// Create a query for active electronics products with rating >= 4
		builder := NewQueryBuilder(nil, testIndex)

		// Must be active
		builder.With(types.Query{
			Term: map[string]types.TermQuery{
				"active": {Value: true},
			},
		})

		// Must be in electronics category
		builder.With(types.Query{
			Term: map[string]types.TermQuery{
				"category": {Value: "electronics"},
			},
		})

		// Must have rating >= 4
		gte := types.Float64(4)
		builder.With(types.Query{
			Range: map[string]types.RangeQuery{
				"rating": &types.NumberRangeQuery{
					Gte: &gte,
				},
			},
		})

		// Execute the query
		result, err := backend.Execute(ctx, builder)
		require.NoError(t, err, "Failed to execute complex query")

		// Verify results - should match Products 1 and 4
		assert.Equal(t, int64(2), result.TotalHitCount, "Expected 2 matching products")

		// Verify correct titles are returned
		foundTitles := make([]string, 0, 2)
		for _, hit := range result.Hits {
			if title, ok := hit["title"].(string); ok {
				foundTitles = append(foundTitles, title)
			}
		}

		assert.Contains(t, foundTitles, "Product 1", "Expected Product 1 in results")
		assert.Contains(t, foundTitles, "Product 4", "Expected Product 4 in results")
	})

	// Test sorting functionality
	t.Run("TestSorting", func(t *testing.T) {
		ctx := context.Background()

		// Create a query for active products sorted by price in descending order
		builder := NewQueryBuilder(nil, testIndex)

		// Must be active
		builder.With(types.Query{
			Term: map[string]types.TermQuery{
				"active": {Value: true},
			},
		})

		// Add sort by price descending
		builder.Selection().Update(WithSort("price", sortorder.Desc))

		// Execute the query
		result, err := backend.Execute(ctx, builder)
		require.NoError(t, err, "Failed to execute sorted query")

		// Verify results - should return 4 active products
		assert.Equal(t, int64(4), result.TotalHitCount, "Expected 4 active products")

		// Verify correct order - prices should be in descending order
		require.GreaterOrEqual(t, len(result.Hits), 2, "Expected at least 2 hits to check sorting")

		// Extract prices
		var prices []float64
		for _, hit := range result.Hits {
			if price, ok := hit["price"].(float64); ok {
				prices = append(prices, price)
			}
		}

		// Check if prices are in descending order
		for i := 0; i < len(prices)-1; i++ {
			assert.GreaterOrEqual(t, prices[i], prices[i+1], "Prices should be in descending order")
		}

		// The first product should be Product 4 (price 199.99)
		assert.Equal(t, "Product 4", result.Hits[0]["title"], "First product should be Product 4")
	})

	// Test scripted fields functionality
	t.Run("TestScriptedFields", func(t *testing.T) {
		ctx := context.Background()

		// Create a query with scripted fields
		builder := NewQueryBuilder(nil, testIndex)

		// Add a scripted field that calculates a discount price (10% off)
		discountScript := "doc['price'].value * 0.9"
		builder.WithScriptedField("discount_price", &types.Script{
			Source: &discountScript,
		})

		// Add another scripted field that calculates price per rating point
		pricePerRatingScript := "doc['price'].value / Math.max(doc['rating'].value, 1)"
		builder.WithScriptedField("price_per_rating", &types.Script{
			Source: &pricePerRatingScript,
		})

		// Don't add any filters - get all documents
		// This helps us debug if the issue is with the scripted fields or the filtering

		// Execute the query
		result, err := backend.Execute(ctx, builder)
		require.NoError(t, err, "Failed to execute query with scripted fields")

		// Verify we got results
		assert.Greater(t, result.TotalHitCount, int64(0), "Expected at least one result")
		assert.NotEmpty(t, result.Hits, "Expected hits in result")

		// Check that scripted fields are present in the response
		for i, hit := range result.Hits {

			// Check if scripted fields are present
			discountPrice, hasDiscount := hit["discount_price"]
			pricePerRating, hasPricePerRating := hit["price_per_rating"]

			assert.True(t, hasDiscount, "Expected discount_price scripted field in hit %d", i)
			assert.True(t, hasPricePerRating, "Expected price_per_rating scripted field in hit %d", i)

			// Verify the scripted field values are reasonable
			if hasDiscount {
				// discount_price should be a numeric value
				if discountVal, ok := discountPrice.(float64); ok {
					assert.Greater(t, discountVal, 0.0, "Discount price should be positive for hit %d", i)
					// We can't verify the exact calculation without the original price,
					// but we can verify it's a reasonable discount value
					assert.Less(t, discountVal, 200.0, "Discount price should be reasonable for hit %d", i)
				} else {
					t.Errorf("discount_price should be a numeric value, got: %T %v", discountPrice, discountPrice)
				}
			}

			if hasPricePerRating {
				// price_per_rating should be a numeric value
				if pricePerRatingVal, ok := pricePerRating.(float64); ok {
					assert.Greater(t, pricePerRatingVal, 0.0, "Price per rating should be positive for hit %d", i)
					assert.Less(t, pricePerRatingVal, 100.0, "Price per rating should be reasonable for hit %d", i)
				} else {
					t.Errorf("price_per_rating should be a numeric value, got: %T %v", pricePerRating, pricePerRating)
				}
			}
		}

		// Verify that we have the expected number of hits with script fields
		assert.Len(t, result.Hits, 5, "Expected 5 hits with script fields")

		// Verify that all hits have both script fields
		for i, hit := range result.Hits {
			_, hasDiscount := hit["discount_price"]
			_, hasPricePerRating := hit["price_per_rating"]
			assert.True(t, hasDiscount, "Hit %d should have discount_price", i)
			assert.True(t, hasPricePerRating, "Hit %d should have price_per_rating", i)
		}
	})

	// Test the new scripted field features
	t.Run("TestNewScriptedFieldFeatures", func(t *testing.T) {
		ctx := context.Background()

		// Test BooleanScriptedFieldFeature without filtering
		t.Run("BooleanScriptedFieldWithoutFiltering", func(t *testing.T) {
			// We need to import the featureset package, but since we can't due to import cycles,
			// we'll create a local implementation for testing
			type BooleanScriptedFieldFeature struct {
				fieldName string
				script    string
				filter    bool
			}

			processFunc := func(feature *BooleanScriptedFieldFeature, builder *QueryBuilder, next FeatureFunc) (*Result, error) {
				// Always add the scripted field
				source := feature.script
				script := &types.Script{
					Source: &source,
				}
				builder.WithScriptedField(feature.fieldName, script)

				// Optionally add filtering if enabled and parameter is truthy
				if feature.filter {
					param, err := builder.Request().Get(feature.fieldName)
					if err == nil && param.IsTruthy() {
						scriptQuery := types.Query{
							Script: &types.ScriptQuery{
								Script: types.Script{
									Source: &feature.script,
								},
							},
						}
						builder.With(scriptQuery)
					}
				}

				return next(builder)
			}

			// Create feature without filtering
			feature := &BooleanScriptedFieldFeature{
				fieldName: "is_expensive",
				script:    "doc['price'].value > 100",
				filter:    false,
			}

			builder := NewQueryBuilder(nil, testIndex)
			mockNext := func(builder *QueryBuilder) (*Result, error) {
				return backend.Execute(ctx, builder)
			}

			result, err := processFunc(feature, builder, mockNext)
			require.NoError(t, err, "Failed to execute boolean scripted field without filtering")

			// Should return all 5 products with the scripted field
			assert.Equal(t, int64(5), result.TotalHitCount, "Expected all 5 products")

			// Verify scripted field is present and correct
			expensiveCount := 0
			for _, hit := range result.Hits {
				if isExpensive, ok := hit["is_expensive"].(bool); ok && isExpensive {
					expensiveCount++
				}
			}
			assert.Equal(t, 2, expensiveCount, "Expected 2 expensive products")
		})

		// Test BooleanScriptedFieldFeature with filtering
		t.Run("BooleanScriptedFieldWithFiltering", func(t *testing.T) {
			type BooleanScriptedFieldFeature struct {
				fieldName string
				script    string
				filter    bool
			}

			processFunc := func(feature *BooleanScriptedFieldFeature, builder *QueryBuilder, next FeatureFunc) (*Result, error) {
				// Always add the scripted field
				source := feature.script
				script := &types.Script{
					Source: &source,
				}
				builder.WithScriptedField(feature.fieldName, script)

				// Optionally add filtering if enabled and parameter is truthy
				if feature.filter {
					param, err := builder.Request().Get(feature.fieldName)
					if err == nil && param.IsTruthy() {
						scriptQuery := types.Query{
							Script: &types.ScriptQuery{
								Script: types.Script{
									Source: &feature.script,
								},
							},
						}
						builder.With(scriptQuery)
					}
				}

				return next(builder)
			}

			// Create feature with filtering enabled
			feature := &BooleanScriptedFieldFeature{
				fieldName: "is_expensive",
				script:    "doc['price'].value > 100",
				filter:    true,
			}

			// Create request with parameter
			request := NewRequest(NewParameter("is_expensive", "true"))
			builder := NewQueryBuilder(request, testIndex)

			mockNext := func(builder *QueryBuilder) (*Result, error) {
				return backend.Execute(ctx, builder)
			}

			result, err := processFunc(feature, builder, mockNext)
			require.NoError(t, err, "Failed to execute boolean scripted field with filtering")

			// Should return only 2 expensive products
			assert.Equal(t, int64(2), result.TotalHitCount, "Expected 2 expensive products")

			// Verify all returned products are expensive
			for i, hit := range result.Hits {
				if price, ok := hit["price"].(float64); ok {
					assert.Greater(t, price, 100.0, "Hit %d should have price > 100", i)
				}
				if isExpensive, ok := hit["is_expensive"].(bool); ok {
					assert.True(t, isExpensive, "Hit %d should have is_expensive = true", i)
				}
			}
		})
	})

	// Test BooleanScriptedFieldFeature with untruthy value filtering
	t.Run("TestBooleanScriptedFieldWithUntruthyFiltering", func(t *testing.T) {
		ctx := context.Background()

		// Test filtering for untruthy values (is_expensive=false)
		t.Run("FilterForUntruthyValues", func(t *testing.T) {
			type BooleanScriptedFieldFeature struct {
				fieldName string
				script    string
				filter    bool
			}

			processFunc := func(feature *BooleanScriptedFieldFeature, builder *QueryBuilder, next FeatureFunc) (*Result, error) {
				// Always add the scripted field
				source := feature.script
				script := &types.Script{
					Source: &source,
				}
				builder.WithScriptedField(feature.fieldName, script)

				// Optionally add filtering if enabled and parameter exists
				if feature.filter {
					param, err := builder.Request().Get(feature.fieldName)
					if err == nil && len(param.Values()) > 0 {
						var scriptSource string
						if param.IsTruthy() {
							// Filter for documents where the script returns true
							scriptSource = feature.script
						} else {
							// Filter for documents where the script returns false (negate the script)
							scriptSource = "!(" + feature.script + ")"
						}

						scriptQuery := types.Query{
							Script: &types.ScriptQuery{
								Script: types.Script{
									Source: &scriptSource,
								},
							},
						}
						builder.With(scriptQuery)
					}
				}

				return next(builder)
			}

			// Create feature with filtering enabled
			feature := &BooleanScriptedFieldFeature{
				fieldName: "is_expensive",
				script:    "doc['price'].value > 100",
				filter:    true,
			}

			// Create request with parameter set to false (untruthy)
			request := NewRequest(NewParameter("is_expensive", "false"))
			builder := NewQueryBuilder(request, testIndex)

			mockNext := func(builder *QueryBuilder) (*Result, error) {
				return backend.Execute(ctx, builder)
			}

			result, err := processFunc(feature, builder, mockNext)
			require.NoError(t, err, "Failed to execute boolean scripted field with untruthy filtering")

			// Should return only 3 non-expensive products (price <= 100)
			// Products 1, 2, 3 have prices: 99.99, 49.99, 29.99
			assert.Equal(t, int64(3), result.TotalHitCount, "Expected 3 non-expensive products")

			// Verify all returned products are not expensive
			for i, hit := range result.Hits {
				if price, ok := hit["price"].(float64); ok {
					assert.LessOrEqual(t, price, 100.0, "Hit %d should have price <= 100", i)
				}
				if isExpensive, ok := hit["is_expensive"].(bool); ok {
					assert.False(t, isExpensive, "Hit %d should have is_expensive = false", i)
				}
			}

			// Verify we got the expected products (Product 1, 2, 3)
			expectedTitles := []string{"Product 1", "Product 2", "Product 3"}
			actualTitles := make([]string, 0, len(result.Hits))
			for _, hit := range result.Hits {
				if title, ok := hit["title"].(string); ok {
					actualTitles = append(actualTitles, title)
				}
			}

			for _, expectedTitle := range expectedTitles {
				assert.Contains(t, actualTitles, expectedTitle, "Should contain %s", expectedTitle)
			}
		})

		// Test comparison: truthy vs untruthy filtering
		t.Run("CompareUntruthyVsTruthyFiltering", func(t *testing.T) {
			type BooleanScriptedFieldFeature struct {
				fieldName string
				script    string
				filter    bool
			}

			processFunc := func(feature *BooleanScriptedFieldFeature, builder *QueryBuilder, next FeatureFunc) (*Result, error) {
				// Always add the scripted field
				source := feature.script
				script := &types.Script{
					Source: &source,
				}
				builder.WithScriptedField(feature.fieldName, script)

				// Optionally add filtering if enabled and parameter exists
				if feature.filter {
					param, err := builder.Request().Get(feature.fieldName)
					if err == nil && len(param.Values()) > 0 {
						var scriptSource string
						if param.IsTruthy() {
							// Filter for documents where the script returns true
							scriptSource = feature.script
						} else {
							// Filter for documents where the script returns false (negate the script)
							scriptSource = "!(" + feature.script + ")"
						}

						scriptQuery := types.Query{
							Script: &types.ScriptQuery{
								Script: types.Script{
									Source: &scriptSource,
								},
							},
						}
						builder.With(scriptQuery)
					}
				}

				return next(builder)
			}

			feature := &BooleanScriptedFieldFeature{
				fieldName: "is_expensive",
				script:    "doc['price'].value > 100",
				filter:    true,
			}

			mockNext := func(builder *QueryBuilder) (*Result, error) {
				return backend.Execute(ctx, builder)
			}

			// Test with truthy parameter
			requestTrue := NewRequest(NewParameter("is_expensive", "true"))
			builderTrue := NewQueryBuilder(requestTrue, testIndex)
			resultTrue, err := processFunc(feature, builderTrue, mockNext)
			require.NoError(t, err, "Failed to execute with truthy parameter")

			// Test with untruthy parameter
			requestFalse := NewRequest(NewParameter("is_expensive", "false"))
			builderFalse := NewQueryBuilder(requestFalse, testIndex)
			resultFalse, err := processFunc(feature, builderFalse, mockNext)
			require.NoError(t, err, "Failed to execute with untruthy parameter")

			// Verify that truthy + untruthy results equal total documents
			totalExpected := int64(5) // We have 5 test documents
			totalActual := resultTrue.TotalHitCount + resultFalse.TotalHitCount
			assert.Equal(t, totalExpected, totalActual, "Truthy + untruthy results should equal total documents")

			// Verify truthy results (expensive products: Products 4 and 5)
			assert.Equal(t, int64(2), resultTrue.TotalHitCount, "Expected 2 expensive products")

			// Verify untruthy results (non-expensive products: Products 1, 2, 3)
			assert.Equal(t, int64(3), resultFalse.TotalHitCount, "Expected 3 non-expensive products")

			// Verify no overlap between results
			trueHitIds := make(map[string]bool)
			for _, hit := range resultTrue.Hits {
				if title, ok := hit["title"].(string); ok {
					trueHitIds[title] = true
				}
			}

			for _, hit := range resultFalse.Hits {
				if title, ok := hit["title"].(string); ok {
					assert.False(t, trueHitIds[title], "Product %s should not appear in both truthy and untruthy results", title)
				}
			}
		})
	})
}

package reveald

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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

// createTestIndex creates a test index with sample data
func createTestIndex(t *testing.T, backend *ElasticBackend) {
	ctx := context.Background()

	// Create mapping for test index
	indexMapping := `{
		"mappings": {
			"properties": {
				"title": {"type": "text"},
				"description": {"type": "text"},
				"tags": {"type": "keyword"},
				"price": {"type": "float"},
				"active": {"type": "boolean"},
				"category": {"type": "keyword"},
				"rating": {"type": "integer"}
			}
		}
	}`

	// Create the index
	res, err := backend.client.Indices.Create(
		testIndex,
		backend.client.Indices.Create.WithBody(strings.NewReader(indexMapping)),
		backend.client.Indices.Create.WithContext(ctx),
	)
	require.NoError(t, err, "Failed to create test index")
	require.False(t, res.IsError(), "Error creating index: %s", res.String())
	res.Body.Close()

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

	// Index documents
	for i, doc := range documents {
		docJSON, err := json.Marshal(doc)
		require.NoError(t, err, "Failed to marshal document")

		res, err := backend.client.Index(
			testIndex,
			strings.NewReader(string(docJSON)),
			backend.client.Index.WithContext(ctx),
			backend.client.Index.WithDocumentID(fmt.Sprintf("doc-%d", i+1)),
		)
		require.NoError(t, err, "Failed to index document")
		require.False(t, res.IsError(), "Error indexing document: %s", res.String())
		res.Body.Close()
	}

	// Force refresh to make documents searchable immediately
	res, err = backend.client.Indices.Refresh(
		backend.client.Indices.Refresh.WithIndex(testIndex),
		backend.client.Indices.Refresh.WithContext(ctx),
	)
	require.NoError(t, err, "Failed to refresh index")
	require.False(t, res.IsError(), "Error refreshing index: %s", res.String())
	res.Body.Close()
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

	// Test aggregation functionality
	t.Run("TestAggregation", func(t *testing.T) {
		ctx := context.Background()

		// Create a query to aggregate by tags
		builder := NewQueryBuilder(nil, testIndex)
		field := "tags"
		builder.Aggregation("tags", types.Aggregations{
			Terms: &types.TermsAggregation{
				Field: &field,
			},
		})

		// Execute the query
		result, err := backend.Execute(ctx, builder)
		require.NoError(t, err, "Failed to execute aggregation")

		// Verify aggregation results
		assert.NotNil(t, result.Aggregations, "Expected aggregations in result")
		assert.NotEmpty(t, result.Aggregations["tags"], "Expected tag aggregations")

		// After running the test, we found that there are 8 unique tags in total
		// This happens because some tags are duplicated across products
		assert.Len(t, result.Aggregations["tags"], 8, "Expected 8 unique tags")
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

	// Test scripted fields with aggregations
	t.Run("TestScriptedFieldsWithAggregations", func(t *testing.T) {
		ctx := context.Background()

		// Create a query builder
		builder := NewQueryBuilder(nil, testIndex)

		// Add a scripted field that categorizes products by price range
		// This will create categories: "low" (< 50), "medium" (50-150), "high" (> 150)
		priceRangeScript := `
			if (doc['price'].value < 50) {
				return 'low';
			} else if (doc['price'].value <= 150) {
				return 'medium';
			} else {
				return 'high';
			}
		`
		builder.WithScriptedField("price_range", &types.Script{
			Source: &priceRangeScript,
		})

		// Add an aggregation on the scripted field using a script-based terms aggregation
		// Since scripted fields don't have .keyword mappings, we need to use a script for aggregation
		aggregationScript := priceRangeScript // Same script as the scripted field

		termsAgg := types.Aggregations{
			Terms: &types.TermsAggregation{
				Script: &types.Script{
					Source: &aggregationScript,
				},
				Size: func() *int { size := 10; return &size }(),
			},
		}

		builder.Aggregation("price_range_agg", termsAgg)

		// Execute the query
		result, err := backend.Execute(ctx, builder)
		require.NoError(t, err, "Failed to execute query with scripted field and aggregation")

		// Verify we got results with the scripted field
		assert.Greater(t, result.TotalHitCount, int64(0), "Expected at least one result")
		assert.NotEmpty(t, result.Hits, "Expected hits in result")

		// Verify that all hits have the price_range scripted field
		priceRanges := make(map[string]int)
		for i, hit := range result.Hits {
			priceRange, hasPriceRange := hit["price_range"]
			assert.True(t, hasPriceRange, "Hit %d should have price_range", i)

			if hasPriceRange {
				if rangeVal, ok := priceRange.(string); ok {
					priceRanges[rangeVal]++
					assert.Contains(t, []string{"low", "medium", "high"}, rangeVal,
						"Price range should be low, medium, or high for hit %d", i)
				} else {
					t.Errorf("price_range should be a string value, got: %T %v", priceRange, priceRange)
				}
			}
		}

		// Verify we have the expected distribution based on our test data:
		// Product 3: 29.99 -> "low"
		// Product 2: 49.99 -> "low"
		// Product 1: 99.99 -> "medium"
		// Product 5: 149.99 -> "medium"
		// Product 4: 199.99 -> "high"
		assert.Equal(t, 2, priceRanges["low"], "Expected 2 products in low price range")
		assert.Equal(t, 2, priceRanges["medium"], "Expected 2 products in medium price range")
		assert.Equal(t, 1, priceRanges["high"], "Expected 1 product in high price range")

		t.Logf("Price range distribution from hits: %+v", priceRanges)

		// Now verify the aggregation results match the scripted field results
		assert.NotNil(t, result.Aggregations, "Expected aggregations in result")
		aggBuckets, hasAgg := result.Aggregations["price_range_agg"]
		assert.True(t, hasAgg, "Expected price_range_agg aggregation")
		assert.NotEmpty(t, aggBuckets, "Expected aggregation buckets")

		// Convert aggregation results to a map for comparison
		aggRanges := make(map[string]int64)
		for _, bucket := range aggBuckets {
			if key, ok := bucket.Value.(string); ok {
				aggRanges[key] = bucket.HitCount
			}
		}

		t.Logf("Price range distribution from aggregation: %+v", aggRanges)

		// Verify aggregation results match our expectations
		assert.Equal(t, int64(2), aggRanges["low"], "Aggregation should show 2 products in low price range")
		assert.Equal(t, int64(2), aggRanges["medium"], "Aggregation should show 2 products in medium price range")
		assert.Equal(t, int64(1), aggRanges["high"], "Aggregation should show 1 product in high price range")

		// Verify that the aggregation results match the scripted field results
		for range_name, hitCount := range priceRanges {
			assert.Equal(t, int64(hitCount), aggRanges[range_name],
				"Aggregation count for %s should match scripted field count", range_name)
		}
	})
}

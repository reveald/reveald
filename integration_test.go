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
		builder.Sort("price", sortorder.Desc)

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
}

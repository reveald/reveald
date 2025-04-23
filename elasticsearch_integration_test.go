package reveald

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcElasticsearch "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestElasticsearchIntegration demonstrates a complete integration test with Elasticsearch
func TestElasticsearchIntegration(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup Elasticsearch container
	ctx := context.Background()
	elasticsearchContainer, esURL := setupElasticsearchContainer(t, ctx)
	defer terminateContainer(t, ctx, elasticsearchContainer)

	// Create Elasticsearch client
	esClient, err := createElasticsearchClient(esURL)
	require.NoError(t, err, "Failed to create Elasticsearch client")

	// Create test index and add documents
	indexName := "test-products"
	createIndex(t, ctx, esClient, indexName)
	indexTestDocuments(t, ctx, esClient, indexName)

	// Test search functionality
	testSearchDocuments(t, ctx, esClient, indexName)

	// Test our backend implementation
	testBackendImplementation(t, esURL, indexName)
}

// setupElasticsearchContainer starts an Elasticsearch container and returns it along with its URL
func setupElasticsearchContainer(t *testing.T, ctx context.Context) (testcontainers.Container, string) {
	elasticsearchContainer, err := tcElasticsearch.Run(ctx,
		"docker.elastic.co/elasticsearch/elasticsearch:8.9.0",
		testcontainers.WithEnv(map[string]string{
			"discovery.type":         "single-node",
			"ES_JAVA_OPTS":           "-Xms512m -Xmx512m",
			"xpack.security.enabled": "false",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("started").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err, "Failed to start Elasticsearch container")

	// Get the HTTP endpoint
	httpURL, err := elasticsearchContainer.Endpoint(ctx, "http")
	require.NoError(t, err, "Failed to get Elasticsearch HTTP URL")

	// Wait a bit to ensure Elasticsearch is fully initialized
	time.Sleep(2 * time.Second)

	return elasticsearchContainer, httpURL
}

// terminateContainer safely terminates the container
func terminateContainer(t *testing.T, ctx context.Context, container testcontainers.Container) {
	if err := container.Terminate(ctx); err != nil {
		t.Logf("Failed to terminate container: %s", err)
	}
}

// createElasticsearchClient creates an Elasticsearch client
func createElasticsearchClient(esURL string) (*elasticsearch.Client, error) {
	config := elasticsearch.Config{
		Addresses: []string{esURL},
	}
	return elasticsearch.NewClient(config)
}

// createIndex creates a test index with mappings
func createIndex(t *testing.T, ctx context.Context, client *elasticsearch.Client, indexName string) {
	// Define index mapping
	mapping := `{
		"mappings": {
			"properties": {
				"name": { "type": "text" },
				"description": { "type": "text" },
				"price": { "type": "float" },
				"category": { "type": "keyword" },
				"tags": { "type": "keyword" },
				"created_at": { "type": "date" }
			}
		}
	}`

	// Create index
	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}
	res, err := req.Do(ctx, client)
	require.NoError(t, err, "Failed to create index")
	defer res.Body.Close()
	require.False(t, res.IsError(), "Error creating index: %s", res.String())
}

// Product represents a test document
type Product struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Price       float64   `json:"price"`
	Category    string    `json:"category"`
	Tags        []string  `json:"tags"`
	CreatedAt   time.Time `json:"created_at"`
}

// indexTestDocuments indexes test documents
func indexTestDocuments(t *testing.T, ctx context.Context, client *elasticsearch.Client, indexName string) {
	products := []Product{
		{
			Name:        "Laptop",
			Description: "High-performance laptop with 16GB RAM",
			Price:       1299.99,
			Category:    "electronics",
			Tags:        []string{"computer", "laptop", "tech"},
			CreatedAt:   time.Now(),
		},
		{
			Name:        "Smartphone",
			Description: "Latest smartphone with advanced camera",
			Price:       899.99,
			Category:    "electronics",
			Tags:        []string{"phone", "mobile", "tech"},
			CreatedAt:   time.Now(),
		},
		{
			Name:        "Headphones",
			Description: "Noise-cancelling wireless headphones",
			Price:       249.99,
			Category:    "accessories",
			Tags:        []string{"audio", "wireless", "tech"},
			CreatedAt:   time.Now(),
		},
	}

	// Bulk index the products
	for i, product := range products {
		productJSON, err := json.Marshal(product)
		require.NoError(t, err, "Failed to marshal product")

		req := esapi.IndexRequest{
			Index:      indexName,
			DocumentID: fmt.Sprintf("product-%d", i+1),
			Body:       strings.NewReader(string(productJSON)),
			Refresh:    "true",
		}

		res, err := req.Do(ctx, client)
		require.NoError(t, err, "Failed to index document")
		defer res.Body.Close()
		require.False(t, res.IsError(), "Error indexing document: %s", res.String())
	}

	// Ensure documents are indexed by refreshing the index
	refreshReq := esapi.IndicesRefreshRequest{
		Index: []string{indexName},
	}
	res, err := refreshReq.Do(ctx, client)
	require.NoError(t, err, "Failed to refresh index")
	defer res.Body.Close()
}

// indexTestDocuments indexes test documents
func indexManyTestDocuments(t *testing.T, ctx context.Context, client *elasticsearch.Client, indexName string, count int) {
	// Bulk index the products
	for i := range count {
		productJSON, err := json.Marshal(Product{
			Name:        fmt.Sprintf("Laptop %04d", i),
			Description: "High-performance laptop with 16GB RAM",
			Price:       100.00 + float64(i),
			Category:    "electronics",
			Tags:        []string{"computer", "laptop", "tech"},
			CreatedAt:   time.Now(),
		})
		require.NoError(t, err, "Failed to marshal product")

		req := esapi.IndexRequest{
			Index:      indexName,
			DocumentID: fmt.Sprintf("product-%d", i+1),
			Body:       strings.NewReader(string(productJSON)),
			Refresh:    "true",
		}

		res, err := req.Do(ctx, client)
		require.NoError(t, err, "Failed to index document")
		defer res.Body.Close()
		require.False(t, res.IsError(), "Error indexing document: %s", res.String())
	}

	// Ensure documents are indexed by refreshing the index
	refreshReq := esapi.IndicesRefreshRequest{
		Index: []string{indexName},
	}
	res, err := refreshReq.Do(ctx, client)
	require.NoError(t, err, "Failed to refresh index")
	defer res.Body.Close()
}

// testSearchDocuments tests search functionality
func testSearchDocuments(t *testing.T, ctx context.Context, client *elasticsearch.Client, indexName string) {
	// Test 1: Search for electronics
	t.Run("Search by category", func(t *testing.T) {
		query := map[string]any{
			"query": map[string]any{
				"match": map[string]any{
					"category": "electronics",
				},
			},
		}

		queryJSON, err := json.Marshal(query)
		require.NoError(t, err, "Failed to marshal query")

		req := esapi.SearchRequest{
			Index: []string{indexName},
			Body:  strings.NewReader(string(queryJSON)),
		}

		res, err := req.Do(ctx, client)
		require.NoError(t, err, "Search request failed")
		defer res.Body.Close()
		require.False(t, res.IsError(), "Search returned error: %s", res.String())

		var searchResult map[string]any
		err = json.NewDecoder(res.Body).Decode(&searchResult)
		require.NoError(t, err, "Failed to parse search response")

		hits := searchResult["hits"].(map[string]any)["hits"].([]any)
		assert.Equal(t, 2, len(hits), "Expected 2 electronics products")
	})

	// Test 2: Search by price range
	t.Run("Search by price range", func(t *testing.T) {
		query := map[string]any{
			"query": map[string]any{
				"range": map[string]any{
					"price": map[string]any{
						"gte": 500,
						"lte": 1000,
					},
				},
			},
		}

		queryJSON, err := json.Marshal(query)
		require.NoError(t, err, "Failed to marshal query")

		req := esapi.SearchRequest{
			Index: []string{indexName},
			Body:  strings.NewReader(string(queryJSON)),
		}

		res, err := req.Do(ctx, client)
		require.NoError(t, err, "Search request failed")
		defer res.Body.Close()
		require.False(t, res.IsError(), "Search returned error: %s", res.String())

		var searchResult map[string]any
		err = json.NewDecoder(res.Body).Decode(&searchResult)
		require.NoError(t, err, "Failed to parse search response")

		hits := searchResult["hits"].(map[string]any)["hits"].([]any)
		assert.Equal(t, 1, len(hits), "Expected 1 product in the price range")
	})

	// Test 3: Full-text search
	t.Run("Full-text search", func(t *testing.T) {
		query := map[string]any{
			"query": map[string]any{
				"multi_match": map[string]any{
					"query":  "wireless",
					"fields": []string{"name", "description", "tags"},
				},
			},
		}

		queryJSON, err := json.Marshal(query)
		require.NoError(t, err, "Failed to marshal query")

		req := esapi.SearchRequest{
			Index: []string{indexName},
			Body:  strings.NewReader(string(queryJSON)),
		}

		res, err := req.Do(ctx, client)
		require.NoError(t, err, "Search request failed")
		defer res.Body.Close()
		require.False(t, res.IsError(), "Search returned error: %s", res.String())

		var searchResult map[string]any
		err = json.NewDecoder(res.Body).Decode(&searchResult)
		require.NoError(t, err, "Failed to parse search response")

		hits := searchResult["hits"].(map[string]any)["hits"].([]any)
		assert.Equal(t, 1, len(hits), "Expected 1 product with 'wireless' term")
	})
}

// testBackendImplementation tests our ElasticBackend implementation
func testBackendImplementation(t *testing.T, esURL string, indexName string) {
	// Create our backend
	backend, err := NewElasticBackend([]string{esURL})
	require.NoError(t, err, "Failed to create ElasticBackend")

	// Create a request
	request := NewRequest()

	// Create a query builder with the request
	builder := NewQueryBuilder(request, indexName)

	// Add a match query for electronics category
	builder.WithMatchQuery("category", "electronics")

	// Execute the query
	ctx := context.Background()
	result, err := backend.Execute(ctx, builder)
	require.NoError(t, err, "Failed to execute query")

	// Verify results
	assert.Equal(t, int64(2), result.TotalHitCount, "Expected 2 electronics products")
	assert.Equal(t, 2, len(result.Hits), "Expected 2 items in result")
}

// TestElasticsearchMultipleQueries demonstrates running multiple queries
func TestElasticsearchMultipleQueries(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup Elasticsearch container
	ctx := context.Background()
	elasticsearchContainer, esURL := setupElasticsearchContainer(t, ctx)
	defer terminateContainer(t, ctx, elasticsearchContainer)

	// Create Elasticsearch client
	esClient, err := createElasticsearchClient(esURL)
	require.NoError(t, err, "Failed to create Elasticsearch client")

	// Create test index and add documents
	indexName := "test-products-multi"
	createIndex(t, ctx, esClient, indexName)
	indexTestDocuments(t, ctx, esClient, indexName)

	// Create our backend
	backend, err := NewElasticBackend([]string{esURL})
	require.NoError(t, err, "Failed to create ElasticBackend")

	// Create requests and query builders
	electronicsRequest := NewRequest()
	electronicsQuery := NewQueryBuilder(electronicsRequest, indexName)
	electronicsQuery.WithMatchQuery("category", "electronics")

	accessoriesRequest := NewRequest()
	accessoriesQuery := NewQueryBuilder(accessoriesRequest, indexName)
	accessoriesQuery.WithMatchQuery("category", "accessories")

	// Execute multiple queries
	results, err := backend.ExecuteMultiple(ctx, []*QueryBuilder{electronicsQuery, accessoriesQuery})
	require.NoError(t, err, "Failed to execute multiple queries")

	// Verify results
	assert.Equal(t, 2, len(results), "Expected 2 result sets")
	assert.Equal(t, int64(2), results[0].TotalHitCount, "Expected 2 electronics products")
	assert.Equal(t, int64(1), results[1].TotalHitCount, "Expected 1 accessories product")
}

// TestElasticsearchAggregations demonstrates using aggregations
func TestElasticsearchAggregations(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup Elasticsearch container
	ctx := context.Background()
	elasticsearchContainer, esURL := setupElasticsearchContainer(t, ctx)
	defer terminateContainer(t, ctx, elasticsearchContainer)

	// Create Elasticsearch client
	esClient, err := createElasticsearchClient(esURL)
	require.NoError(t, err, "Failed to create Elasticsearch client")

	// Create test index and add documents
	indexName := "test-products-aggs"
	createIndex(t, ctx, esClient, indexName)
	indexTestDocuments(t, ctx, esClient, indexName)

	// Create our backend
	backend, err := NewElasticBackend([]string{esURL})
	require.NoError(t, err, "Failed to create ElasticBackend")

	// Create a request
	request := NewRequest()

	// Create a query builder with aggregations
	builder := NewQueryBuilder(request, indexName)

	// Add a match all query (using an empty match query as a workaround)
	builder.WithMatchQuery("_all", "")

	// Add aggregations
	builder.AddTermsAggregation("categories", "category", 10)

	// Execute the query
	result, err := builder.Execute(ctx, backend)
	require.NoError(t, err, "Failed to execute query with aggregations")

	// Verify aggregation results
	assert.NotNil(t, result.Aggregations, "Expected aggregations in result")
}

// TestElasticsearchPagination demonstrates using pagination
func TestElasticsearchPagination(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup Elasticsearch container
	ctx := context.Background()
	elasticsearchContainer, esURL := setupElasticsearchContainer(t, ctx)
	defer terminateContainer(t, ctx, elasticsearchContainer)

	// Create Elasticsearch client
	esClient, err := createElasticsearchClient(esURL)
	require.NoError(t, err, "Failed to create Elasticsearch client")

	// Create test index and add documents
	indexName := "test-products-pagination"
	createIndex(t, ctx, esClient, indexName)
	indexManyTestDocuments(t, ctx, esClient, indexName, 50)

	// Create our backend
	backend, err := NewElasticBackend([]string{esURL})
	require.NoError(t, err, "Failed to create ElasticBackend")

	// Create a request
	request := NewRequest()

	// Create a query builder with aggregations
	builder := NewQueryBuilder(request, indexName)

	builder.WithMatchQuery("category", "electronics")
	builder.Sort("price", sortorder.Asc)
	builder.SetSize(10) // Page 2, 10 items per page
	builder.SetFrom(20)

	// Execute the query
	result, err := builder.Execute(ctx, backend)
	require.NoError(t, err, "Failed to execute query with aggregations")

	// Verify pagination results
	assert.Equal(t, 10, len(result.Hits), "Expected 10 items in result")
	assert.Equal(t, int64(50), result.TotalHitCount, "Expected 50 total items")

	// Check the first item
	assert.Equal(t, "Laptop 0020", result.Hits[0]["name"], "Expected first item to be 'Laptop 0020'")
}

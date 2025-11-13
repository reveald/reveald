package hampusodanielstester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/reveald/reveald/v2"
	"github.com/reveald/reveald/v2/featureset"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	elasticContainer "github.com/testcontainers/testcontainers-go/modules/elasticsearch"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	elasticVersion = "8.12.1" // Use the version compatible with your client
	testIndex      = "test-index"
)

// setupElasticsearch starts an Elasticsearch container and returns its connection details
func setupElasticsearch(t *testing.T) (*elasticContainer.ElasticsearchContainer, string) {
	ctx := context.Background()

	// Create and start the Elasticsearch container
	elasticContainer, err := elasticContainer.RunContainer(ctx,
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

// createElasticsearchClient creates an Elasticsearch client
func createElasticsearchClient(esURL string) (*elasticsearch.TypedClient, error) {
	config := elasticsearch.Config{
		Addresses: []string{esURL},
	}
	return elasticsearch.NewTypedClient(config)
}

func createTestIndex(t *testing.T, client *elasticsearch.TypedClient) {
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
	res, err := client.Indices.Create(testIndex).
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
		res, err := client.Index(testIndex).
			Id(fmt.Sprintf("doc-%d", i+1)).
			Request(doc).
			Do(ctx)
		require.NoError(t, err, "Failed to index document")
		require.NotEmpty(t, res.Id_, "Document ID should not be empty")
	}

	// Force refresh to make documents searchable immediately
	refreshRes, err := client.Indices.Refresh().
		Index(testIndex).
		Do(ctx)
	require.NoError(t, err, "Failed to refresh index")
	require.NotNil(t, refreshRes, "Refresh response should not be nil")
}

func TestEverythin(t *testing.T) {
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

	elasticClient, err := createElasticsearchClient(httpURL)
	require.NoError(t, err, "Failed to create Elasticsearch client")

	// Create test index and populate with data
	createTestIndex(t, elasticClient)

	backend, err := reveald.NewElasticBackend([]string{httpURL})
	require.NoError(t, err, "Failed to create Reveald Elasticsearch backend")

	t.Run("TestBasicSearch", func(t *testing.T) {
		ctx := context.Background()

		ep := reveald.NewEndpoint(backend, reveald.WithIndices(testIndex))

		err = ep.Register(
			featureset.NewDynamicFilterFeature("title"),
		)
		require.NoError(t, err, "Failed to register features")

		req := reveald.NewRequest(
			reveald.NewParameter("title", "Product 1"),
		)

		res, err := ep.Execute(ctx, req)
		require.NoError(t, err, "Failed to execute search")

		assert.Len(t, res.Hits, 1)
	})

}

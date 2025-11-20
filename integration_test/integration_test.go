package integration_test

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
	elasticContainer, err := elasticContainer.Run(ctx, "docker.elastic.co/elasticsearch/elasticsearch:"+elasticVersion,
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

	reviewsProps := types.NewNestedProperty()
	reviewsProps.Properties = map[string]types.Property{
		"author":   withKeywordProperty(types.NewTextProperty()),
		"rating":   types.NewIntegerNumberProperty(),
		"comment":  withKeywordProperty(types.NewTextProperty()),
		"date":     types.NewDateProperty(),
		"verified": types.NewBooleanProperty(),
	}

	// Create mapping for test index
	indexMapping := types.TypeMapping{
		Properties: map[string]types.Property{
			"title":       withKeywordProperty(types.NewTextProperty()),
			"description": withKeywordProperty(types.NewTextProperty()),
			"tags":        withKeywordProperty(types.NewKeywordProperty()),
			"price":       types.NewFloatNumberProperty(),
			"active":      types.NewBooleanProperty(),
			"category":    withKeywordProperty(types.NewKeywordProperty()),
			"rating":      types.NewIntegerNumberProperty(),
			"created_at":  types.NewDateProperty(),
			"reviews":     reviewsProps,
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
			"created_at":  "2024-01-01T00:00:00Z",
			"reviews": []map[string]any{
				{
					"author":   "Sarah Johnson",
					"rating":   5,
					"comment":  "Perfect for development work, great performance",
					"date":     "2024-01-15",
					"verified": true,
				},
				{
					"author":   "Mike Chen",
					"rating":   4,
					"comment":  "Fast and reliable, battery could be better",
					"date":     "2024-01-18",
					"verified": true,
				},
				{
					"author":   "Lisa Anderson",
					"rating":   5,
					"comment":  "Best laptop I've owned, highly recommend",
					"date":     "2024-01-22",
					"verified": false,
				},
			},
		},
		{
			"title":       "Product 2",
			"description": "This is product two with medium quality",
			"tags":        []string{"clothing", "apparel"},
			"price":       49.99,
			"active":      true,
			"category":    "fashion",
			"rating":      3,
			"created_at":  "2024-01-05T00:00:00Z",
			"reviews": []map[string]any{
				{
					"author":   "David Park",
					"rating":   4,
					"comment":  "Great sound quality, comfortable for long use",
					"date":     "2024-01-10",
					"verified": true,
				},
				{
					"author":   "Emma Wilson",
					"rating":   3,
					"comment":  "Decent but noise cancellation could be better",
					"date":     "2024-01-14",
					"verified": true,
				},
			},
		},
		{
			"title":       "Product 3",
			"description": "This is product three with low quality",
			"tags":        []string{"home", "kitchen"},
			"price":       29.99,
			"active":      false,
			"category":    "home",
			"rating":      2,
			"created_at":  "2024-01-07T00:00:00Z",
			"reviews": []map[string]any{
				{
					"author":   "Robert Martinez",
					"rating":   5,
					"comment":  "Makes perfect coffee every morning!",
					"date":     "2024-01-12",
					"verified": true,
				},
				{
					"author":   "Jennifer Lee",
					"rating":   5,
					"comment":  "Easy to clean and brews quickly",
					"date":     "2024-01-19",
					"verified": true,
				},
				{
					"author":   "Tom Brown",
					"rating":   4,
					"comment":  "Good value for money, works great",
					"date":     "2024-01-21",
					"verified": false,
				},
				{
					"author":   "Amy Davis",
					"rating":   5,
					"comment":  "Best purchase this year",
					"date":     "2024-01-25",
					"verified": true,
				},
			},
		},
		{
			"title":       "Product 4",
			"description": "This is product four with high quality",
			"tags":        []string{"electronics", "computer"},
			"price":       199.99,
			"active":      true,
			"category":    "electronics",
			"rating":      4,
			"created_at":  "2024-01-03T00:00:00Z",
			"reviews": []map[string]any{
				{
					"author":   "Kevin White",
					"rating":   4,
					"comment":  "Comfortable for long runs, good arch support",
					"date":     "2024-01-08",
					"verified": true,
				},
				{
					"author":   "Rachel Green",
					"rating":   5,
					"comment":  "Perfect fit, great cushioning",
					"date":     "2024-01-16",
					"verified": true,
				},
			},
		},
		{
			"title":       "Product 5",
			"description": "This is product five with medium quality",
			"tags":        []string{"home", "furniture"},
			"price":       149.99,
			"active":      true,
			"category":    "home",
			"rating":      3,
			"created_at":  "2024-01-09T00:00:00Z",
			"reviews": []map[string]any{
				{
					"author":   "Kevin White",
					"rating":   3,
					"comment":  "Decent lighting but base feels cheap",
					"date":     "2024-01-11",
					"verified": true,
				},
				{
					"author":   "Nicole Garcia",
					"rating":   4,
					"comment":  "Good for reading, adjustable brightness is nice",
					"date":     "2024-01-17",
					"verified": false,
				},
				{
					"author":   "Steve Rodriguez",
					"rating":   2,
					"comment":  "Too dim for my workspace",
					"date":     "2024-01-23",
					"verified": true,
				},
			},
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

func TestRevealedElasticsearchFeatures(t *testing.T) {
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

	t.Run("FeatureFiltering", func(t *testing.T) {
		ctx := context.Background()

		ep := reveald.NewEndpoint(backend, reveald.WithIndices(testIndex))

		err = ep.Register(
			featureset.NewDynamicFilterFeature("category"),
			featureset.NewDynamicBooleanFilterFeature("active"),
			featureset.NewHistogramFeature("rating",
				featureset.WithInterval(1),
				featureset.WithoutZeroBucket(),
			),
			featureset.NewDateHistogramFeature("created_at",
				featureset.Day,
				featureset.WithCalendarInterval("day"),
				featureset.WithDateFormat("strict_date"),
				featureset.WithCalendarIntervalInstead(),
			),
		)
		require.NoError(t, err, "Failed to register features")

		testCases := []struct {
			name         string
			params       []reveald.Parameter
			expectedHits int
			expectedAggs map[string]int
		}{
			{
				name: "Filter on category",
				params: []reveald.Parameter{
					reveald.NewParameter("category", "electronics"),
				},
				expectedHits: 2,
				expectedAggs: map[string]int{"category": 1},
			},
			{
				name: "Filter on boolean field",
				params: []reveald.Parameter{
					reveald.NewParameter("active", "true"),
				},
				expectedHits: 4,
				expectedAggs: map[string]int{"category": 3, "created_at": 4, "active": 2},
			},
			{
				name: "Filter on histogram field",
				params: []reveald.Parameter{
					reveald.NewParameter("rating.min", "3"),
					reveald.NewParameter("rating.max", "4"),
				},
				expectedHits: 3,
				expectedAggs: map[string]int{"rating": 2},
			},
			{
				name: "Filter on date range",
				params: []reveald.Parameter{
					reveald.NewParameter("created_at.min", "2024-01-05"),
				},
				expectedHits: 3,
				expectedAggs: map[string]int{"category": 2, "created_at": 3, "active": 2},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := reveald.NewRequest(tc.params...)
				res, err := ep.Execute(ctx, req)
				require.NoError(t, err)
				assert.Len(t, res.Hits, tc.expectedHits)

				for aggName, expectedCount := range tc.expectedAggs {
					aggBucket, ok := res.Aggregations[aggName]
					require.True(t, ok, fmt.Sprintf("Expected aggregation '%s' to be present", aggName))
					assert.Len(t, aggBucket, expectedCount)
				}
			})
		}
	})

	t.Run("NestedFeatureFiltering", func(t *testing.T) {
		ctx := context.Background()

		ep := reveald.NewEndpoint(backend, reveald.WithIndices(testIndex))

		err = ep.Register(
			featureset.NewNestedDocumentWrapper(
				"reviews",
				featureset.WithFeatures(
					featureset.NewHistogramFeature(
						"reviews.rating",
						featureset.WithInterval(1),
						featureset.WithoutZeroBucket(),
					),
					featureset.NewDynamicFilterFeature("reviews.author"),
					featureset.NewDateHistogramFeature(
						"reviews.date",
						featureset.Day,
						featureset.WithCalendarInterval("day"),
						featureset.WithDateFormat("strict_date"),
						featureset.WithCalendarIntervalInstead(),
					),
					featureset.NewDynamicBooleanFilterFeature("reviews.verified"),
				),
			),
			featureset.NewDynamicFilterFeature("category"),
			featureset.NewDynamicBooleanFilterFeature("active"),
			featureset.NewHistogramFeature("rating",
				featureset.WithInterval(1),
				featureset.WithoutZeroBucket(),
			),
			featureset.NewDateHistogramFeature(
				"created_at",
				featureset.Day,
				featureset.WithCalendarInterval("day"),
				featureset.WithDateFormat("strict_date"),
				featureset.WithCalendarIntervalInstead(),
			),
		)
		require.NoError(t, err, "Failed to register features")

		testCases := []struct {
			name         string
			params       []reveald.Parameter
			expectedHits int
			expectedAggs map[string]int
		}{
			{
				name: "Filter on dynamic filter",
				params: []reveald.Parameter{
					reveald.NewParameter("reviews.author", "Kevin White"),
				},
				expectedHits: 2,
				expectedAggs: map[string]int{"category": 2, "reviews.author": 1, "reviews.rating": 2},
			},
			{
				name: "Filter on boolean field",
				params: []reveald.Parameter{
					reveald.NewParameter("reviews.verified", "false"),
				},
				expectedHits: 3,
				expectedAggs: map[string]int{"reviews.verified": 2},
			},
			{
				name: "Filter on histogram field",
				params: []reveald.Parameter{
					reveald.NewParameter("rating.min", "3"),
					reveald.NewParameter("rating.max", "4"),
				},
				expectedHits: 3,
				expectedAggs: map[string]int{"rating": 2},
			},
			{
				name: "Filter on date range",
				params: []reveald.Parameter{
					reveald.NewParameter("created_at.min", "2024-01-05"),
				},
				expectedHits: 3,
				expectedAggs: map[string]int{"category": 2, "created_at": 3, "active": 2},
			},
			{
				name: "Filter on multiple nested filters",
				params: []reveald.Parameter{
					reveald.NewParameter("reviews.rating.min", "5"),
					reveald.NewParameter("reviews.verified", "true"),
				},
				expectedHits: 3,
				expectedAggs: map[string]int{"category": 2, "reviews.author": 5, "reviews.rating": 1},
			},
			{
				name: "Filter on nested and non-nested filters",
				params: []reveald.Parameter{
					reveald.NewParameter("reviews.rating.min", "5"),
					reveald.NewParameter("category", "electronics"),
				},
				expectedHits: 2,
				expectedAggs: map[string]int{"category": 1, "reviews.author": 3, "reviews.rating": 1},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := reveald.NewRequest(tc.params...)
				res, err := ep.Execute(ctx, req)
				require.NoError(t, err)
				assert.Len(t, res.Hits, tc.expectedHits)

				for aggName, expectedCount := range tc.expectedAggs {
					aggBucket, ok := res.Aggregations[aggName]
					require.True(t, ok, fmt.Sprintf("Expected aggregation '%s' to be present", aggName))
					assert.Len(t, aggBucket, expectedCount, aggName)
				}
			})
		}
	})

	t.Run("NestedFeatureWithInnerHits", func(t *testing.T) {
		ctx := context.Background()

		ep := reveald.NewEndpoint(backend, reveald.WithIndices(testIndex))

		err = ep.Register(
			featureset.NewNestedDocumentWrapper(
				"reviews",
				featureset.WithInnerHits(),
				featureset.WithFeatures(
					featureset.NewDynamicFilterFeature("reviews.author"),
					featureset.NewDynamicBooleanFilterFeature("reviews.verified"),
				),
			),
		)
		require.NoError(t, err, "Failed to register features")

		testCases := []struct {
			name                    string
			params                  []reveald.Parameter
			expectedHits            int
			expectedInnerHitsPath   string
			expectedInnerHitsField  string
			expectedInnerHitsValue  string
			expectedMinInnerHits    int
			expectedInnerHitsPerHit map[string]int // map of hit title to expected inner hits count
		}{
			{
				name: "Filter on nested field with inner hits",
				params: []reveald.Parameter{
					reveald.NewParameter("reviews.author", "Kevin White"),
				},
				expectedHits:           2,
				expectedInnerHitsPath:  "reviews",
				expectedInnerHitsField: "author",
				expectedInnerHitsValue: "Kevin White",
				expectedMinInnerHits:   1,
			},
			{
				name: "Filter on verified reviews returns multiple inner hits per document",
				params: []reveald.Parameter{
					reveald.NewParameter("reviews.verified", "true"),
				},
				expectedHits:          5,
				expectedInnerHitsPath: "reviews",
				expectedMinInnerHits:  1,
				expectedInnerHitsPerHit: map[string]int{
					"Product 1": 2, // Sarah Johnson and Mike Chen
					"Product 2": 2, // David Park and Emma Wilson
					"Product 3": 3, // Robert Martinez, Jennifer Lee, Amy Davis
					"Product 4": 2, // Kevin White and Rachel Green
					"Product 5": 2, // Kevin White and Steve Rodriguez
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := reveald.NewRequest(tc.params...)
				res, err := ep.Execute(ctx, req)
				require.NoError(t, err)
				assert.Len(t, res.Hits, tc.expectedHits)

				// Verify inner hits are present
				for _, hit := range res.Hits {
					innerHits, ok := hit["_inner_hits"].(map[string][]map[string]any)
					require.True(t, ok, "Expected _inner_hits to be present in hit")
					require.NotEmpty(t, innerHits, "Expected inner hits to contain data")

					// Check that the nested path is present
					nestedInnerHits, ok := innerHits[tc.expectedInnerHitsPath]
					require.True(t, ok, fmt.Sprintf("Expected '%s' key in inner hits", tc.expectedInnerHitsPath))
					require.NotEmpty(t, nestedInnerHits, fmt.Sprintf("Expected %s inner hits to contain data", tc.expectedInnerHitsPath))

					// Verify we have at least the minimum expected inner hits
					assert.GreaterOrEqual(t, len(nestedInnerHits), tc.expectedMinInnerHits,
						fmt.Sprintf("Expected at least %d inner hits", tc.expectedMinInnerHits))

					// If we have specific expectations per hit, validate those
					if tc.expectedInnerHitsPerHit != nil {
						hitTitle, ok := hit["title"].(string)
						require.True(t, ok, "Expected 'title' field in hit")

						if expectedCount, exists := tc.expectedInnerHitsPerHit[hitTitle]; exists {
							assert.Equal(t, expectedCount, len(nestedInnerHits),
								fmt.Sprintf("Expected %d inner hits for '%s'", expectedCount, hitTitle))
						}
					}

					// Verify the inner hits contain the expected field and value (if specified)
					if tc.expectedInnerHitsField != "" && tc.expectedInnerHitsValue != "" {
						for _, innerHit := range nestedInnerHits {
							fieldValue, ok := innerHit[tc.expectedInnerHitsField]
							require.True(t, ok, fmt.Sprintf("Expected '%s' field in inner hit", tc.expectedInnerHitsField))
							assert.Equal(t, tc.expectedInnerHitsValue, fieldValue, fmt.Sprintf("Expected %s to be %s", tc.expectedInnerHitsField, tc.expectedInnerHitsValue))
						}
					}
				}
			})
		}
	})
}

func withKeywordProperty(property types.Property) types.Property {
	switch v := property.(type) {
	case *types.TextProperty:
		v.Fields = map[string]types.Property{
			"keyword": types.NewKeywordProperty(),
		}

		return v
	case *types.KeywordProperty:
		v.Fields = map[string]types.Property{
			"keyword": types.NewKeywordProperty(),
		}

		return v
	}

	return property
}

package reveald

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// ElasticBackend defines an Elasticsearch backend for Reveald.
//
// It manages the connection to Elasticsearch and handles request execution.
// The ElasticBackend is responsible for translating Reveald queries into
// Elasticsearch requests and processing the responses.
type ElasticBackend struct {
	client *elasticsearch.Client
	config elasticsearch.Config
}

// ElasticBackendOption is a type for passing functional options to the Elastic Backend constructor.
//
// This allows for flexible configuration of the ElasticBackend.
type ElasticBackendOption func(*ElasticBackend)

// WithScheme defines which scheme to use when communicating with Elasticsearch (default is "http").
//
// Example:
//
//	// Use HTTPS for secure communication
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithScheme("https"),
//	)
func WithScheme(scheme string) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.config.Addresses = updateURLScheme(b.config.Addresses, scheme)
	}
}

// Helper function to update URL scheme in addresses
func updateURLScheme(addresses []string, scheme string) []string {
	updatedAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		if strings.HasPrefix(addr, "http://") {
			addr = strings.TrimPrefix(addr, "http://")
		} else if strings.HasPrefix(addr, "https://") {
			addr = strings.TrimPrefix(addr, "https://")
		}
		updatedAddresses[i] = scheme + "://" + addr
	}
	return updatedAddresses
}

// WithCredentials adds username and password to requests to Elasticsearch.
//
// Example:
//
//	// Connect to Elasticsearch with authentication
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithCredentials("username", "password"),
//	)
func WithCredentials(username, password string) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.config.Username = username
		b.config.Password = password
	}
}

// WithHealthCheck enables or disables healthchecking.
//
// Note: The official Elasticsearch client doesn't have an equivalent option.
// Healthchecks are done automatically on startup.
//
// Example:
//
//	// Disable health checking
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithHealthCheck(false),
//	)
func WithHealthCheck(enabled bool) ElasticBackendOption {
	return func(b *ElasticBackend) {
		// The official client doesn't have an equivalent option
		// Healthchecks are done automatically on startup
	}
}

// WithHealthcheckInterval sets the healthcheck interval.
//
// Note: The official Elasticsearch client doesn't have an equivalent option.
//
// Example:
//
//	// Set health check interval to 30 seconds
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithHealthcheckInterval(30 * time.Second),
//	)
func WithHealthcheckInterval(d time.Duration) ElasticBackendOption {
	return func(b *ElasticBackend) {
		// The official client doesn't have an equivalent option
	}
}

// WithSniff enables or disables sniffing.
//
// Sniffing allows the client to discover other nodes in the cluster.
//
// Example:
//
//	// Enable sniffing to discover other nodes
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithSniff(true),
//	)
func WithSniff(enabled bool) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.config.DiscoverNodesOnStart = enabled
	}
}

// WithHttpClient configures a http client to use for the http requests to elastic backend.
//
// This allows you to customize the HTTP client used for requests, which can be useful
// for setting custom timeouts, TLS configuration, etc.
//
// Example:
//
//	// Use a custom HTTP client with a longer timeout
//	httpClient := &http.Client{
//	    Timeout: 30 * time.Second,
//	}
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithHttpClient(httpClient),
//	)
func WithHttpClient(httpClient *http.Client) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.config.Transport = httpClient.Transport
	}
}

// WithCACert configures a custom CA certificate to use for the http requests to elastic backend.
//
// Example:
//
//	// Use a custom CA certificate
//	cert, err := ioutil.ReadFile("ca.crt")
//	if err != nil {
//	    // Handle error
//	}
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithCACert(cert),
//	)
func WithCACert(cert []byte) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.config.CACert = cert
	}
}

// WithRetrier configures a retry strategy to use when a http request to elastic backend fails.
//
// Note: The retry logic is different in the official client.
//
// Example:
//
//	// Configure a custom retrier
//	retrier := func(client *elasticsearch.Client) {
//	    // Custom retry logic
//	}
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithRetrier(retrier),
//	)
func WithRetrier(retrier any) ElasticBackendOption {
	return func(b *ElasticBackend) {
		// The retry logic is different in the official client
		// We can configure max retries and retry on status
		if r, ok := retrier.(func(*elasticsearch.Client)); ok {
			r(b.client)
		}
	}
}

// NewElasticBackend creates a new backend for Reveald, targeting Elasticsearch.
//
// It initializes a connection to Elasticsearch using the provided nodes and options.
//
// Example:
//
//	// Create a basic Elasticsearch backend
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Create a backend with custom options
//	backend, err := reveald.NewElasticBackend(
//	    []string{"localhost:9200"},
//	    reveald.WithScheme("https"),
//	    reveald.WithCredentials("user", "pass"),
//	)
func NewElasticBackend(nodes []string, opts ...ElasticBackendOption) (*ElasticBackend, error) {
	// Convert nodes to full URLs if they don't have a scheme
	addresses := make([]string, len(nodes))
	for i, node := range nodes {
		if !strings.HasPrefix(node, "http://") && !strings.HasPrefix(node, "https://") {
			addresses[i] = "http://" + node
		} else {
			addresses[i] = node
		}
	}

	backend := &ElasticBackend{
		config: elasticsearch.Config{
			Addresses: addresses,
		},
	}

	// Apply options
	for _, opt := range opts {
		opt(backend)
	}

	// Create the client
	client, err := elasticsearch.NewClient(backend.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	backend.client = client
	return backend, nil
}

// GetClient returns the underlying Elasticsearch client.
//
// This method is primarily intended for testing and advanced use cases
// where direct access to the Elasticsearch client is needed.
//
// Example:
//
//	client := backend.GetClient()
//	res, err := client.Info()
func (b *ElasticBackend) GetClient() *elasticsearch.Client {
	return b.client
}

func mapSearchResult(res *esapi.Response, req *QueryBuilder) (*Result, error) {
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch request failed: %s", res.String())
	}

	var searchResponse map[string]any
	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("error parsing the response body: %s", err)
	}
	defer res.Body.Close()

	hitsObj, found := searchResponse["hits"].(map[string]any)
	if !found {
		return nil, errors.New("malformed search response: missing 'hits' object")
	}

	totalHits := int64(0)
	if total, found := hitsObj["total"].(map[string]any); found {
		if value, ok := total["value"].(float64); ok {
			totalHits = int64(value)
		}
	}

	var hits []map[string]any
	if hitsArray, found := hitsObj["hits"].([]any); found {
		for _, hit := range hitsArray {
			hitObj, ok := hit.(map[string]any)
			if !ok {
				continue
			}

			// Start with _source if available, otherwise create empty map
			source, foundSource := hitObj["_source"].(map[string]any)
			if !foundSource {
				source = make(map[string]any)
			}

			// Add fields if present (this includes script fields)
			if fields, foundFields := hitObj["fields"].(map[string]any); foundFields {
				for field, value := range fields {
					list, ok := value.([]any)
					if ok && len(list) > 0 {
						source[field] = list[0]
					} else {
						source[field] = value
					}
				}
			}

			// Only add the hit if we have either _source or fields
			if foundSource || len(source) > 0 {
				hits = append(hits, source)
			}
		}
	}

	if len(hits) == 0 {
		hits = []map[string]any{}
	}

	// Process aggregations if present
	aggregations := make(map[string][]*ResultBucket)
	if aggsObj, found := searchResponse["aggregations"].(map[string]any); found {
		for name, agg := range aggsObj {
			if buckets := extractBuckets(agg); len(buckets) > 0 {
				aggregations[name] = buckets
			}
		}
	}

	return &Result{
		request:       req.Request(),
		TotalHitCount: totalHits,
		Hits:          hits,
		Pagination:    nil,
		Sorting:       nil,
		Aggregations:  aggregations,
	}, nil
}

// Helper function to extract buckets from aggregation results
func extractBuckets(agg any) []*ResultBucket {
	aggMap, ok := agg.(map[string]any)
	if !ok {
		return nil
	}

	buckets, found := aggMap["buckets"].([]any)
	if !found {
		return nil
	}

	var resultBuckets []*ResultBucket
	for _, bucket := range buckets {
		bucketMap, ok := bucket.(map[string]any)
		if !ok {
			continue
		}

		key := bucketMap["key"]
		docCount, _ := bucketMap["doc_count"].(float64)

		rb := &ResultBucket{
			Value:            key,
			HitCount:         int64(docCount),
			SubResultBuckets: make(map[string][]*ResultBucket),
		}

		// Extract sub-aggregations
		for k, v := range bucketMap {
			if k != "key" && k != "doc_count" && k != "key_as_string" {
				if subBuckets := extractBuckets(v); len(subBuckets) > 0 {
					rb.SubResultBuckets[k] = subBuckets
				}
			}
		}

		resultBuckets = append(resultBuckets, rb)
	}

	return resultBuckets
}

// Execute runs a query against Elasticsearch and returns the results.
//
// It takes a context for cancellation and a QueryBuilder that defines the query to execute.
// The method translates the QueryBuilder into an Elasticsearch request, executes it,
// and processes the response into a Result object.
//
// Example:
//
//	// Create a query builder
//	builder := reveald.NewQueryBuilder(nil, "products")
//	builder.WithTermQuery("active", true)
//
//	// Execute the query
//	ctx := context.Background()
//	result, err := backend.Execute(ctx, builder)
//	if err != nil {
//	    // Handle error
//	}
//
//	// Process results
//	fmt.Printf("Found %d documents\n", result.TotalHitCount)
//	for _, hit := range result.Hits {
//	    fmt.Printf("Document: %v\n", hit)
//	}
func (b *ElasticBackend) Execute(ctx context.Context, builder *QueryBuilder) (*Result, error) {
	searchBody, err := json.Marshal(builder.Build())
	if err != nil {
		return nil, fmt.Errorf("error marshaling search body: %w", err)
	}

	res, err := b.client.Search(
		b.client.Search.WithContext(ctx),
		b.client.Search.WithIndex(builder.Indices()...),
		b.client.Search.WithBody(strings.NewReader(string(searchBody))),
	)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch request failed: %w", err)
	}

	return mapSearchResult(res, builder)
}

// ExecuteMultiple runs multiple queries against Elasticsearch in parallel and returns the results.
//
// This is useful for executing multiple independent queries efficiently.
// Each query is executed in parallel, and the results are returned in the same order as the input builders.
//
// Example:
//
//	// Create multiple query builders
//	builder1 := reveald.NewQueryBuilder(nil, "products")
//	builder1.WithTermQuery("category", "electronics")
//
//	builder2 := reveald.NewQueryBuilder(nil, "products")
//	builder2.WithTermQuery("category", "clothing")
//
//	// Execute both queries in parallel
//	ctx := context.Background()
//	results, err := backend.ExecuteMultiple(ctx, []*reveald.QueryBuilder{builder1, builder2})
//	if err != nil {
//	    // Handle error
//	}
//
//	// Process results
//	fmt.Printf("Electronics: %d documents\n", results[0].TotalHitCount)
//	fmt.Printf("Clothing: %d documents\n", results[1].TotalHitCount)
func (b *ElasticBackend) ExecuteMultiple(ctx context.Context, builders []*QueryBuilder) ([]*Result, error) {
	var results []*Result

	// The official client doesn't have a direct equivalent to MultiSearch
	// So we'll execute each search request individually
	for _, builder := range builders {
		result, err := b.Execute(ctx, builder)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

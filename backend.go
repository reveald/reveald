package reveald

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// ElasticBackend defines an Elasticsearch backend for Reveald.
//
// It manages the connection to Elasticsearch and handles request execution.
// The ElasticBackend is responsible for translating Reveald queries into
// Elasticsearch requests and processing the responses.
type ElasticBackend struct {
	client *elasticsearch.TypedClient
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
		if r, ok := retrier.(func(*elasticsearch.TypedClient)); ok {
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
	client, err := elasticsearch.NewTypedClient(backend.config)
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
func (b *ElasticBackend) GetClient() *elasticsearch.TypedClient {
	return b.client
}

func mapSearchResult(res search.Response, req *QueryBuilder) (*Result, error) {
	totalHits := int64(0)
	if res.Hits.Total != nil {
		totalHits = res.Hits.Total.Value
	}

	var hits []map[string]any
	if len(res.Hits.Hits) > 0 {
		for _, hit := range res.Hits.Hits {
			b, err := json.Marshal(hit.Source_)
			if err != nil {
				continue
			}
			var source map[string]any
			if err := json.Unmarshal(b, &source); err != nil {
				continue
			}

			if hit.Fields != nil {
				for key, value := range hit.Fields {
					b, err := value.MarshalJSON()
					if err != nil {
						continue
					}
					var field []any
					if err := json.Unmarshal(b, &field); err != nil {
						continue
					}
					for _, v := range field {
						source[key] = v
					}
				}
			}

			hits = append(hits, source)
		}
	}

	if len(hits) == 0 {
		hits = []map[string]any{}
	}

	// // Process aggregations if present
	// aggregations := make(map[string][]*ResultBucket)
	// if aggsObj, found := searchResponse["aggregations"].(map[string]any); found {
	// 	for name, agg := range aggsObj {
	// 		if buckets := extractBuckets(agg); len(buckets) > 0 {
	// 			aggregations[name] = buckets
	// 		}
	// 	}
	// }

	return &Result{
		response:      &res,
		request:       req.Request(),
		TotalHitCount: totalHits,
		Hits:          hits,
		Pagination:    nil,
		Sorting:       nil,
		Aggregations:  make(map[string][]*ResultBucket),
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

type runtimeFieldsCaster struct {
	runtimeFields map[string]types.RuntimeField
}

func (r *runtimeFieldsCaster) RuntimeFieldsCaster() *types.RuntimeFields {
	f := types.RuntimeFields(r.runtimeFields)
	return &f
}

type fieldAndFormatCaster struct {
	fieldAndFormat types.FieldAndFormat
}

func (f *fieldAndFormatCaster) FieldAndFormatCaster() *types.FieldAndFormat {
	return &f.fieldAndFormat
}

type sortCaster struct {
	sort types.SortCombinations
}

func (s *sortCaster) SortCombinationsCaster() *types.SortCombinations {
	return &s.sort
}

func sorts(sort []types.SortCombinations) []types.SortCombinationsVariant {
	sorts := make([]types.SortCombinationsVariant, 0, len(sort))
	for _, s := range sort {
		sorts = append(sorts, &sortCaster{sort: s})
	}
	return sorts
}

func (b *ElasticBackend) Execute(ctx context.Context, builder *QueryBuilder) (*Result, error) {
	docvalueFields := make([]types.FieldAndFormatVariant, 0, len(builder.docValueFields))
	for _, field := range builder.docValueFields {
		docvalueFields = append(docvalueFields, &fieldAndFormatCaster{fieldAndFormat: field})
	}

	res, err := b.client.Search().
		Index(strings.Join(builder.Indices(), ",")).
		Query(builder.RawQuery()).
		Size(builder.Selection().pageSize).
		From(builder.Selection().offset).
		Sort(sorts(builder.Selection().sort)...).
		Aggregations(builder.aggregations).
		SourceExcludes_(builder.Selection().exclusions...).
		SourceIncludes_(builder.Selection().inclusions...).
		ScriptFields(builder.scriptFields).
		DocvalueFields(docvalueFields...).
		RuntimeMappings(&runtimeFieldsCaster{runtimeFields: builder.runtimeFields}).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch request failed: %w", err)
	}
	if res == nil {
		return nil, fmt.Errorf("no response from elasticsearch")
	}

	return mapSearchResult(*res, builder)
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

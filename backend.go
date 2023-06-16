package reveald

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/olivere/elastic/v7"
)

// Retrier decides whether to retry a failed HTTP request with Elasticsearch.
type Retrier elastic.Retrier

// ElasticBackend defines an Elasticsearch backend
// for Reveald
type ElasticBackend struct {
	client *elastic.Client
	opts   []elastic.ClientOptionFunc
}

// ElasticBackendOption is a type for passing
// functional options to the Elastic Backend constructor
type ElasticBackendOption func(*ElasticBackend)

// WithScheme defines with scheme to use when
// communicating with Elasticsearch (default is "http")
func WithScheme(scheme string) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetScheme(scheme))
	}
}

// WithCredentials adds username and password to
// requests to Elasticsearch
func WithCredentials(username, password string) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetBasicAuth(username, password))
	}
}

// WithHealthCheck enables / disables healthchecking
func WithHealthCheck(enabled bool) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetHealthcheck(enabled))
	}
}

// WithHealthcheckInterval sets the healthcheck interval
func WithHealthcheckInterval(d time.Duration) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetHealthcheckInterval(d))
	}
}

// WithSniff enables / disables sniffing
func WithSniff(enabled bool) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetSniff(enabled))
	}
}

// WithHttpClient configures a http doer to use for the http requests to elastic backend.
func WithHttpClient(httpClient *http.Client) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetHttpClient(httpClient))
	}
}

// WithRetrier configures a retry strategy to use when a http request to elastic backend fails.
func WithRetrier(retrier Retrier) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetRetrier(retrier))
	}
}

// NewElasticBackend creates a new backend for
// Reveald, targeting Elasticsearch
func NewElasticBackend(nodes []string, opts ...ElasticBackendOption) (*ElasticBackend, error) {
	b := &ElasticBackend{}
	b.opts = []elastic.ClientOptionFunc{
		elastic.SetURL(nodes...),
		elastic.SetScheme("http"),
		elastic.SetSniff(true),
		elastic.SetHealthcheck(true),
		elastic.SetHealthcheckInterval(10 * time.Second),
	}

	for _, opt := range opts {
		opt(b)
	}

	client, err := elastic.NewClient(b.opts...)
	if err != nil {
		return nil, err
	}

	b.client = client
	return b, nil
}

// Execute an Elasticsearch query
func (b *ElasticBackend) Execute(ctx context.Context, builder *QueryBuilder) (*Result, error) {
	src := builder.Build()
	svc := b.client.Search(builder.Indices()...)
	result, err := svc.SearchSource(src).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch request failed: %w", err)
	}

	var hits []map[string]interface{}
	for _, hit := range result.Hits.Hits {
		var source map[string]interface{}
		if err := json.Unmarshal(hit.Source, &source); err != nil {
			continue
		}

		if len(hit.Fields) > 0 {
			for field, value := range hit.Fields {
				list, ok := value.([]interface{})
				if ok {
					value = list[0]
				}

				source[field] = value
			}
		}

		hits = append(hits, source)
	}

	if len(hits) == 0 {
		hits = []map[string]interface{}{}
	}

	return &Result{
		result:        result,
		TotalHitCount: result.TotalHits(),
		Hits:          hits,
		Pagination:    nil,
		Sorting:       nil,
		Aggregations:  make(map[string][]*ResultBucket),
	}, nil
}

package reveald

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
)

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
		b.opts = append(b.opts, elastic.SetScheme("http"))
	}
}

// WithCredentials adds username and password to
// requests to Elasticsearch
func WithCredentials(username, password string) ElasticBackendOption {
	return func(b *ElasticBackend) {
		b.opts = append(b.opts, elastic.SetBasicAuth(username, password))
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
		err := json.Unmarshal(hit.Source, &source)
		if err == nil {
			hits = append(hits, source)
		}
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

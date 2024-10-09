package reveald

import (
	"context"
	"fmt"
	"time"
)

// FeatureFunc is a type alias for the `next` feature
// to process a request/response stream
type FeatureFunc func(*QueryBuilder) (*Result, error)

// Feature is an interface which defines a Reveald
// search building block
type Feature interface {
	Process(*QueryBuilder, FeatureFunc) (*Result, error)
}

// Backend is an interface defining the backing
// search engine
type Backend interface {
	Execute(context.Context, *QueryBuilder) (*Result, error)
	ExecuteMultiple(context.Context, []*QueryBuilder) ([]*Result, error)
}

// Endpoint defines an entry point for a specific search
// query type
type Endpoint struct {
	backend  Backend
	indices  []string
	features []Feature
}

// Indices is a type alias for a string slice
type Indices []string

// WithIndices defines an index collection that
// an Endpoint should query
func WithIndices(index ...string) Indices {
	var collection Indices
	collection = append(collection, index...)
	return collection
}

// NewEndpoint returns a new Endpoint for a specific
// search query type
func NewEndpoint(backend Backend, indices Indices) *Endpoint {
	return &Endpoint{
		backend: backend,
		indices: indices,
	}
}

// Register a new set of features used when building
// a search query
func (e *Endpoint) Register(features ...Feature) error {
	e.features = append(e.features, features...)
	return nil
}

// Execute a search query request
func (e *Endpoint) Execute(ctx context.Context, request *Request) (*Result, error) {
	start := time.Now()
	builder := NewQueryBuilder(request, e.indices...)

	cc := &callchain{}
	for _, feature := range e.features {
		cc.add(feature)
	}

	result, err := cc.exec(builder, func(qb *QueryBuilder) (*Result, error) {
		return e.backend.Execute(ctx, qb)
	})
	if err != nil {
		return nil, fmt.Errorf("backend failed executing request: %w", err)
	}

	result.request = request
	result.Duration = time.Since(start)
	return result, nil
}

func (e *Endpoint) ExecuteMultiple(ctx context.Context, requests []*Request) ([]*Result, error) {
	queryBuilders := make([]*QueryBuilder, 0, len(requests))
	for _, req := range requests {
		builder := NewQueryBuilder(req, e.indices...)

		cc := &callchain{}
		for _, feature := range e.features {
			cc.add(feature)
		}

		queryBuilders = append(queryBuilders, builder)
	}

	results, err := e.backend.ExecuteMultiple(ctx, queryBuilders)
	if err != nil {
		return nil, fmt.Errorf("backend failed executing requests: %w", err)
	}

	return results, nil
}

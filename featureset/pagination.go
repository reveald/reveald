package featureset

import (
	"strconv"

	"github.com/reveald/reveald"
)

const (
	defaultPageSize int = 24
)

// PaginationFeature handles pagination for Elasticsearch queries.
//
// It processes pagination parameters from the request and applies them to the query,
// as well as adding pagination information to the result.
//
// Example:
//
//	// Create a basic pagination feature with default settings
//	paginationFeature := featureset.NewPaginationFeature()
//
//	// Use the pagination feature in a feature chain
//	result, err := paginationFeature.Process(builder, nextFeature)
type PaginationFeature struct {
	pageSize    int
	maxPageSize int
	maxOffset   int
}

// PaginationOption is a functional option for configuring a PaginationFeature.
type PaginationOption func(*PaginationFeature)

// WithPageSize sets the default page size for the pagination feature.
//
// Example:
//
//	// Create a pagination feature with a default page size of 10
//	paginationFeature := featureset.NewPaginationFeature(
//	    featureset.WithPageSize(10),
//	)
func WithPageSize(pageSize int) PaginationOption {
	return func(pf *PaginationFeature) {
		pf.pageSize = pageSize
	}
}

// WithMaxPageSize sets the maximum allowed page size for the pagination feature.
//
// This prevents clients from requesting too many documents at once.
//
// Example:
//
//	// Create a pagination feature with a maximum page size of 100
//	paginationFeature := featureset.NewPaginationFeature(
//	    featureset.WithMaxPageSize(100),
//	)
func WithMaxPageSize(maxPageSize int) PaginationOption {
	return func(pf *PaginationFeature) {
		pf.maxPageSize = maxPageSize
	}
}

// WithMaxOffset sets the maximum allowed offset for the pagination feature.
//
// This prevents clients from requesting pages that are too far into the result set,
// which can be inefficient in Elasticsearch.
//
// Example:
//
//	// Create a pagination feature with a maximum offset of 1000
//	paginationFeature := featureset.NewPaginationFeature(
//	    featureset.WithMaxOffset(1000),
//	)
func WithMaxOffset(maxOffset int) PaginationOption {
	return func(pf *PaginationFeature) {
		pf.maxOffset = maxOffset
	}
}

// NewPaginationFeature creates a new pagination feature with the specified options.
//
// Example:
//
//	// Create a pagination feature with custom settings
//	paginationFeature := featureset.NewPaginationFeature(
//	    featureset.WithPageSize(10),
//	    featureset.WithMaxPageSize(100),
//	    featureset.WithMaxOffset(1000),
//	)
func NewPaginationFeature(opts ...PaginationOption) *PaginationFeature {
	pf := &PaginationFeature{
		pageSize:    defaultPageSize,
		maxPageSize: defaultPageSize,
		maxOffset:   -1,
	}

	for _, opt := range opts {
		opt(pf)
	}

	return pf
}

// Process applies pagination to the query builder and processes the result.
//
// It extracts pagination parameters from the request, applies them to the query,
// and adds pagination information to the result.
//
// Example:
//
//	// Use the pagination feature in a feature chain
//	result, err := paginationFeature.Process(builder, nextFeature)
func (pf *PaginationFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	pf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return pf.handle(builder.Request(), r)
}

// build applies pagination settings to the query builder.
func (pf *PaginationFeature) build(builder *reveald.QueryBuilder) {
	offset, err := toValue(builder.Request(), "offset")
	if err != nil || offset < 0 || (pf.maxOffset > 0 && offset > pf.maxOffset) {
		offset = 0
	}

	pageSize, err := toValue(builder.Request(), "size")
	if err != nil || pageSize < 0 || pageSize > pf.maxPageSize {
		pageSize = pf.pageSize
	}

	builder.SetSize(pageSize)
	builder.SetFrom(offset)
}

// handle adds pagination information to the result.
func (pf *PaginationFeature) handle(req *reveald.Request, result *reveald.Result) (*reveald.Result, error) {
	offset, err := toValue(req, "offset")
	if err != nil || offset < 0 || (pf.maxOffset > 0 && offset > pf.maxOffset) {
		offset = 0
	}

	pageSize, err := toValue(req, "size")
	if err != nil || pageSize < 0 || pageSize > pf.maxPageSize {
		pageSize = pf.pageSize
	}

	result.Pagination = &reveald.ResultPagination{
		Offset:   offset,
		PageSize: pageSize,
	}
	return result, nil
}

// toValue converts a request parameter to an integer value.
func toValue(req *reveald.Request, param string) (int, error) {
	p, err := req.Get(param)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(p.Value())
}

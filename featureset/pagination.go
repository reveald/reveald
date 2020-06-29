package featureset

import (
	"strconv"

	"github.com/reveald/reveald"
)

const (
	defaultPageSize int = 24
)

type PaginationFeature struct {
	pageSize    int
	maxPageSize int
	maxOffset   int
}

type PaginationOption func(*PaginationFeature)

func WithPageSize(pageSize int) PaginationOption {
	return func(pf *PaginationFeature) {
		pf.pageSize = pageSize
	}
}

func WithMaxPageSize(maxPageSize int) PaginationOption {
	return func(pf *PaginationFeature) {
		pf.maxPageSize = maxPageSize
	}
}

func WithMaxOffset(maxOffset int) PaginationOption {
	return func(pf *PaginationFeature) {
		pf.maxOffset = maxOffset
	}
}

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

func (pf *PaginationFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	pf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return pf.handle(builder.Request(), r)
}

func (pf *PaginationFeature) build(builder *reveald.QueryBuilder) {
	offset, err := toValue(builder.Request(), "offset")
	if err != nil || offset < 0 || (pf.maxOffset > 0 && offset > pf.maxOffset) {
		offset = 0
	}

	pageSize, err := toValue(builder.Request(), "size")
	if err != nil || pageSize < 0 || pageSize > pf.maxPageSize {
		pageSize = pf.pageSize
	}

	builder.
		Selection().
		Update(
			reveald.WithPageSize(pageSize),
			reveald.WithOffset(offset))
}

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

func toValue(req *reveald.Request, param string) (int, error) {
	p, err := req.Get(param)
	if err != nil {
		return -1, err
	}

	return strconv.Atoi(p.Value())
}

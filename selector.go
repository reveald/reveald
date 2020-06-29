package reveald

import "github.com/olivere/elastic/v7"

// DocumentSelector is a container for Elasticsearch
// data, which is not part of a document search - it
// includes information about page sizes, sorting, and
// field inclusion/exclusion
type DocumentSelector struct {
	inclusions []string
	exclusions []string
	offset     int
	pageSize   int
	sort       *elastic.FieldSort
}

const (
	defaultPageSize = 24
)

// Selector is a functional option used when
// creating a DocumentSelector
type Selector func(*DocumentSelector)

// WithProperties defines a set of document fields
// to include on a document result
func WithProperties(properties ...string) Selector {
	return func(s *DocumentSelector) {
		s.inclusions = append(s.inclusions, properties...)
	}
}

// WithoutProperties defines a set of document fields
// to exclude from a document result
func WithoutProperties(properties ...string) Selector {
	return func(s *DocumentSelector) {
		s.exclusions = append(s.exclusions, properties...)
	}
}

// WithPageSize defines a page size for a search
func WithPageSize(size int) Selector {
	return func(s *DocumentSelector) {
		s.pageSize = size
	}
}

// WithOffset defines which offset to use for a search
func WithOffset(offset int) Selector {
	return func(s *DocumentSelector) {
		s.offset = offset
	}
}

// WithSort defines a sort for a search result
func WithSort(sort *elastic.FieldSort) Selector {
	return func(s *DocumentSelector) {
		s.sort = sort
	}
}

// NewDocumentSelector specifies a default selection
// for pagination, sort, and field exclusion
func NewDocumentSelector(selectors ...Selector) *DocumentSelector {
	ds := &DocumentSelector{
		offset:   0,
		pageSize: defaultPageSize,
		sort:     nil,
	}

	for _, selector := range selectors {
		selector(ds)
	}

	return ds
}

// Update a DocumentSelector with new settings
func (ds *DocumentSelector) Update(selectors ...Selector) {
	for _, selector := range selectors {
		selector(ds)
	}
}

// Sort returns the current sort for a search request
func (ds *DocumentSelector) Sort() *elastic.FieldSort {
	return ds.sort
}

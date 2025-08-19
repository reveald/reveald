package reveald

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
)

// DocumentSelector is a container for Elasticsearch data, which is not part of a document search.
//
// It includes information about page sizes, sorting, and field inclusion/exclusion.
// The DocumentSelector is typically used with a QueryBuilder to control pagination,
// sorting, and field selection.
type DocumentSelector struct {
	inclusions []string
	exclusions []string
	offset     int
	pageSize   int
	sort       []types.SortCombinations
}

const (
	defaultPageSize = 24
)

// Selector is a functional option used when creating or updating a DocumentSelector.
//
// This allows for a flexible and readable way to configure document selection options.
type Selector func(*DocumentSelector)

// WithProperties defines a set of document fields to include in a document result.
//
// This is equivalent to the "_source.includes" parameter in Elasticsearch.
//
// Example:
//
//	// Include only id, name, and price fields
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithProperties("id", "name", "price"),
//	)
func WithProperties(properties ...string) Selector {
	return func(s *DocumentSelector) {
		s.inclusions = append(s.inclusions, properties...)
	}
}

// WithoutProperties defines a set of document fields to exclude from a document result.
//
// This is equivalent to the "_source.excludes" parameter in Elasticsearch.
//
// Example:
//
//	// Exclude description and metadata fields
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithoutProperties("description", "metadata"),
//	)
func WithoutProperties(properties ...string) Selector {
	return func(s *DocumentSelector) {
		s.exclusions = append(s.exclusions, properties...)
	}
}

// WithPageSize defines a page size for a search.
//
// This controls how many documents are returned per page.
//
// Example:
//
//	// Return 10 documents per page
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithPageSize(10),
//	)
func WithPageSize(size int) Selector {
	return func(s *DocumentSelector) {
		s.pageSize = size
	}
}

// WithOffset defines the offset (starting document) for a search.
//
// This is used for pagination, to skip a certain number of documents.
//
// Example:
//
//	// Skip the first 20 documents (for page 3 with page size 10)
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithPageSize(10),
//	    reveald.WithOffset(20),
//	)
func WithOffset(offset int) Selector {
	return func(s *DocumentSelector) {
		s.offset = offset
	}
}

// WithSort defines a sort field and order for a search.
//
// This controls the order in which documents are returned.
//
// Example:
//
//	// Sort by price in descending order
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithSort("price", "desc"),
//	)
//
//	// Sort by name in ascending order
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithSort("name", "asc"),
//	)
func WithSort(field string, order sortorder.SortOrder) Selector {
	return func(s *DocumentSelector) {
		s.sort = append(s.sort, types.SortOptions{
			SortOptions: map[string]types.FieldSort{
				field: {
					Order: &order,
				},
			},
		})
	}
}

// NewDocumentSelector returns a new document selector instance.
//
// It is typically used with a query builder to specify which fields to include and exclude,
// as well as pagination settings.
//
// Example:
//
//	// Create a document selector with multiple options
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithPageSize(10),
//	    reveald.WithOffset(20),
//	    reveald.WithSort("price", "desc"),
//	    reveald.WithProperties("id", "name", "price"),
//	)
func NewDocumentSelector(selectors ...Selector) *DocumentSelector {
	s := &DocumentSelector{
		inclusions: []string{},
		exclusions: []string{},
		offset:     0,
		pageSize:   defaultPageSize,
		sort:       nil,
	}

	for _, sel := range selectors {
		sel(s)
	}

	return s
}

// Update a DocumentSelector with new settings.
//
// This allows you to modify an existing DocumentSelector with new options.
//
// Example:
//
//	// Create a basic selector
//	selector := reveald.NewDocumentSelector(
//	    reveald.WithPageSize(10),
//	)
//
//	// Later, update it with additional options
//	selector.Update(
//	    reveald.WithOffset(20),
//	    reveald.WithSort("price", "desc"),
//	)
func (ds *DocumentSelector) Update(selectors ...Selector) {
	for _, selector := range selectors {
		selector(ds)
	}
}

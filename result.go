package reveald

import (
	"time"

	"github.com/olivere/elastic/v7"
)

// Result is a construct containing the search result,
// Elasticsearch aggregations, and meta data
type Result struct {
	result        *elastic.SearchResult
	request       *Request
	TotalHitCount int64
	Hits          []map[string]interface{}
	Aggregations  map[string][]*ResultBucket
	Pagination    *ResultPagination
	Sorting       *ResultSorting
	Duration      time.Duration
}

// RawResult returns the raw Elasticsearch response
func (r *Result) RawResult() *elastic.SearchResult {
	return r.result
}

// Request returns the executed request
func (r *Result) Request() *Request {
	return r.request
}

// ResultBucket is a container for aggregations
type ResultBucket struct {
	Value            interface{}
	HitCount         int64
	SubResultBuckets map[string][]*ResultBucket
}

// ResultPagination is a container for pagination
// information, such as current offset and which
// page size the result has
type ResultPagination struct {
	Offset   int
	PageSize int
}

// ResultSorting is a container for sort options
// available for the request
type ResultSorting struct {
	Param   string
	Options []*ResultSortingOption
}

// ResultSortingOption defines a possible
// value to sort a result set on
type ResultSortingOption struct {
	Name      string
	Property  string
	Ascending bool
	Selected  bool
}

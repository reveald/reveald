package reveald

import (
	"time"
)

// Result is a construct containing the search result, Elasticsearch aggregations, and metadata.
//
// It encapsulates all the information returned from an Elasticsearch query, including
// the matching documents, aggregation results, pagination information, and query duration.
//
// Example:
//
//	// Execute a query and get results
//	result, err := backend.Execute(ctx, builder)
//	if err != nil {
//	    // Handle error
//	}
//
//	// Access the results
//	fmt.Printf("Found %d documents\n", result.TotalHitCount)
//	for _, hit := range result.Hits {
//	    fmt.Printf("Document ID: %s, Name: %s\n", hit["id"], hit["name"])
//	}
//
//	// Access aggregations
//	if categories, ok := result.Aggregations["categories"]; ok {
//	    for _, bucket := range categories {
//	        fmt.Printf("Category: %v, Count: %d\n", bucket.Value, bucket.HitCount)
//	    }
//	}
type Result struct {
	request       *Request
	TotalHitCount int64
	Hits          []map[string]any
	Aggregations  map[string][]*ResultBucket
	Pagination    *ResultPagination
	Sorting       *ResultSorting
	Duration      time.Duration
}

// Request returns the executed request that produced this result.
//
// This can be useful for accessing the original request parameters.
//
// Example:
//
//	// Get the original request
//	request := result.Request()
//	if request != nil && request.Has("category") {
//	    // Use the category parameter
//	    param, _ := request.Get("category")
//	    fmt.Printf("Filtered by category: %s\n", param.Value())
//	}
func (r *Result) Request() *Request {
	return r.request
}

// ResultBucket is a container for aggregations.
//
// It represents a single bucket in an Elasticsearch aggregation result,
// containing the bucket value, document count, and any sub-aggregations.
//
// Example:
//
//	// Access aggregation buckets
//	for _, bucket := range result.Aggregations["categories"] {
//	    fmt.Printf("Category: %v, Count: %d\n", bucket.Value, bucket.HitCount)
//
//	    // Access sub-aggregations if any
//	    if subBuckets, ok := bucket.SubResultBuckets["brands"]; ok {
//	        for _, subBucket := range subBuckets {
//	            fmt.Printf("  Brand: %v, Count: %d\n", subBucket.Value, subBucket.HitCount)
//	        }
//	    }
//	}
type ResultBucket struct {
	Value            any
	HitCount         int64
	SubResultBuckets map[string][]*ResultBucket
}

// ResultPagination is a container for pagination information.
//
// It includes the current offset and page size used for the query.
//
// Example:
//
//	// Access pagination information
//	if result.Pagination != nil {
//	    fmt.Printf("Page size: %d, Offset: %d\n",
//	        result.Pagination.PageSize,
//	        result.Pagination.Offset)
//
//	    // Calculate current page (1-based)
//	    currentPage := (result.Pagination.Offset / result.Pagination.PageSize) + 1
//	    fmt.Printf("Current page: %d\n", currentPage)
//	}
type ResultPagination struct {
	Offset   int
	PageSize int
}

// ResultSorting is a container for sort options available for the request.
//
// It includes the sort parameter and available sort options.
//
// Example:
//
//	// Access sorting information
//	if result.Sorting != nil {
//	    fmt.Printf("Sorted by: %s\n", result.Sorting.Param)
//
//	    // List available sort options
//	    fmt.Println("Available sort options:")
//	    for _, option := range result.Sorting.Options {
//	        fmt.Printf("  %s (%s)\n", option.Label, option.Value)
//	    }
//	}
type ResultSorting struct {
	Param   string
	Options []*ResultSortingOption
}

// ResultSortingOption is a container for a sort option.
//
// It represents a single sort option with a label and value.
//
// Example:
//
//	// Create a custom sort option
//	sortOption := &reveald.ResultSortingOption{
//	    Label: "Price (High to Low)",
//	    Value: "price:desc",
//	}
type ResultSortingOption struct {
	Label string
	Value string
}

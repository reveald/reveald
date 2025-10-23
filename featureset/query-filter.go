package featureset

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
)

// QueryFilterFeature adds a query string filter based on a request parameter.
//
// It creates a query_string query for full-text search across specified fields.
// This is useful for implementing search box functionality.
//
// Example:
//
//	// Create a query filter for the "q" parameter that searches across all fields
//	queryFilter := featureset.NewQueryFilterFeature()
//
//	// Use the query filter in a feature chain
//	result, err := queryFilter.Process(builder, nextFeature)
type QueryFilterFeature struct {
	name   string
	fields []string
}

// QueryFilterOption is a functional option for configuring a QueryFilterFeature.
type QueryFilterOption func(*QueryFilterFeature)

// WithQueryParam sets the request parameter name to use for the query string.
//
// Example:
//
//	// Create a query filter that uses the "search" parameter instead of the default "q"
//	queryFilter := featureset.NewQueryFilterFeature(
//	    featureset.WithQueryParam("search"),
//	)
func WithQueryParam(name string) QueryFilterOption {
	return func(qff *QueryFilterFeature) {
		qff.name = name
	}
}

// WithFields specifies which fields to search in.
//
// If not specified, the query will search across all fields.
//
// Example:
//
//	// Create a query filter that searches only in title and description fields
//	queryFilter := featureset.NewQueryFilterFeature(
//	    featureset.WithFields("title", "description"),
//	)
func WithFields(fields ...string) QueryFilterOption {
	return func(qff *QueryFilterFeature) {
		qff.fields = fields
	}
}

// NewQueryFilterFeature creates a new query filter feature with the specified options.
//
// By default, it uses the "q" parameter and searches across all fields.
//
// Example:
//
//	// Create a query filter with custom settings
//	queryFilter := featureset.NewQueryFilterFeature(
//	    featureset.WithQueryParam("search"),
//	    featureset.WithFields("title", "description", "tags"),
//	)
func NewQueryFilterFeature(opts ...QueryFilterOption) *QueryFilterFeature {
	qff := &QueryFilterFeature{
		name:   "q",
		fields: []string{},
	}

	for _, opt := range opts {
		opt(qff)
	}

	return qff
}

// Process applies the query string filter to the query builder and processes the result.
//
// It extracts the query parameter from the request and adds a query_string query
// to the query builder if the parameter exists.
//
// Example:
//
//	// Use the query filter in a feature chain
//	result, err := queryFilter.Process(builder, nextFeature)
func (qff *QueryFilterFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	if !builder.Request().Has(qff.name) {
		return next(builder)
	}

	v, err := builder.Request().Get(qff.name)
	if err != nil || v.Value() == "" {
		return next(builder)
	}

	// Create query string query directly with typed objects
	lenient := true
	queryStringQuery := types.Query{
		QueryString: &types.QueryStringQuery{
			Query:   v.Value(),
			Lenient: &lenient,
		},
	}

	// Add fields if specified
	if len(qff.fields) > 0 {
		queryStringQuery.QueryString.Fields = qff.fields
	}

	builder.With(queryStringQuery)
	return next(builder)
}

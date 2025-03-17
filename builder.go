package reveald

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
)

/*
NOTE: Migration to Official Elasticsearch Types
------------------------------------------------
This file contains the implementation of QueryBuilder using the official Elasticsearch Go client types.
This implementation provides type safety and better IDE support by using the strongly typed API.

While retaining compatibility with the legacy any based approach for backward compatibility.
*/

// QueryBuilder is a construct to build a
// dynamic Elasticsearch query
type QueryBuilder struct {
	request        *Request
	indices        []string
	boolQuery      *types.BoolQuery
	aggregations   map[string]types.Aggregations
	sorts          []types.SortOptions
	size           *int
	from           *int
	sourceIncludes []string
	sourceExcludes []string
	scriptFields   map[string]types.ScriptField
	runtimeFields  map[string]types.RuntimeField
	docValueFields []types.FieldAndFormat
	postFilter     *types.Query
}

// NewQueryBuilder returns a new base query for a set of indices.
//
// It initializes a QueryBuilder with the provided request and indices,
// setting up empty collections for various query components like bool queries,
// aggregations, sorts, etc.
//
// Example:
//
//	// Create a basic query builder for the "products" index
//	builder := reveald.NewQueryBuilder(nil, "products")
//
//	// Create a query builder with a request and multiple indices
//	request := reveald.NewRequest()
//	builder := reveald.NewQueryBuilder(request, "products", "categories")
func NewQueryBuilder(r *Request, indices ...string) *QueryBuilder {
	return &QueryBuilder{
		request:        r,
		indices:        indices,
		boolQuery:      &types.BoolQuery{},
		aggregations:   make(map[string]types.Aggregations),
		sorts:          []types.SortOptions{},
		sourceIncludes: []string{},
		sourceExcludes: []string{},
		scriptFields:   make(map[string]types.ScriptField),
		runtimeFields:  make(map[string]types.RuntimeField),
		docValueFields: []types.FieldAndFormat{},
	}
}

// Request returns the current Request instance associated with this QueryBuilder.
//
// The Request contains parameters that can be used for filtering documents.
//
// Example:
//
//	request := builder.Request()
//	if request != nil && request.Has("category") {
//	    // Use the category parameter
//	}
func (qb *QueryBuilder) Request() *Request {
	return qb.request
}

// Indices returns the target indices for the Elasticsearch query.
//
// Example:
//
//	indices := builder.Indices()
//	fmt.Printf("Searching in indices: %v\n", indices)
func (qb *QueryBuilder) Indices() []string {
	return qb.indices
}

// SetIndices changes the query builder indices to be used
// while searching in Elasticsearch.
//
// Example:
//
//	// Update indices to search in
//	builder.SetIndices("products", "categories")
func (qb *QueryBuilder) SetIndices(indices ...string) {
	qb.indices = indices
}

// With adds a "must" clause to the bool query, filtering documents
// that must match the specified query.
//
// This is equivalent to using a "must" clause in an Elasticsearch bool query.
//
// Example:
//
//	// Find documents where the "active" field is true
//	builder.With(types.Query{
//	    Term: map[string]types.TermQuery{
//	        "active": {Value: true},
//	    },
//	})
//
//	// Find documents matching a specific text
//	builder.With(types.Query{
//	    Match: map[string]types.MatchQuery{
//	        "description": {Query: "premium product"},
//	    },
//	})
func (qb *QueryBuilder) With(query types.Query) {
	if qb.boolQuery.Must == nil {
		qb.boolQuery.Must = []types.Query{}
	}
	qb.boolQuery.Must = append(qb.boolQuery.Must, query)
}

// Without adds a "must_not" clause to the bool query, filtering out documents
// that match the specified query.
//
// This is equivalent to using a "must_not" clause in an Elasticsearch bool query.
//
// Example:
//
//	// Exclude documents where "out_of_stock" is true
//	builder.Without(types.Query{
//	    Term: map[string]types.TermQuery{
//	        "out_of_stock": {Value: true},
//	    },
//	})
//
//	// Exclude documents in a specific price range
//	maxPrice := types.Float64(100)
//	builder.Without(types.Query{
//	    Range: map[string]types.RangeQuery{
//	        "price": &types.NumberRangeQuery{
//	            Gt: &maxPrice,
//	        },
//	    },
//	})
func (qb *QueryBuilder) Without(query types.Query) {
	if qb.boolQuery.MustNot == nil {
		qb.boolQuery.MustNot = []types.Query{}
	}
	qb.boolQuery.MustNot = append(qb.boolQuery.MustNot, query)
}

// Boost adds a "should" clause to the bool query, which increases the relevance score
// of documents that match the specified query, but doesn't require them to match.
//
// This is equivalent to using a "should" clause in an Elasticsearch bool query.
//
// Example:
//
//	// Boost documents with the "premium" tag
//	builder.Boost(types.Query{
//	    Term: map[string]types.TermQuery{
//	        "tags": {Value: "premium"},
//	    },
//	})
//
//	// Boost documents with a high rating
//	minRating := types.Float64(4.5)
//	builder.Boost(types.Query{
//	    Range: map[string]types.RangeQuery{
//	        "rating": &types.NumberRangeQuery{
//	            Gte: &minRating,
//	        },
//	    },
//	})
func (qb *QueryBuilder) Boost(query types.Query) {
	if qb.boolQuery.Should == nil {
		qb.boolQuery.Should = []types.Query{}
	}
	qb.boolQuery.Should = append(qb.boolQuery.Should, query)
}

// PostFilterWith adds a "must" clause to the post_filter query, which filters documents
// after aggregations have been calculated.
//
// Post filters are useful when you want to apply filters that shouldn't affect aggregations.
//
// Example:
//
//	// Filter documents by category after calculating aggregations
//	builder.PostFilterWith(types.Query{
//	    Term: map[string]types.TermQuery{
//	        "category": {Value: "electronics"},
//	    },
//	})
func (qb *QueryBuilder) PostFilterWith(query types.Query) {
	if qb.postFilter == nil {
		qb.postFilter = &types.Query{
			Bool: &types.BoolQuery{},
		}
	}
	if qb.postFilter.Bool.Must == nil {
		qb.postFilter.Bool.Must = []types.Query{}
	}
	qb.postFilter.Bool.Must = append(qb.postFilter.Bool.Must, query)
}

// PostFilterWithout adds a "must_not" clause to the post_filter query, which excludes documents
// after aggregations have been calculated.
//
// This is useful when you want to exclude certain documents from the results without
// affecting the aggregation calculations.
//
// Example:
//
//	// Exclude out-of-stock products after calculating aggregations
//	builder.PostFilterWithout(types.Query{
//	    Term: map[string]types.TermQuery{
//	        "in_stock": {Value: false},
//	    },
//	})
func (qb *QueryBuilder) PostFilterWithout(query types.Query) {
	if qb.postFilter == nil {
		qb.postFilter = &types.Query{
			Bool: &types.BoolQuery{},
		}
	}
	if qb.postFilter.Bool.MustNot == nil {
		qb.postFilter.Bool.MustNot = []types.Query{}
	}
	qb.postFilter.Bool.MustNot = append(qb.postFilter.Bool.MustNot, query)
}

// PostFilterBoost adds a "should" clause to the post_filter query, which boosts the relevance
// of documents that match the specified query after aggregations have been calculated.
//
// Example:
//
//	// Boost featured products after calculating aggregations
//	builder.PostFilterBoost(types.Query{
//	    Term: map[string]types.TermQuery{
//	        "featured": {Value: true},
//	    },
//	})
func (qb *QueryBuilder) PostFilterBoost(query types.Query) {
	if qb.postFilter == nil {
		qb.postFilter = &types.Query{
			Bool: &types.BoolQuery{},
		}
	}
	if qb.postFilter.Bool.Should == nil {
		qb.postFilter.Bool.Should = []types.Query{}
	}
	qb.postFilter.Bool.Should = append(qb.postFilter.Bool.Should, query)
}

// Selection returns a DocumentSelector for this query builder.
//
// The DocumentSelector allows you to specify which fields to include/exclude,
// pagination settings, and sorting options.
//
// Example:
//
//	// Configure pagination and sorting
//	builder.Selection().Update(
//	    reveald.WithPageSize(10),
//	    reveald.WithOffset(20),
//	    reveald.WithSort("price", "desc"),
//	)
//
//	// Include only specific fields
//	builder.Selection().Update(
//	    reveald.WithProperties("id", "name", "price", "category"),
//	)
func (qb *QueryBuilder) Selection() *DocumentSelector {
	// Create a DocumentSelector if none exists yet
	// This allows the user to retrieve current selection settings
	// and also to chain methods on the DocumentSelector
	if qb.size == nil {
		defaultSize := 24
		qb.size = &defaultSize
	}
	if qb.from == nil {
		defaultFrom := 0
		qb.from = &defaultFrom
	}

	// Create a selector from existing values
	selector := &DocumentSelector{
		inclusions: qb.sourceIncludes,
		exclusions: qb.sourceExcludes,
		offset:     *qb.from,
		pageSize:   *qb.size,
	}

	// Set current sort if available
	if len(qb.sorts) > 0 {
		// Convert the sort options to the format expected by DocumentSelector
		// This is a simplification - we only handle the first sort option
		sortData, _ := json.Marshal(qb.sorts[0])
		_ = json.Unmarshal(sortData, &selector.sort)
	}

	return selector
}

// Aggregation adds an aggregation to the query.
//
// Aggregations allow you to group and extract statistics from your data.
//
// Example:
//
//	// Add a terms aggregation on the "category" field
//	fieldName := "category"
//	size := 10
//	termsAgg := types.Aggregations{
//	    Terms: &types.TermsAggregation{
//	        Field: &fieldName,
//	        Size:  &size,
//	    },
//	}
//	builder.Aggregation("categories", termsAgg)
//
//	// Add a range aggregation on the "price" field
//	builder.Aggregation("price_ranges", types.Aggregations{
//	    Range: &types.RangeAggregation{
//	        Field: ptr.String("price"),
//	        Ranges: []types.NamedRange{
//	            {Key: ptr.String("cheap"), From: ptr.Float64(0), To: ptr.Float64(50)},
//	            {Key: ptr.String("medium"), From: ptr.Float64(50), To: ptr.Float64(100)},
//	            {Key: ptr.String("expensive"), From: ptr.Float64(100)},
//	        },
//	    },
//	})
func (qb *QueryBuilder) Aggregation(name string, agg types.Aggregations) {
	qb.aggregations[name] = agg
}

// WithTermQuery is a convenience method to add a term query to the "must" clause.
//
// A term query finds documents that contain the exact term specified in the provided field.
//
// Example:
//
//	// Find active products
//	builder.WithTermQuery("active", true)
//
//	// Find products in a specific category
//	builder.WithTermQuery("category", "electronics")
func (qb *QueryBuilder) WithTermQuery(field string, value any) {
	termQuery := types.Query{
		Term: map[string]types.TermQuery{
			field: {Value: value},
		},
	}
	qb.With(termQuery)
}

// WithMatchQuery is a convenience method to add a match query to the "must" clause.
//
// A match query is a standard query for performing full-text search, including
// options like fuzziness, operator, etc.
//
// Example:
//
//	// Find products with "premium" in their description
//	builder.WithMatchQuery("description", "premium")
//
//	// Find products with "wireless headphones" in their name
//	builder.WithMatchQuery("name", "wireless headphones")
func (qb *QueryBuilder) WithMatchQuery(field string, value any) {
	// For string values
	if stringValue, ok := value.(string); ok {
		matchQuery := types.Query{
			Match: map[string]types.MatchQuery{
				field: {Query: stringValue},
			},
		}
		qb.With(matchQuery)
		return
	}

	// For non-string values, use term query
	qb.WithTermQuery(field, value)
}

// WithRangeQuery is a convenience method to add a range query to the "must" clause.
//
// A range query finds documents with field values within the specified range.
//
// Parameters:
//   - field: The field to query
//   - gt: Greater than value (exclusive)
//   - gte: Greater than or equal to value (inclusive)
//   - lt: Less than value (exclusive)
//   - lte: Less than or equal to value (inclusive)
//
// Example:
//
//	// Find products with price >= 50 and < 100
//	builder.WithRangeQuery("price", nil, 50, 100, nil)
//
//	// Find products with rating > 4
//	builder.WithRangeQuery("rating", 4, nil, nil, nil)
//
//	// Find products created before 2023
//	builder.WithRangeQuery("created_at", nil, nil, nil, "2023-01-01")
func (qb *QueryBuilder) WithRangeQuery(field string, gt, gte, lt, lte any) {
	// Convert any values to *types.Float64 if they're not nil
	var gtFloat, gteFloat, ltFloat, lteFloat *types.Float64

	if gt != nil {
		if f, ok := gt.(float64); ok {
			val := types.Float64(f)
			gtFloat = &val
		}
	}

	if gte != nil {
		if f, ok := gte.(float64); ok {
			val := types.Float64(f)
			gteFloat = &val
		}
	}

	if lt != nil {
		if f, ok := lt.(float64); ok {
			val := types.Float64(f)
			ltFloat = &val
		}
	}

	if lte != nil {
		if f, ok := lte.(float64); ok {
			val := types.Float64(f)
			lteFloat = &val
		}
	}

	rangeQuery := types.Query{
		Range: map[string]types.RangeQuery{
			field: &types.NumberRangeQuery{
				Gt:  gtFloat,
				Gte: gteFloat,
				Lt:  ltFloat,
				Lte: lteFloat,
			},
		},
	}
	qb.With(rangeQuery)
}

// RawQuery returns the underlying bool query.
//
// This can be useful when you need to access or modify the raw query structure.
//
// Example:
//
//	// Get the raw query to inspect or modify it
//	rawQuery := builder.RawQuery()
//	fmt.Printf("Current query: %+v\n", rawQuery)
func (qb *QueryBuilder) RawQuery() *types.Query {
	return &types.Query{
		Bool: qb.boolQuery,
	}
}

// WithScriptedField adds a scripted field to the query.
//
// Scripted fields compute values on-the-fly during query execution.
//
// Example:
//
//	// Add a scripted field that calculates discounted price
//	builder.WithScriptedField("discounted_price", &types.Script{
//	    Source: "doc['price'].value * (1 - doc['discount'].value)",
//	})
func (qb *QueryBuilder) WithScriptedField(field string, script *types.Script) {
	qb.scriptFields[field] = types.ScriptField{
		Script: *script,
	}
}

// WithRuntimeMappings adds runtime fields to the query.
//
// Runtime fields are defined at query time and can be used for calculations
// without modifying the index mapping.
//
// Example:
//
//	// Add a runtime field that calculates the full name
//	builder.WithRuntimeMappings(map[string]types.RuntimeField{
//	    "full_name": {
//	        Type: "keyword",
//	        Script: &types.Script{
//	            Source: "emit(doc['first_name'].value + ' ' + doc['last_name'].value)",
//	        },
//	    },
//	})
func (qb *QueryBuilder) WithRuntimeMappings(runtimeMappings map[string]types.RuntimeField) {
	maps.Copy(qb.runtimeFields, runtimeMappings)
}

// DocvalueFields specifies which fields to return as doc_value_fields in the response.
//
// Doc value fields are returned as-is without analysis or parsing.
//
// Example:
//
//	// Return price and stock fields as doc value fields
//	builder.DocvalueFields("price", "stock")
func (qb *QueryBuilder) DocvalueFields(docvalueFields ...string) {
	for _, field := range docvalueFields {
		qb.docValueFields = append(qb.docValueFields, types.FieldAndFormat{
			Field: field,
		})
	}
}

// AddTermsAggregation is a convenience method to add a terms aggregation.
//
// Terms aggregations group documents by terms found in a specific field.
//
// Parameters:
//   - name: The name of the aggregation
//   - field: The field to aggregate on
//   - size: The maximum number of buckets to return
//
// Example:
//
//	// Group products by category, returning the top 10 categories
//	builder.AddTermsAggregation("categories", "category", 10)
//
//	// Group products by brand, returning the top 5 brands
//	builder.AddTermsAggregation("brands", "brand.keyword", 5)
func (qb *QueryBuilder) AddTermsAggregation(name string, field string, size int) {
	fieldCopy := field // Create a copy to get address of
	sizeCopy := size   // Create a copy to get address of

	agg := types.Aggregations{
		Terms: &types.TermsAggregation{
			Field: &fieldCopy,
			Size:  &sizeCopy,
		},
	}
	qb.Aggregation(name, agg)
}

// Sort adds a sort option to the query.
//
// Documents will be sorted according to the specified field and order.
//
// Example:
//
//	// Sort by price in descending order
//	builder.Sort("price", sortorder.Desc)
//
//	// Sort by name in ascending order
//	builder.Sort("name.keyword", sortorder.Asc)
//
//	// Sort by multiple fields
//	builder.Sort("price", sortorder.Desc)
//	builder.Sort("rating", sortorder.Desc)
func (qb *QueryBuilder) Sort(field string, order sortorder.SortOrder) {
	orderCopy := order // Create a copy to get address of

	sort := types.SortOptions{
		SortOptions: map[string]types.FieldSort{
			field: {Order: &orderCopy},
		},
	}
	qb.sorts = append(qb.sorts, sort)
}

// SetSize sets the maximum number of documents to return.
//
// This is equivalent to the "size" parameter in Elasticsearch.
//
// Example:
//
//	// Return at most 10 documents
//	builder.SetSize(10)
func (qb *QueryBuilder) SetSize(size int) {
	qb.size = &size
}

// SetFrom sets the number of documents to skip.
//
// This is equivalent to the "from" parameter in Elasticsearch and is used for pagination.
//
// Example:
//
//	// Skip the first 20 documents (for page 3 with page size 10)
//	builder.SetFrom(20)
func (qb *QueryBuilder) SetFrom(from int) {
	qb.from = &from
}

// IncludeFields specifies which fields to include in the response.
//
// This is equivalent to the "_source.includes" parameter in Elasticsearch.
//
// Example:
//
//	// Only include id, name, and price fields
//	builder.IncludeFields("id", "name", "price")
func (qb *QueryBuilder) IncludeFields(fields ...string) {
	qb.sourceIncludes = append(qb.sourceIncludes, fields...)
}

// ExcludeFields specifies which fields to exclude from the response.
//
// This is equivalent to the "_source.excludes" parameter in Elasticsearch.
//
// Example:
//
//	// Exclude description and metadata fields
//	builder.ExcludeFields("description", "metadata")
func (qb *QueryBuilder) ExcludeFields(fields ...string) {
	qb.sourceExcludes = append(qb.sourceExcludes, fields...)
}

// Build constructs the Elasticsearch query as a map.
//
// This is primarily used internally and for backward compatibility.
//
// Example:
//
//	// Build the query as a map
//	queryMap := builder.Build()
//	jsonBytes, _ := json.Marshal(queryMap)
//	fmt.Println(string(jsonBytes))
func (qb *QueryBuilder) Build() map[string]any {
	// First create a properly typed search request
	request := qb.BuildRequest()

	// Then convert it to map[string]any for the current backend
	data, _ := json.Marshal(request)
	var result map[string]any
	_ = json.Unmarshal(data, &result)

	return result
}

// BuildRequest constructs an Elasticsearch search request using the official client types.
//
// This is used internally by the Execute method.
//
// Example:
//
//	// Build a search request
//	request := builder.BuildRequest()
//
//	// Inspect the request
//	fmt.Printf("Searching in indices: %v\n", request.Index)
//	fmt.Printf("Query: %+v\n", request.Query)
func (qb *QueryBuilder) BuildRequest() *search.Request {
	// Create the search request
	request := &search.Request{}

	// Add query if we have any conditions
	if len(qb.boolQuery.Must) > 0 || len(qb.boolQuery.MustNot) > 0 || len(qb.boolQuery.Should) > 0 {
		request.Query = &types.Query{
			Bool: qb.boolQuery,
		}
	}

	// Add post filter if specified
	if qb.postFilter != nil {
		request.PostFilter = qb.postFilter
	}

	// Add aggregations if we have any
	if len(qb.aggregations) > 0 {
		request.Aggregations = qb.aggregations
	}

	// Add sort if we have any
	for _, sort := range qb.sorts {
		request.Sort = append(request.Sort, sort)
	}

	// Add size and from if set
	if qb.size != nil {
		request.Size = qb.size
	}

	if qb.from != nil {
		request.From = qb.from
	}

	// Add source filtering if we have includes or excludes
	if len(qb.sourceIncludes) > 0 || len(qb.sourceExcludes) > 0 {
		request.Source_ = types.SourceFilter{
			Excludes: qb.sourceExcludes,
			Includes: qb.sourceIncludes,
		}
	}

	// Add runtime fields if we have any
	if len(qb.runtimeFields) > 0 {
		request.RuntimeMappings = qb.runtimeFields
	}

	// Add script fields if we have any
	if len(qb.scriptFields) > 0 {
		request.ScriptFields = qb.scriptFields
	}

	// Add doc value fields if we have any
	if len(qb.docValueFields) > 0 {
		request.DocvalueFields = qb.docValueFields
	}

	return request
}

// Execute runs the query against the provided Elasticsearch backend and returns the results.
//
// This is a convenience method that combines BuildRequest and backend.Execute.
//
// Example:
//
//	// Execute the query and get results
//	result, err := builder.Execute(ctx, backend)
//	if err != nil {
//	    // Handle error
//	}
//
//	// Process the results
//	fmt.Printf("Found %d documents\n", result.TotalHitCount)
//	for _, hit := range result.Hits {
//	    fmt.Printf("Document: %v\n", hit)
//	}
func (qb *QueryBuilder) Execute(ctx context.Context, backend *ElasticBackend) (*Result, error) {
	return backend.Execute(ctx, qb)
}

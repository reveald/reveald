# reveald

A high-level, fluent interface for building Elasticsearch queries in Go, built on top of the official Elasticsearch Go client.

## Overview

Reveald is a Go library that simplifies working with Elasticsearch by providing a clean, fluent API for building and executing queries. It wraps the official [Elasticsearch Go client](https://github.com/elastic/go-elasticsearch) and offers a more intuitive interface for common Elasticsearch operations.

Key features:
- Fluent query building interface
- Support for complex boolean queries
- Aggregation handling
- Sorting and pagination
- Document selection and filtering
- Type safety with official Elasticsearch types
- Strong integration test suite

## Installation

```bash
go get github.com/reveald/reveald/v1
```

Requirements:
- Go 1.20 or higher
- Elasticsearch 7.x or 8.x

## Usage

### Creating a backend

```go
import "github.com/reveald/reveald/v1"

// Create an Elasticsearch backend
backend, err := reveald.NewElasticBackend(
    []string{"http://localhost:9200"},
    reveald.WithCredentials("username", "password"), // Optional
    reveald.WithScheme("https"),                     // Optional
)
if err != nil {
    // Handle error
}
```

### Basic search query

```go
import (
    "github.com/reveald/reveald/v1"
    "github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// Create a query builder
builder := reveald.NewQueryBuilder(nil, "my-index")

// Add a simple term query using typed API
termQuery := types.Query{
    Term: map[string]types.TermQuery{
        "active": {Value: true},
    },
}
builder.With(termQuery)

// Execute the query
ctx := context.Background()
result, err := backend.Execute(ctx, builder)
if err != nil {
    // Handle error
}

// Access the results
fmt.Printf("Found %d documents\n", result.TotalHitCount)
for _, hit := range result.Hits {
    fmt.Printf("Document: %v\n", hit)
}
```

### Complex boolean query

```go
import (
    "github.com/reveald/reveald/v1"
    "github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// Create a query builder
builder := reveald.NewQueryBuilder(nil, "products")

// Must be active
builder.With(types.Query{
    Term: map[string]types.TermQuery{
        "active": {Value: true},
    },
})

// Must be in electronics category
builder.With(types.Query{
    Term: map[string]types.TermQuery{
        "category": {Value: "electronics"},
    },
})

// Must have rating >= 4
gte := types.Float64(4)
builder.With(types.Query{
    Range: map[string]types.RangeQuery{
        "rating": &types.NumberRangeQuery{
            Gte: &gte,
        },
    },
})

// Must NOT be out of stock
builder.Without(types.Query{
    Term: map[string]types.TermQuery{
        "out_of_stock": {Value: true},
    },
})

// Should have "premium" tag (boosts relevance)
builder.Boost(types.Query{
    Term: map[string]types.TermQuery{
        "tags": {Value: "premium"},
    },
})

// Execute the query
result, err := backend.Execute(ctx, builder)
```

### Aggregations

```go
import (
    "github.com/reveald/reveald/v1"
    "github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

builder := reveald.NewQueryBuilder(nil, "products")

// Add a terms aggregation on the "category" field
fieldName := "category"
size := 10
termsAgg := types.Aggregations{
    Terms: &types.TermsAggregation{
        Field: &fieldName,
        Size:  &size,
    },
}
builder.Aggregation("categories", termsAgg)

// Execute the query
result, err := backend.Execute(ctx, builder)

// Access aggregation results
for _, bucket := range result.Aggregations["categories"] {
    fmt.Printf("Category: %v, Count: %d\n", bucket.Value, bucket.HitCount)
}
```

### Convenience Methods

The library also provides convenience methods for common operations:

```go
// Create a term query
builder.WithTermQuery("active", true)

// Create a range query
builder.WithRangeQuery("rating", nil, 4, nil, nil) // rating >= 4

// Create a match query
builder.WithMatchQuery("description", "premium product")

// Add a terms aggregation
builder.AddTermsAggregation("categories", "category", 10)
```

### Sorting and pagination

```go
import (
    "github.com/reveald/reveald/v1"
    "github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
)

builder := reveald.NewQueryBuilder(nil, "products")

// Add sorting (price descending)
builder.Sort("price", sortorder.Desc)

// Add pagination
builder.SetSize(10)   // items per page
builder.SetFrom(10)   // skip the first 10 results

// Or use the Selection API for backward compatibility
builder.Selection().Update(
    reveald.WithPageSize(10),
    reveald.WithOffset(10),
)

// Execute the query
result, err := backend.Execute(ctx, builder)
```

### Field selection

```go
builder := reveald.NewQueryBuilder(nil, "products")

// Only include specific fields
builder.IncludeFields("id", "title", "price", "category")

// Execute the query
result, err := backend.Execute(ctx, builder)
```

## Architecture

Reveald consists of several key components:

- **ElasticBackend**: Manages the connection to Elasticsearch and handles request execution
- **QueryBuilder**: Builds Elasticsearch queries with a fluent interface using typed structures
- **DocumentSelector**: Manages document selection, pagination, and sorting
- **Result**: Contains the search results, aggregations, and metadata

## Type Safety with Official Elasticsearch Types

The API leverages the official Elasticsearch Go client's types for query construction, providing both type safety and flexibility:

```go
// Import the required types
import "github.com/elastic/go-elasticsearch/v8/typedapi/types"

// Use the typed API for better IDE support and compile-time checks
builder.With(types.Query{
    Term: map[string]types.TermQuery{
        "active": {Value: true},
    },
})

// Or use convenience methods for common operations
builder.WithTermQuery("active", true)
```

Benefits of the typed API:
- **Type safety**: Compile-time checks for valid query structures
- **Better IDE support**: Code completion and documentation
- **Cleaner code**: No type assertions or generic maps
- **Future-proof**: Automatic updates with Elasticsearch API changes

This makes it easier to work with complex query structures like nested queries, geo queries, and script queries, which can be error-prone when built manually.

## Testing

The package includes both unit tests and integration tests. The integration tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up an Elasticsearch instance in a Docker container, allowing tests to run against a real Elasticsearch server.

### Running tests

Run all tests (including integration tests):
```bash
go test ./...
```

Run only unit tests (skip integration tests):
```bash
go test -short ./...
```

Run specific integration tests:
```bash
go test -run TestElasticBackendWithTestcontainers
```
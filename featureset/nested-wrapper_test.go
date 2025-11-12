package featureset

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/reveald/reveald/v2"
	"github.com/stretchr/testify/assert"
)

// Note: Full Process() flow tests including query building and aggregation handling
// are in integration tests. Unit tests focus on construction and configuration.

func TestNewNestedDocumentWrapper(t *testing.T) {
	t.Run("with single feature", func(t *testing.T) {
		feature := NewDynamicFilterFeature("items.category")
		wrapper := NewNestedDocumentWrapper("items", feature)

		assert.Equal(t, "items", wrapper.path)
		assert.Len(t, wrapper.features, 1)
		assert.False(t, wrapper.disjunctive)
	})

	t.Run("with multiple features", func(t *testing.T) {
		feature1 := NewDynamicFilterFeature("items.category")
		feature2 := NewDynamicFilterFeature("items.tags")
		feature3 := NewHistogramFeature("items.price")

		wrapper := NewNestedDocumentWrapper("items", feature1, feature2, feature3)

		assert.Equal(t, "items", wrapper.path)
		assert.Len(t, wrapper.features, 3)
		assert.False(t, wrapper.disjunctive)
	})

	t.Run("with disjunctive mode", func(t *testing.T) {
		feature := NewDynamicFilterFeature("items.category")
		wrapper := NewNestedDocumentWrapper("items", feature).Disjunctive(true)

		assert.True(t, wrapper.disjunctive)
	})
}

func TestNestedDocumentWrapper_Property(t *testing.T) {
	wrapper := NewNestedDocumentWrapper("items",
		NewDynamicFilterFeature("items.category"),
	)

	assert.Equal(t, "items", wrapper.Property())
}

func TestNestedDocumentWrapper_Features(t *testing.T) {
	feature1 := NewDynamicFilterFeature("items.category")
	feature2 := NewDynamicFilterFeature("items.tags")

	wrapper := NewNestedDocumentWrapper("items", feature1, feature2)
	features := wrapper.Features()

	assert.Len(t, features, 2)
	assert.Equal(t, feature1, features[0])
	assert.Equal(t, feature2, features[1])
}

func TestNestedDocumentWrapper_QueryBuilding(t *testing.T) {
	t.Run("builds nested query with dynamic filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
		)

		req := reveald.NewRequest()
		req.Set("items.category", "Widget", "Gadget")
		req.Set("items.tags", "Electronics", "Books")

		builder := reveald.NewQueryBuilder(req, "test-index")

		// Use a mock backend that doesn't execute, just for query building
		backend := &mockBackend{}
		endpoint := reveald.NewEndpoint(backend, reveald.Indices{"test-index"})
		endpoint.Register(wrapper)

		// Build the query through the feature chain
		wrapper.buildNestedQueryFilter(builder)

		esReq := builder.BuildRequest()

		// Verify query was generated
		assert.NotNil(t, esReq.Query)

		// Log the query structure for inspection
		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
	})

	t.Run("builds nested aggregations with dynamic filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
		)

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify aggregations were generated
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")
		assert.Contains(t, esReq.Aggregations, "items.tags")

		// Log the aggregations for inspection
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("builds nested histogram aggregation", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewHistogramFeature("items.price", WithInterval(5000)),
		)

		req := reveald.NewRequest()
		req.Set("items.price.min", "10000")
		req.Set("items.price.max", "50000")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify query and aggregations
		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.price")

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("builds nested date histogram aggregation", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDateHistogramFeature("items.createdAt", Month,
				WithDateFormat("yyyy-MM-dd"),
			),
		)

		req := reveald.NewRequest()
		req.Set("items.createdAt.min", "2023-01-01")
		req.Set("items.createdAt.max", "2023-12-31")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify query and aggregations
		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.createdAt")

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("builds mixed feature types", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
			NewHistogramFeature("items.price", WithInterval(5000)),
			NewDateHistogramFeature("items.createdAt", Month),
		)

		req := reveald.NewRequest()
		req.Set("items.category", "Widget", "Gadget")
		req.Set("items.tags", "Electronics")
		req.Set("items.price.min", "20000")
		req.Set("items.price.max", "60000")
		req.Set("items.createdAt.min", "2023-01-01")
		req.Set("items.createdAt.max", "2023-12-31")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify all aggregations were created
		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")
		assert.Contains(t, esReq.Aggregations, "items.tags")
		assert.Contains(t, esReq.Aggregations, "items.price")
		assert.Contains(t, esReq.Aggregations, "items.createdAt")

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("disjunctive mode filters differently", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
		).Disjunctive(true)

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.Set("items.tags", "Electronics")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// In disjunctive mode, aggregations should still be created
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")
		assert.Contains(t, esReq.Aggregations, "items.tags")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated aggregations (disjunctive):\n%s", aggsJSON)
	})

	t.Run("handles missing values", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category",
				WithAggregationSize(50),
				WithMissingValueAs("(No model)"),
			),
		)

		req := reveald.NewRequest()
		req.Set("items.category", "Widget", "(No model)")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query with missing values:\n%s", queryJSON)
		t.Logf("Generated aggregations with missing values:\n%s", aggsJSON)
	})

	t.Run("handles no filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
		)

		req := reveald.NewRequest()
		// No filters set

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Should still have aggregations, but query might be empty or minimal
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")
		assert.Contains(t, esReq.Aggregations, "items.tags")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated aggregations (no filters):\n%s", aggsJSON)
	})
}

// mockBackend implements reveald.Backend for testing
type mockBackend struct{}

func (m *mockBackend) Execute(ctx context.Context, qb *reveald.QueryBuilder) (*reveald.Result, error) {
	// Return empty result for testing
	return &reveald.Result{}, nil
}

func (m *mockBackend) ExecuteMultiple(ctx context.Context, qbs []*reveald.QueryBuilder) ([]*reveald.Result, error) {
	// Return empty results for testing
	results := make([]*reveald.Result, len(qbs))
	for i := range results {
		results[i] = &reveald.Result{}
	}
	return results, nil
}

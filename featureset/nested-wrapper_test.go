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
		feature := NewDynamicFilterFeature("carRelations.car.model")
		wrapper := NewNestedDocumentWrapper("carRelations", feature)

		assert.Equal(t, "carRelations", wrapper.path)
		assert.Len(t, wrapper.features, 1)
		assert.False(t, wrapper.disjunctive)
	})

	t.Run("with multiple features", func(t *testing.T) {
		feature1 := NewDynamicFilterFeature("carRelations.car.model")
		feature2 := NewDynamicFilterFeature("carRelations.car.color")
		feature3 := NewHistogramFeature("carRelations.price")

		wrapper := NewNestedDocumentWrapper("carRelations", feature1, feature2, feature3)

		assert.Equal(t, "carRelations", wrapper.path)
		assert.Len(t, wrapper.features, 3)
		assert.False(t, wrapper.disjunctive)
	})

	t.Run("with disjunctive mode", func(t *testing.T) {
		feature := NewDynamicFilterFeature("carRelations.car.model")
		wrapper := NewNestedDocumentWrapper("carRelations", feature).Disjunctive(true)

		assert.True(t, wrapper.disjunctive)
	})
}

func TestNestedDocumentWrapper_Property(t *testing.T) {
	wrapper := NewNestedDocumentWrapper("carRelations",
		NewDynamicFilterFeature("carRelations.car.model"),
	)

	assert.Equal(t, "carRelations", wrapper.Property())
}

func TestNestedDocumentWrapper_Features(t *testing.T) {
	feature1 := NewDynamicFilterFeature("carRelations.car.model")
	feature2 := NewDynamicFilterFeature("carRelations.car.color")

	wrapper := NewNestedDocumentWrapper("carRelations", feature1, feature2)
	features := wrapper.Features()

	assert.Len(t, features, 2)
	assert.Equal(t, feature1, features[0])
	assert.Equal(t, feature2, features[1])
}

func TestNestedDocumentWrapper_QueryBuilding(t *testing.T) {
	t.Run("builds nested query with dynamic filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDynamicFilterFeature("carRelations.car.model"),
			NewDynamicFilterFeature("carRelations.car.color"),
		)

		req := reveald.NewRequest()
		req.Set("carRelations.car.model", "V60", "XC60")
		req.Set("carRelations.car.color", "Glacier Silver", "Black Stone")

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
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDynamicFilterFeature("carRelations.car.model"),
			NewDynamicFilterFeature("carRelations.car.color"),
		)

		req := reveald.NewRequest()
		req.Set("carRelations.car.model", "V60")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify aggregations were generated
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "carRelations.car.model")
		assert.Contains(t, esReq.Aggregations, "carRelations.car.color")

		// Log the aggregations for inspection
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("builds nested histogram aggregation", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewHistogramFeature("carRelations.price", WithInterval(5000)),
		)

		req := reveald.NewRequest()
		req.Set("carRelations.price.min", "10000")
		req.Set("carRelations.price.max", "50000")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify query and aggregations
		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "carRelations.price")

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("builds nested date histogram aggregation", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDateHistogramFeature("carRelations.purchaseDate", Month,
				WithDateFormat("yyyy-MM-dd"),
			),
		)

		req := reveald.NewRequest()
		req.Set("carRelations.purchaseDate.min", "2023-01-01")
		req.Set("carRelations.purchaseDate.max", "2023-12-31")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify query and aggregations
		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "carRelations.purchaseDate")

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("builds mixed feature types", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDynamicFilterFeature("carRelations.car.model"),
			NewDynamicFilterFeature("carRelations.car.color"),
			NewHistogramFeature("carRelations.price", WithInterval(5000)),
			NewDateHistogramFeature("carRelations.purchaseDate", Month),
		)

		req := reveald.NewRequest()
		req.Set("carRelations.car.model", "V60", "XC60")
		req.Set("carRelations.car.color", "Glacier Silver")
		req.Set("carRelations.price.min", "20000")
		req.Set("carRelations.price.max", "60000")
		req.Set("carRelations.purchaseDate.min", "2023-01-01")
		req.Set("carRelations.purchaseDate.max", "2023-12-31")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Verify all aggregations were created
		assert.NotNil(t, esReq.Query)
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "carRelations.car.model")
		assert.Contains(t, esReq.Aggregations, "carRelations.car.color")
		assert.Contains(t, esReq.Aggregations, "carRelations.price")
		assert.Contains(t, esReq.Aggregations, "carRelations.purchaseDate")

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated query:\n%s", queryJSON)
		t.Logf("Generated aggregations:\n%s", aggsJSON)
	})

	t.Run("disjunctive mode filters differently", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDynamicFilterFeature("carRelations.car.model"),
			NewDynamicFilterFeature("carRelations.car.color"),
		).Disjunctive(true)

		req := reveald.NewRequest()
		req.Set("carRelations.car.model", "V60")
		req.Set("carRelations.car.color", "Glacier Silver")

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// In disjunctive mode, aggregations should still be created
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "carRelations.car.model")
		assert.Contains(t, esReq.Aggregations, "carRelations.car.color")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Generated aggregations (disjunctive):\n%s", aggsJSON)
	})

	t.Run("handles missing values", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDynamicFilterFeature("carRelations.car.model",
				WithAggregationSize(50),
				WithMissingValueAs("(No model)"),
			),
		)

		req := reveald.NewRequest()
		req.Set("carRelations.car.model", "V60", "(No model)")

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
		wrapper := NewNestedDocumentWrapper("carRelations",
			NewDynamicFilterFeature("carRelations.car.model"),
			NewDynamicFilterFeature("carRelations.car.color"),
		)

		req := reveald.NewRequest()
		// No filters set

		builder := reveald.NewQueryBuilder(req, "test-index")
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Should still have aggregations, but query might be empty or minimal
		assert.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "carRelations.car.model")
		assert.Contains(t, esReq.Aggregations, "carRelations.car.color")

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

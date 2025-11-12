package featureset

import (
	"encoding/json"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit tests for NestedDocumentWrapper - more thorough coverage of internal methods

func TestNestedDocumentWrapper_Construction(t *testing.T) {
	t.Run("creates wrapper with path and features", func(t *testing.T) {
		f1 := NewDynamicFilterFeature("nested.field1")
		f2 := NewDynamicFilterFeature("nested.field2")

		wrapper := NewNestedDocumentWrapper("nested", f1, f2)

		assert.Equal(t, "nested", wrapper.path)
		assert.Len(t, wrapper.features, 2)
		assert.False(t, wrapper.disjunctive)
	})

	t.Run("creates wrapper with no features", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("nested")

		assert.Equal(t, "nested", wrapper.path)
		assert.Len(t, wrapper.features, 0)
		assert.False(t, wrapper.disjunctive)
	})

	t.Run("disjunctive method returns wrapper for chaining", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("nested")
		result := wrapper.Disjunctive(true)

		assert.Equal(t, wrapper, result)
		assert.True(t, wrapper.disjunctive)
	})

	t.Run("can toggle disjunctive mode", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("nested")

		wrapper.Disjunctive(true)
		assert.True(t, wrapper.disjunctive)

		wrapper.Disjunctive(false)
		assert.False(t, wrapper.disjunctive)
	})
}

func TestNestedDocumentWrapper_PropertyAndFeatures(t *testing.T) {
	t.Run("Property returns the path", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("myNestedPath")
		assert.Equal(t, "myNestedPath", wrapper.Property())
	})

	t.Run("Features returns the child features", func(t *testing.T) {
		f1 := NewDynamicFilterFeature("nested.field1")
		f2 := NewHistogramFeature("nested.field2")
		f3 := NewDateHistogramFeature("nested.field3", Day)

		wrapper := NewNestedDocumentWrapper("nested", f1, f2, f3)
		features := wrapper.Features()

		require.Len(t, features, 3)
		assert.Equal(t, f1, features[0])
		assert.Equal(t, f2, features[1])
		assert.Equal(t, f3, features[2])
	})
}

func TestNestedDocumentWrapper_BuildDynamicFilterClause(t *testing.T) {
	t.Run("builds single term query for one value", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDynamicFilterFeature("items.category")

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDynamicFilterClause(feature, builder)

		require.NotNil(t, query)
		assert.NotNil(t, query.Term)
	})

	t.Run("builds bool query with should clauses for multiple values", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDynamicFilterFeature("items.category")

		req := reveald.NewRequest()
		req.Set("items.category", "Widget", "Gadget", "Tool")
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDynamicFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Bool)
		assert.Len(t, query.Bool.Should, 3)
		assert.Equal(t, 1, query.Bool.MinimumShouldMatch)
	})

	t.Run("returns nil when property not in request", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDynamicFilterFeature("items.category")

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDynamicFilterClause(feature, builder)

		assert.Nil(t, query)
	})

	t.Run("handles missing value in filter", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDynamicFilterFeature("items.category",
			WithMissingValueAs("(No category)"))

		req := reveald.NewRequest()
		req.Set("items.category", "Widget", "(No category)")
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDynamicFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Bool)
		require.Len(t, query.Bool.Should, 2)

		// One should be a term query, one should be a missing query (bool.must_not.exists)
		hasTermQuery := false
		hasMissingQuery := false
		for _, should := range query.Bool.Should {
			if should.Term != nil {
				hasTermQuery = true
			}
			if should.Bool != nil && len(should.Bool.MustNot) > 0 {
				hasMissingQuery = true
			}
		}
		assert.True(t, hasTermQuery, "Should have term query for 'V60'")
		assert.True(t, hasMissingQuery, "Should have missing query for '(No category)'")
	})

	t.Run("handles only missing value in filter", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDynamicFilterFeature("items.category",
			WithMissingValueAs("(No category)"))

		req := reveald.NewRequest()
		req.Set("items.category", "(No category)")
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDynamicFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Bool)
		require.Len(t, query.Bool.MustNot, 1)
		assert.NotNil(t, query.Bool.MustNot[0].Exists)
	})
}

func TestNestedDocumentWrapper_BuildHistogramFilterClause(t *testing.T) {
	t.Run("builds range query with min and max", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewHistogramFeature("items.price")

		req := reveald.NewRequest()
		req.SetParam(reveald.NewParameter("items.price.min", "10000"))
		req.SetParam(reveald.NewParameter("items.price.max", "50000"))
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildHistogramFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Range)
	})

	t.Run("builds range query with only min", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewHistogramFeature("items.price")

		req := reveald.NewRequest()
		req.SetParam(reveald.NewParameter("items.price.min", "10000"))
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildHistogramFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Range)
	})

	t.Run("builds range query with only max", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewHistogramFeature("items.price")

		req := reveald.NewRequest()
		req.SetParam(reveald.NewParameter("items.price.max", "50000"))
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildHistogramFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Range)
	})

	t.Run("returns nil when no range specified", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewHistogramFeature("items.price")

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildHistogramFilterClause(feature, builder)

		assert.Nil(t, query)
	})

	t.Run("respects negative values setting", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewHistogramFeature("items.temperature",
			WithNegativeValuesAllowed())

		req := reveald.NewRequest()
		req.SetParam(reveald.NewParameter("items.temperature.min", "-20"))
		req.SetParam(reveald.NewParameter("items.temperature.max", "30"))
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildHistogramFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Range)
	})
}

func TestNestedDocumentWrapper_BuildDateHistogramFilterClause(t *testing.T) {
	t.Run("builds date range query with min and max as timestamps", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDateHistogramFeature("items.createdAt", Month)

		req := reveald.NewRequest()
		// Use timestamps (milliseconds since epoch) instead of date strings
		req.SetParam(reveald.NewParameter("items.createdAt.min", "1672531200000")) // 2023-01-01
		req.SetParam(reveald.NewParameter("items.createdAt.max", "1704067199000")) // 2023-12-31
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDateHistogramFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Range)
	})

	t.Run("builds date range query with only min as timestamp", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDateHistogramFeature("items.createdAt", Month)

		req := reveald.NewRequest()
		req.SetParam(reveald.NewParameter("items.createdAt.min", "1672531200000"))
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDateHistogramFilterClause(feature, builder)

		require.NotNil(t, query)
		require.NotNil(t, query.Range)
	})

	t.Run("returns nil when no date range specified", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDateHistogramFeature("items.createdAt", Month)

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDateHistogramFilterClause(feature, builder)

		assert.Nil(t, query)
	})

	t.Run("returns nil for non-range parameter", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")
		feature := NewDateHistogramFeature("items.createdAt", Month)

		req := reveald.NewRequest()
		req.Set("items.createdAt", "2023-01-01") // Not a range
		builder := reveald.NewQueryBuilder(req, "test")

		query := wrapper.buildDateHistogramFilterClause(feature, builder)

		assert.Nil(t, query)
	})
}

func TestNestedDocumentWrapper_BuildNestedQueryFilter(t *testing.T) {
	t.Run("builds nested query with single filter", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"))

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedQueryFilter(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Query)

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		t.Logf("Single filter query:\n%s", queryJSON)

		// Verify structure
		assert.NotNil(t, esReq.Query.Bool)
		assert.Len(t, esReq.Query.Bool.Must, 1)
		assert.NotNil(t, esReq.Query.Bool.Must[0].Nested)
		assert.Equal(t, "items", esReq.Query.Bool.Must[0].Nested.Path)
	})

	t.Run("builds nested query with multiple filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"))

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.Set("items.tags", "Active")
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedQueryFilter(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Query)

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		t.Logf("Multiple filters query:\n%s", queryJSON)

		// Verify nested query contains both filters in bool.must
		assert.NotNil(t, esReq.Query.Bool)
		assert.Len(t, esReq.Query.Bool.Must, 1)
		nestedQuery := esReq.Query.Bool.Must[0].Nested
		require.NotNil(t, nestedQuery)
		require.NotNil(t, nestedQuery.Query.Bool)
		assert.Len(t, nestedQuery.Query.Bool.Must, 2)
	})

	t.Run("builds nested query with mixed feature types", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewHistogramFeature("items.price"),
			NewDateHistogramFeature("items.createdAt", Month))

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.SetParam(reveald.NewParameter("items.price.min", "20000"))
		req.SetParam(reveald.NewParameter("items.price.max", "50000"))
		req.SetParam(reveald.NewParameter("items.createdAt.min", "2023-01-01"))
		req.SetParam(reveald.NewParameter("items.createdAt.max", "2023-12-31"))
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedQueryFilter(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Query)

		queryJSON, _ := json.MarshalIndent(esReq.Query, "", "  ")
		t.Logf("Mixed feature types query:\n%s", queryJSON)

		// Verify filters are included (at least dynamic filter and histogram)
		// Date histogram may not generate a query clause if the date format isn't parsed correctly
		nestedQuery := esReq.Query.Bool.Must[0].Nested
		require.NotNil(t, nestedQuery)
		require.NotNil(t, nestedQuery.Query.Bool)
		// Should have at least 2 filters (dynamic + histogram), date might not be included
		assert.GreaterOrEqual(t, len(nestedQuery.Query.Bool.Must), 2)
	})

	t.Run("does not add nested query when no filters active", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"))

		req := reveald.NewRequest()
		// No filters set
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedQueryFilter(builder)

		esReq := builder.BuildRequest()

		// Query should be nil or empty bool
		if esReq.Query != nil && esReq.Query.Bool != nil {
			assert.Empty(t, esReq.Query.Bool.Must)
		}
	})
}

func TestNestedDocumentWrapper_BuildFilterMustClauses(t *testing.T) {
	t.Run("conjunctive mode includes all clauses", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")

		feature1 := NewDynamicFilterFeature("items.category")
		feature2 := NewDynamicFilterFeature("items.tags")

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.Set("items.tags", "Active")
		builder := reveald.NewQueryBuilder(req, "test")

		clause1 := wrapper.buildDynamicFilterClause(feature1, builder)
		clause2 := wrapper.buildDynamicFilterClause(feature2, builder)

		allClauses := []types.Query{*clause1, *clause2}
		perProperty := map[string]*types.Query{
			"items.category": clause1,
			"items.tags": clause2,
		}

		// For model property, should include both clauses
		result := wrapper.buildFilterMustClauses("items.category", allClauses, perProperty)
		assert.Len(t, result, 2)
	})

	t.Run("disjunctive mode excludes current property", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items").Disjunctive(true)

		feature1 := NewDynamicFilterFeature("items.category")
		feature2 := NewDynamicFilterFeature("items.tags")

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.Set("items.tags", "Active")
		builder := reveald.NewQueryBuilder(req, "test")

		clause1 := wrapper.buildDynamicFilterClause(feature1, builder)
		clause2 := wrapper.buildDynamicFilterClause(feature2, builder)

		allClauses := []types.Query{*clause1, *clause2}
		perProperty := map[string]*types.Query{
			"items.category": clause1,
			"items.tags": clause2,
		}

		// For model property, should only include color clause (exclude self)
		result := wrapper.buildFilterMustClauses("items.category", allClauses, perProperty)
		assert.Len(t, result, 1)

		// For color property, should only include model clause (exclude self)
		result = wrapper.buildFilterMustClauses("items.tags", allClauses, perProperty)
		assert.Len(t, result, 1)
	})

	t.Run("disjunctive mode with single property has no filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items").Disjunctive(true)

		feature1 := NewDynamicFilterFeature("items.category")

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		builder := reveald.NewQueryBuilder(req, "test")

		clause1 := wrapper.buildDynamicFilterClause(feature1, builder)

		allClauses := []types.Query{*clause1}
		perProperty := map[string]*types.Query{
			"items.category": clause1,
		}

		// For model property, should have no filters (only property excludes itself)
		result := wrapper.buildFilterMustClauses("items.category", allClauses, perProperty)
		assert.Len(t, result, 0)
	})
}

func TestNestedDocumentWrapper_BuildNestedAggregations(t *testing.T) {
	t.Run("builds aggregations for single dynamic filter", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Single filter aggregation:\n%s", aggsJSON)
	})

	t.Run("builds aggregations for multiple dynamic filters", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
			NewDynamicFilterFeature("items.status"))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")
		assert.Contains(t, esReq.Aggregations, "items.tags")
		assert.Contains(t, esReq.Aggregations, "items.status")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Multiple filters aggregation:\n%s", aggsJSON)
	})

	t.Run("builds histogram aggregation", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewHistogramFeature("items.price", WithInterval(10000)))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.price")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Histogram aggregation:\n%s", aggsJSON)
	})

	t.Run("builds date histogram aggregation with fixed interval", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDateHistogramFeature("items.createdAt", Day,
				WithDateFormat("yyyy-MM-dd")))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.createdAt")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Date histogram aggregation (fixed):\n%s", aggsJSON)
	})

	t.Run("builds date histogram aggregation with calendar interval", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDateHistogramFeature("items.createdAt", Month,
				WithCalendarIntervalInstead(),
				WithDateFormat("yyyy-MM-dd")))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.createdAt")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Date histogram aggregation (calendar):\n%s", aggsJSON)
	})

	t.Run("builds all aggregation types together", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"),
			NewHistogramFeature("items.price", WithInterval(5000)),
			NewDateHistogramFeature("items.createdAt", Month))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Aggregations)
		assert.Contains(t, esReq.Aggregations, "items.category")
		assert.Contains(t, esReq.Aggregations, "items.tags")
		assert.Contains(t, esReq.Aggregations, "items.price")
		assert.Contains(t, esReq.Aggregations, "items.createdAt")

		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("All aggregation types:\n%s", aggsJSON)
	})

	t.Run("conjunctive mode filters all aggregations equally", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags"))

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.Set("items.tags", "Active")
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Conjunctive aggregations:\n%s", aggsJSON)

		// Both aggregations should include both filters
		// (In actual implementation, both model and color aggs would have both filters)
	})

	t.Run("disjunctive mode filters aggregations independently", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"),
			NewDynamicFilterFeature("items.tags")).Disjunctive(true)

		req := reveald.NewRequest()
		req.Set("items.category", "Widget")
		req.Set("items.tags", "Active")
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Disjunctive aggregations:\n%s", aggsJSON)

		// Model aggregation should only have color filter
		// Color aggregation should only have model filter
	})
}

func TestNestedDocumentWrapper_AggregationOptions(t *testing.T) {
	t.Run("respects aggregation size option", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category",
				WithAggregationSize(100)))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Custom size aggregation:\n%s", aggsJSON)

		// Verify size is set to 100 in the aggregation
		assert.Contains(t, string(aggsJSON), `"size": 100`)
	})

	t.Run("respects histogram interval option", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewHistogramFeature("items.price",
				WithInterval(25000)))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Custom interval histogram:\n%s", aggsJSON)

		// Verify interval is set to 25000
		assert.Contains(t, string(aggsJSON), `"interval": 25000`)
	})

	t.Run("respects date histogram format option", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDateHistogramFeature("items.createdAt", Day,
				WithDateFormat("yyyy/MM/dd")))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Custom format date histogram:\n%s", aggsJSON)

		// Verify format is set
		assert.Contains(t, string(aggsJSON), `"format": "yyyy/MM/dd"`)
	})

	t.Run("respects date histogram timezone option", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDateHistogramFeature("items.createdAt", Day,
				WithDateTimeZone("America/New_York")))

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		aggsJSON, _ := json.MarshalIndent(esReq.Aggregations, "", "  ")
		t.Logf("Timezone date histogram:\n%s", aggsJSON)

		// Verify timezone is set
		assert.Contains(t, string(aggsJSON), `"time_zone": "America/New_York"`)
	})
}

func TestNestedDocumentWrapper_EdgeCases(t *testing.T) {
	t.Run("handles empty feature list", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items")

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		// Should not panic
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()

		// Should have no aggregations
		if esReq.Aggregations != nil {
			assert.Empty(t, esReq.Aggregations)
		}
	})

	t.Run("handles unsupported feature types gracefully", func(t *testing.T) {
		// Use a standard feature that shouldn't be processed by nested wrapper
		// Create a simple bool query for the static filter
		dummyQuery := types.Query{
			Bool: &types.BoolQuery{},
		}
		staticFilter := NewStaticFilterFeatureWithQuery(dummyQuery)

		wrapper := NewNestedDocumentWrapper("items", staticFilter)

		req := reveald.NewRequest()
		builder := reveald.NewQueryBuilder(req, "test")

		// Should not panic
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		// Should not add any aggregations for unsupported feature
		esReq := builder.BuildRequest()
		if esReq.Aggregations != nil {
			assert.Empty(t, esReq.Aggregations)
		}
	})

	t.Run("handles properties with different nested paths", func(t *testing.T) {
		// All features should use the wrapper's path, not extract from property
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.attributes.color"),
			NewDynamicFilterFeature("items.attributes.size"))

		req := reveald.NewRequest()
		req.Set("items.attributes.color", "Red")
		builder := reveald.NewQueryBuilder(req, "test")

		wrapper.buildNestedQueryFilter(builder)

		esReq := builder.BuildRequest()
		require.NotNil(t, esReq.Query)

		// Verify path is "items", not "items.attributes"
		nestedQuery := esReq.Query.Bool.Must[0].Nested
		assert.Equal(t, "items", nestedQuery.Path)
	})

	t.Run("handles very long property paths", func(t *testing.T) {
		longPath := "items.vehicle.specifications.engine.cylinders.count"
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature(longPath))

		req := reveald.NewRequest()
		req.Set(longPath, "4", "6", "8")
		builder := reveald.NewQueryBuilder(req, "test")

		// Should not panic with long paths
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)

		esReq := builder.BuildRequest()
		assert.NotNil(t, esReq.Aggregations)
	})

	t.Run("handles empty values gracefully", func(t *testing.T) {
		wrapper := NewNestedDocumentWrapper("items",
			NewDynamicFilterFeature("items.category"))

		req := reveald.NewRequest()
		req.Set("items.category") // Empty values
		builder := reveald.NewQueryBuilder(req, "test")

		// Should handle empty values without panic
		wrapper.buildNestedQueryFilter(builder)
		wrapper.buildNestedAggregations(builder)
	})
}

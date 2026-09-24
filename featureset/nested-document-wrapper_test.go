package featureset

import (
	"errors"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
	"github.com/stretchr/testify/assert"
)

func TestNestedDocumentWrapper_Exclusions(t *testing.T) {
	errStop := errors.New("stop")

	// build runs the wrapper and returns the main query built from its child features
	build := func(wrapper *NestedDocumentWrapper, params ...reveald.Parameter) *search.Request {
		builder := reveald.NewQueryBuilder(reveald.NewRequest(params...), "index")
		_, err := wrapper.Process(builder, func(*reveald.QueryBuilder) (*reveald.Result, error) {
			return nil, errStop
		})
		assert.ErrorIs(t, err, errStop)
		return builder.BuildRequest()
	}

	// excluded returns the author excluded by a parent-level nested must_not clause
	excluded := func(t *testing.T, q types.Query) any {
		assert.NotNil(t, q.Nested)
		assert.Equal(t, "reviews", q.Nested.Path)
		return q.Nested.Query.Bool.Should[0].Term["reviews.author.keyword"].Value
	}

	t.Run("exclusion only", func(t *testing.T) {
		req := build(
			NewNestedDocumentWrapper("reviews",
				WithInnerHits(),
				WithFeatures(NewDynamicFilterFeature("reviews.author")),
			),
			reveald.NewParameter("reviews.author.not", "John"),
		)

		// Parent documents with any review by John are excluded
		assert.Empty(t, req.Query.Bool.Must)
		assert.Len(t, req.Query.Bool.MustNot, 1)
		assert.Equal(t, "John", excluded(t, req.Query.Bool.MustNot[0]))
		assert.Nil(t, req.Query.Bool.MustNot[0].Nested.InnerHits)

		// The main query already excludes parents, so the nested filter only has includes
		agg := req.Aggregations["reviews.author"]
		assert.Empty(t, agg.Aggregations["reviews.author._filter"].Filter.Bool.MustNot)
	})

	t.Run("inclusion and exclusion", func(t *testing.T) {
		req := build(
			NewNestedDocumentWrapper("reviews",
				WithInnerHits(),
				WithFeatures(
					NewDynamicFilterFeature("reviews.author"),
					NewDynamicFilterFeature("reviews.rating"),
				),
			),
			reveald.NewParameter("reviews.rating", "5"),
			reveald.NewParameter("reviews.author.not", "John"),
		)

		// Inclusions are wrapped together in one nested query with inner hits
		assert.Len(t, req.Query.Bool.Must, 1)
		nested := req.Query.Bool.Must[0].Nested
		assert.NotNil(t, nested.InnerHits)
		assert.Len(t, nested.Query.Bool.Must, 1)
		assert.Empty(t, nested.Query.Bool.MustNot)

		// Exclusions are applied to parent documents
		assert.Len(t, req.Query.Bool.MustNot, 1)
		assert.Equal(t, "John", excluded(t, req.Query.Bool.MustNot[0]))
	})

	t.Run("disjunctive applies other exclusions to parents", func(t *testing.T) {
		req := build(
			NewNestedDocumentWrapper("reviews",
				WithDisjunctiveAggregations(),
				WithFeatures(
					NewDynamicFilterFeature("reviews.author"),
					NewDynamicFilterFeature("reviews.rating"),
				),
			),
			reveald.NewParameter("reviews.author.not", "John"),
		)

		// Own exclusion is skipped: no parent filter layer
		author := req.Aggregations["reviews.author"]
		assert.NotNil(t, author.Global)
		assert.Contains(t, author.Aggregations, "reviews.author._nested")
		assert.NotContains(t, author.Aggregations, "reviews.author._parent")

		// Other aggregations filter parents before entering the nested scope
		rating := req.Aggregations["reviews.rating"]
		parent := rating.Aggregations["reviews.rating._parent"]
		assert.Len(t, parent.Filter.Bool.MustNot, 1)
		assert.Equal(t, "John", excluded(t, parent.Filter.Bool.MustNot[0]))
		assert.Contains(t, parent.Aggregations, "reviews.rating._nested")
	})
}

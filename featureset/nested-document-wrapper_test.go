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

	// aggFilter returns the filter applied to a wrapped aggregation
	aggFilter := func(req *search.Request, name string, disjunctive bool) *types.BoolQuery {
		agg := req.Aggregations[name]
		if disjunctive {
			agg = agg.Aggregations[name+"._nested"]
		}
		return agg.Aggregations[name+"._filter"].Filter.Bool
	}

	excluded := func(q types.Query) any {
		return q.Bool.Should[0].Term["reviews.author.keyword"].Value
	}

	t.Run("exclusion only", func(t *testing.T) {
		req := build(
			NewNestedDocumentWrapper("reviews",
				WithFeatures(NewDynamicFilterFeature("reviews.author")),
			),
			reveald.NewParameter("reviews.author.not", "John"),
		)

		assert.Len(t, req.Query.Bool.Must, 1)
		nested := req.Query.Bool.Must[0].Nested
		assert.Equal(t, "reviews", nested.Path)
		assert.Len(t, nested.Query.Bool.MustNot, 1)
		assert.Equal(t, "John", excluded(nested.Query.Bool.MustNot[0]))

		filter := aggFilter(req, "reviews.author", false)
		assert.Len(t, filter.MustNot, 1)
		assert.Equal(t, "John", excluded(filter.MustNot[0]))
	})

	t.Run("inclusion and exclusion", func(t *testing.T) {
		req := build(
			NewNestedDocumentWrapper("reviews",
				WithFeatures(
					NewDynamicFilterFeature("reviews.author"),
					NewDynamicFilterFeature("reviews.rating"),
				),
			),
			reveald.NewParameter("reviews.rating", "5"),
			reveald.NewParameter("reviews.author.not", "John"),
		)

		nested := req.Query.Bool.Must[0].Nested
		assert.Len(t, nested.Query.Bool.Must, 1)
		assert.Len(t, nested.Query.Bool.MustNot, 1)

		for _, name := range []string{"reviews.author", "reviews.rating"} {
			filter := aggFilter(req, name, false)
			assert.Len(t, filter.Must, 1, name)
			assert.Len(t, filter.MustNot, 1, name)
		}
	})

	t.Run("disjunctive skips own exclusion", func(t *testing.T) {
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

		assert.Empty(t, aggFilter(req, "reviews.author", true).MustNot)

		filter := aggFilter(req, "reviews.rating", true)
		assert.Len(t, filter.MustNot, 1)
		assert.Equal(t, "John", excluded(filter.MustNot[0]))
	})
}

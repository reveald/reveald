package featureset_test

import (
	"reflect"
	"testing"

	"github.com/reveald/reveald/v2"
	"github.com/reveald/reveald/v2/featureset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_Reflect_StringFieldSortsOnKeyword verifies that string fields produce sort options
// pointing to the .keyword subfield, which is required for sorting in Elasticsearch.
// []string slice fields should NOT get .keyword since arrays sort directly.
func Test_Reflect_StringFieldSortsOnKeyword(t *testing.T) {
	type TTarget struct {
		Name    string   `json:"name"`
		Message string   `json:"message" reveald:"searchable"`
		Tags    []string `json:"tags"`
		Count   int      `json:"count"`
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(TTarget{}))
	sf := featureset.NewSortingFeature("sort", sortOpts...)

	req := reveald.NewRequest()
	result, err := sf.Process(reveald.NewQueryBuilder(req, "test-index"), func(b *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{}, nil
	})
	require.NoError(t, err)
	require.NotNil(t, result.Sorting)

	optMap := map[string]string{}
	for _, o := range result.Sorting.Options {
		optMap[o.Name] = o.Value
	}

	assert.Equal(t, "name.keyword", optMap["name-desc"], "string field should sort on .keyword")
	assert.Equal(t, "name.keyword", optMap["name-asc"], "string field should sort on .keyword")
	assert.Equal(t, "message.keyword", optMap["message-desc"], "searchable string field should sort on .keyword")
	assert.Equal(t, "message.keyword", optMap["message-asc"], "searchable string field should sort on .keyword")
	assert.Equal(t, "tags", optMap["tags-desc"], "[]string field should NOT use .keyword")
	assert.Equal(t, "tags", optMap["tags-asc"], "[]string field should NOT use .keyword")
	assert.Equal(t, "count", optMap["count-desc"], "int field should sort on bare field name")
	assert.Equal(t, "count", optMap["count-asc"], "int field should sort on bare field name")
}

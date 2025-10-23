package featureset

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v1"
	"github.com/stretchr/testify/assert"
)

func Test_NewStaticFilterFeature(t *testing.T) {
	table := []struct {
		name    string
		options []StaticFilterOption
		check   func(*testing.T, *types.Query)
	}{
		{
			name:    "no options",
			options: []StaticFilterOption{},
			check: func(t *testing.T, q *types.Query) {
				assert.Nil(t, q)
			},
		},
		{
			name:    "required property",
			options: []StaticFilterOption{WithRequiredProperty("property")},
			check: func(t *testing.T, q *types.Query) {
				assert.NotNil(t, q)
				assert.NotNil(t, q.Bool)
				assert.NotNil(t, q.Bool.Must)
				assert.Len(t, q.Bool.Must, 1)
				assert.NotNil(t, q.Bool.Must[0].Exists)
				assert.Equal(t, "property", q.Bool.Must[0].Exists.Field)
			},
		},
		{
			name:    "required value",
			options: []StaticFilterOption{WithRequiredValue("property", "value")},
			check: func(t *testing.T, q *types.Query) {
				assert.NotNil(t, q)
				assert.NotNil(t, q.Bool)
				assert.NotNil(t, q.Bool.Must)
				assert.Len(t, q.Bool.Must, 1)
				assert.NotNil(t, q.Bool.Must[0].Term)
				termQuery := q.Bool.Must[0].Term["property"]
				assert.Equal(t, "value", termQuery.Value)
			},
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			sff := NewStaticFilterFeature(tt.options...)
			tt.check(t, sff.query)
		})
	}
}

func Test_StaticFilterFeature_Build(t *testing.T) {
	// We need to check the structure that gets added to the query builder
	// which might be different than what's directly in sff.query
	for _, tt := range []struct {
		name    string
		options []StaticFilterOption
	}{
		{"no options", []StaticFilterOption{}},
		{"required property", []StaticFilterOption{WithRequiredProperty("property")}},
		{"required value", []StaticFilterOption{WithRequiredValue("property", "value")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sff := NewStaticFilterFeature(tt.options...)
			qb := reveald.NewQueryBuilder(&reveald.Request{}, "-")

			_, err := sff.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
				return nil, nil
			})
			assert.NoError(t, err)

			// For the official Elasticsearch client, we can directly check the Query object
			rawQuery := qb.RawQuery()
			assert.NotNil(t, rawQuery)

			if tt.name == "no options" {
				// Either Bool should be nil or have no conditions
				if rawQuery.Bool != nil {
					assert.Nil(t, rawQuery.Bool.Must)
					assert.Nil(t, rawQuery.Bool.MustNot)
					assert.Nil(t, rawQuery.Bool.Should)
				}
			} else {
				// Should have a bool query with a must clause
				assert.NotNil(t, rawQuery.Bool)
				assert.NotNil(t, rawQuery.Bool.Must)
				assert.NotEmpty(t, rawQuery.Bool.Must)
			}
		})
	}
}

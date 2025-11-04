package featureset

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/reveald/reveald/v2"
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
		{
			name:    "missing property",
			options: []StaticFilterOption{WithMissingProperty("property")},
			check: func(t *testing.T, q *types.Query) {
				assert.NotNil(t, q)
				assert.NotNil(t, q.Bool)
				assert.NotNil(t, q.Bool.Must)
				assert.Len(t, q.Bool.Must, 1)
				// Should have a bool query with must_not exists
				innerBool := q.Bool.Must[0].Bool
				assert.NotNil(t, innerBool)
				assert.NotNil(t, innerBool.MustNot)
				assert.Len(t, innerBool.MustNot, 1)
				assert.NotNil(t, innerBool.MustNot[0].Exists)
				assert.Equal(t, "property", innerBool.MustNot[0].Exists.Field)
			},
		},
		{
			name:    "exclude missing property",
			options: []StaticFilterOption{WithExcludeMissingProperty("property")},
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
			name: "multiple options - required value and exclude missing",
			options: []StaticFilterOption{
				WithRequiredValue("category", "electronics"),
				WithExcludeMissingProperty("price"),
			},
			check: func(t *testing.T, q *types.Query) {
				assert.NotNil(t, q)
				assert.NotNil(t, q.Bool)
				assert.NotNil(t, q.Bool.Must)
				assert.Len(t, q.Bool.Must, 2)

				// Check for term query
				var foundTerm, foundExists bool
				for _, mustClause := range q.Bool.Must {
					if mustClause.Term != nil {
						termQuery := mustClause.Term["category"]
						assert.Equal(t, "electronics", termQuery.Value)
						foundTerm = true
					}
					if mustClause.Exists != nil {
						assert.Equal(t, "price", mustClause.Exists.Field)
						foundExists = true
					}
				}
				assert.True(t, foundTerm, "Should have term query")
				assert.True(t, foundExists, "Should have exists query")
			},
		},
		{
			name: "multiple options - required property and missing property",
			options: []StaticFilterOption{
				WithRequiredProperty("active"),
				WithMissingProperty("archived"),
			},
			check: func(t *testing.T, q *types.Query) {
				assert.NotNil(t, q)
				assert.NotNil(t, q.Bool)
				assert.NotNil(t, q.Bool.Must)
				assert.Len(t, q.Bool.Must, 2)

				// Check for exists query on "active" and missing query on "archived"
				var foundActive, foundArchivedMissing bool
				for _, mustClause := range q.Bool.Must {
					if mustClause.Exists != nil && mustClause.Exists.Field == "active" {
						foundActive = true
					}
					if mustClause.Bool != nil && mustClause.Bool.MustNot != nil {
						for _, mustNotClause := range mustClause.Bool.MustNot {
							if mustNotClause.Exists != nil && mustNotClause.Exists.Field == "archived" {
								foundArchivedMissing = true
							}
						}
					}
				}
				assert.True(t, foundActive, "Should have exists query for active")
				assert.True(t, foundArchivedMissing, "Should have missing query for archived")
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
		{"missing property", []StaticFilterOption{WithMissingProperty("property")}},
		{"exclude missing property", []StaticFilterOption{WithExcludeMissingProperty("property")}},
		{
			"multiple options",
			[]StaticFilterOption{
				WithRequiredValue("category", "electronics"),
				WithExcludeMissingProperty("price"),
			},
		},
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

package featureset

import (
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
	"github.com/stretchr/testify/assert"
)

func Test_NewStaticFilterFeature(t *testing.T) {
	table := []struct {
		name    string
		options []StaticFilterOption
		result  elastic.Query
	}{
		{"no options", []StaticFilterOption{}, nil},
		{"required property", []StaticFilterOption{WithRequiredProperty("property")}, elastic.NewBoolQuery().Must(elastic.NewExistsQuery("property"))},
		{"required value", []StaticFilterOption{WithRequiredValue("property", "value")}, elastic.NewBoolQuery().Must(elastic.NewTermQuery("property", "value"))},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			sff := NewStaticFilterFeature(tt.options...)
			assert.Equal(t, tt.result, sff.query)
		})
	}
}

func Test_StaticFilterFeature_Build(t *testing.T) {
	table := []struct {
		name    string
		options []StaticFilterOption
		result  elastic.Query
	}{
		{"no options", []StaticFilterOption{}, elastic.NewBoolQuery()},
		{"required property", []StaticFilterOption{WithRequiredProperty("property")}, elastic.NewBoolQuery().Must(elastic.NewBoolQuery().Must(elastic.NewExistsQuery("property")))},
		{"required value", []StaticFilterOption{WithRequiredValue("property", "value")}, elastic.NewBoolQuery().Must(elastic.NewBoolQuery().Must(elastic.NewTermQuery("property", "value")))},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			sff := NewStaticFilterFeature(tt.options...)
			qb := reveald.NewQueryBuilder(&reveald.Request{}, "-")

			_, err := sff.Process(qb, func(_ *reveald.QueryBuilder) (*reveald.Result, error) {
				return nil, nil
			})
			assert.NoError(t, err)

			assert.Equal(t, tt.result, qb.RawQuery())
		})
	}
}

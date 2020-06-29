package featureset

import (
	"testing"

	"github.com/reveald/reveald"
	"github.com/stretchr/testify/assert"
)

func Test_NewSortingFeature(t *testing.T) {
	table := []struct {
		name          string
		param         string
		options       []SortingOption
		defaultOption string
		result        map[string]sortingOption
	}{
		{"no options", "sort", []SortingOption{}, "", make(map[string]sortingOption)},
		{"without default", "sort", []SortingOption{WithSortOption("opt", "prop", true)}, "", map[string]sortingOption{"opt": {"prop", true}}},
		{"with default", "sort", []SortingOption{WithSortOption("opt", "prop", true), WithDefaultSortOption("opt")}, "opt", map[string]sortingOption{"opt": {"prop", true}}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			sf := NewSortingFeature(tt.param, tt.options...)
			assert.Equal(t, tt.param, sf.param)
			assert.Equal(t, tt.defaultOption, sf.defaultOption)
			assert.Equal(t, tt.result, sf.options)
		})
	}
}

func Test_SortingFeature_Build(t *testing.T) {
	table := []struct {
		name    string
		feature *SortingFeature
		req     *reveald.Request
	}{
		{"request missing param", NewSortingFeature("sort", WithSortOption("name", "property", true)), reveald.NewRequest()},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			qb := reveald.NewQueryBuilder(tt.req, "-")
			tt.feature.build(qb)

			sort := qb.Selection().Sort()
			if sort != nil {
				t.Errorf("sort expected to be nil, was %v", sort)
			}
		})
	}
}

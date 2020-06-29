package reveald

import (
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

func Test_NewDocumentSelector(t *testing.T) {
	table := []struct {
		name      string
		selectors []Selector
		validate  func(*DocumentSelector) bool
	}{
		{"default page size", []Selector{}, func(ds *DocumentSelector) bool {
			return ds.pageSize == defaultPageSize
		}},
		{"default offset", []Selector{}, func(ds *DocumentSelector) bool {
			return ds.offset == 0
		}},
		{"default sort", []Selector{}, func(ds *DocumentSelector) bool {
			return ds.sort == nil
		}},
		{"set page size", []Selector{WithPageSize(10)}, func(ds *DocumentSelector) bool {
			return ds.pageSize == 10
		}},
		{"set offset", []Selector{WithOffset(10)}, func(ds *DocumentSelector) bool {
			return ds.offset == 10
		}},
		{"set sort", []Selector{WithSort(elastic.NewFieldSort("test"))}, func(ds *DocumentSelector) bool {
			return assert.Equal(t, elastic.NewFieldSort("test"), ds.sort)
		}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewDocumentSelector(tt.selectors...)
			assert.True(t, tt.validate(ds))
		})
	}
}

func Test_Update(t *testing.T) {
	table := []struct {
		name      string
		ds        *DocumentSelector
		selectors []Selector
		validate  func(*DocumentSelector) bool
	}{
		{"from default page size", NewDocumentSelector(), []Selector{WithPageSize(10)}, func(ds *DocumentSelector) bool {
			return ds.pageSize == 10
		}},
		{"from default offset", NewDocumentSelector(), []Selector{WithOffset(10)}, func(ds *DocumentSelector) bool {
			return ds.offset == 10
		}},
		{"from default sort", NewDocumentSelector(), []Selector{WithSort(elastic.NewFieldSort("test"))}, func(ds *DocumentSelector) bool {
			return assert.Equal(t, elastic.NewFieldSort("test"), ds.sort)
		}},
		{"from set page size", NewDocumentSelector(WithPageSize(20)), []Selector{WithPageSize(10)}, func(ds *DocumentSelector) bool {
			return ds.pageSize == 10
		}},
		{"from set offset", NewDocumentSelector(WithOffset(20)), []Selector{WithOffset(10)}, func(ds *DocumentSelector) bool {
			return ds.offset == 10
		}},
		{"from set sort", NewDocumentSelector(WithSort(elastic.NewFieldSort("test2"))), []Selector{WithSort(elastic.NewFieldSort("test"))}, func(ds *DocumentSelector) bool {
			return assert.Equal(t, elastic.NewFieldSort("test"), ds.sort)
		}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			tt.ds.Update(tt.selectors...)
			assert.True(t, tt.validate(tt.ds))
		})
	}
}

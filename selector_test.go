package reveald

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/stretchr/testify/assert"
)

func Test_NewDocumentSelector(t *testing.T) {
	t.Run("default page size", func(t *testing.T) {
		ds := NewDocumentSelector()
		assert.Equal(t, defaultPageSize, ds.pageSize)
	})

	t.Run("default offset", func(t *testing.T) {
		ds := NewDocumentSelector()
		assert.Equal(t, 0, ds.offset)
	})

	t.Run("default sort", func(t *testing.T) {
		ds := NewDocumentSelector()
		assert.Nil(t, ds.sort)
	})

	t.Run("set page size", func(t *testing.T) {
		ds := NewDocumentSelector(WithPageSize(10))
		assert.Equal(t, 10, ds.pageSize)
	})

	t.Run("set offset", func(t *testing.T) {
		ds := NewDocumentSelector(WithOffset(10))
		assert.Equal(t, 10, ds.offset)
	})

	t.Run("set sort", func(t *testing.T) {
		ds := NewDocumentSelector(WithSort("test", sortorder.Asc))
		assert.NotNil(t, ds.sort)
	})
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
		{"from default sort", NewDocumentSelector(), []Selector{WithSort("test", sortorder.Asc)}, func(ds *DocumentSelector) bool {
			if ds.sort == nil {
				return false
			}
			field := ds.sort[0].(types.SortOptions)
			options, optionsOk := field.SortOptions["test"]
			if !optionsOk {
				return false
			}
			return options.Order.Name == "asc"
		}},
		{"from set page size", NewDocumentSelector(WithPageSize(20)), []Selector{WithPageSize(10)}, func(ds *DocumentSelector) bool {
			return ds.pageSize == 10
		}},
		{"from set offset", NewDocumentSelector(WithOffset(20)), []Selector{WithOffset(10)}, func(ds *DocumentSelector) bool {
			return ds.offset == 10
		}},
		{"from set sort", NewDocumentSelector(WithSort("test2", sortorder.Asc)), []Selector{WithSort("test", sortorder.Asc)}, func(ds *DocumentSelector) bool {
			if ds.sort == nil {
				return false
			}
			field := ds.sort[0].(types.SortOptions)
			return field.SortOptions["test2"].Order.Name == "asc"
		}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			tt.ds.Update(tt.selectors...)
			assert.True(t, tt.validate(tt.ds))
		})
	}
}

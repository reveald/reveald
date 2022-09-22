package featureset

import (
	"github.com/olivere/elastic/v7"
	"github.com/reveald/reveald"
)

type sortingOption struct {
	property  string
	ascending bool
}

type SortingFeature struct {
	param         string
	options       map[string]sortingOption
	defaultOption string
}

type SortingOption func(*SortingFeature)

func WithSortOption(name, property string, ascending bool) SortingOption {
	return func(sf *SortingFeature) {
		sf.options[name] = sortingOption{
			property,
			ascending,
		}
	}
}

func WithDefaultSortOption(name string) SortingOption {
	return func(sf *SortingFeature) {
		sf.defaultOption = name
	}
}

func NewSortingFeature(param string, opts ...SortingOption) *SortingFeature {
	sf := &SortingFeature{
		param:   param,
		options: make(map[string]sortingOption),
	}

	for _, opt := range opts {
		opt(sf)
	}

	return sf
}

func (sf *SortingFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	sf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return sf.handle(builder.Request(), r)
}

func (sf *SortingFeature) build(builder *reveald.QueryBuilder) {
	key := sf.defaultOption

	if builder.Request().Has(sf.param) {
		v, err := builder.Request().Get(sf.param)
		if err != nil {
			return
		}

		key = v.Value()
	}

	if key == "" {
		return
	}

	option, ok := sf.options[key]
	if !ok {
		return
	}

	sort := elastic.NewFieldSort(option.property)
	if option.ascending {
		sort = sort.Asc()
	}
	if !option.ascending {
		sort = sort.Desc()
	}

	builder.Selection().Update(reveald.WithSort(sort))
}

func (sf *SortingFeature) handle(req *reveald.Request, result *reveald.Result) (*reveald.Result, error) {
	var options []*reveald.ResultSortingOption

	selected := sf.defaultOption
	if req.Has(sf.param) {
		v, err := req.Get(sf.param)
		if err == nil {
			selected = v.Value()
		}
	}

	for k, v := range sf.options {
		options = append(options, &reveald.ResultSortingOption{
			Name:      k,
			Property:  v.property,
			Ascending: v.ascending,
			Selected:  selected == k,
		})
	}

	result.Sorting = &reveald.ResultSorting{
		Param:   sf.param,
		Options: options,
	}

	return result, nil
}

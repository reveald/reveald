package featureset

import (
	"github.com/reveald/reveald"
)

// sortingOption represents a single sort option with a property name and sort direction.
type sortingOption struct {
	property  string
	ascending bool
}

// SortingFeature handles sorting for Elasticsearch queries.
//
// It processes sorting parameters from the request and applies them to the query,
// as well as adding sorting information to the result.
//
// Example:
//
//	// Create a sorting feature for the "sort" parameter
//	sortingFeature := featureset.NewSortingFeature("sort",
//	    featureset.WithSortOption("price_asc", "price", true),
//	    featureset.WithSortOption("price_desc", "price", false),
//	    featureset.WithDefaultSortOption("price_desc"),
//	)
//
//	// Use the sorting feature in a feature chain
//	result, err := sortingFeature.Process(builder, nextFeature)
type SortingFeature struct {
	param         string
	options       map[string]sortingOption
	defaultOption string
}

// SortingOption is a functional option for configuring a SortingFeature.
type SortingOption func(*SortingFeature)

// WithSortOption adds a sort option to the sorting feature.
//
// Parameters:
//   - name: The name of the sort option (used in the request parameter)
//   - property: The field to sort on
//   - ascending: Whether to sort in ascending order (true) or descending order (false)
//
// Example:
//
//	// Add sort options for price
//	sortingFeature := featureset.NewSortingFeature("sort",
//	    featureset.WithSortOption("price_asc", "price", true),
//	    featureset.WithSortOption("price_desc", "price", false),
//	)
func WithSortOption(name, property string, ascending bool) SortingOption {
	return func(sf *SortingFeature) {
		sf.options[name] = sortingOption{
			property,
			ascending,
		}
	}
}

// WithDefaultSortOption sets the default sort option to use when no sort parameter is provided.
//
// Example:
//
//	// Set the default sort option to price descending
//	sortingFeature := featureset.NewSortingFeature("sort",
//	    featureset.WithSortOption("price_asc", "price", true),
//	    featureset.WithSortOption("price_desc", "price", false),
//	    featureset.WithDefaultSortOption("price_desc"),
//	)
func WithDefaultSortOption(name string) SortingOption {
	return func(sf *SortingFeature) {
		sf.defaultOption = name
	}
}

// NewSortingFeature creates a new sorting feature with the specified parameter name and options.
//
// The parameter name is the request parameter that will be used to specify the sort option.
//
// Example:
//
//	// Create a sorting feature with multiple options
//	sortingFeature := featureset.NewSortingFeature("sort",
//	    featureset.WithSortOption("price_asc", "price", true),
//	    featureset.WithSortOption("price_desc", "price", false),
//	    featureset.WithSortOption("name_asc", "name.keyword", true),
//	    featureset.WithDefaultSortOption("price_desc"),
//	)
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

// Process applies sorting to the query builder and processes the result.
//
// It extracts sorting parameters from the request, applies them to the query,
// and adds sorting information to the result.
//
// Example:
//
//	// Use the sorting feature in a feature chain
//	result, err := sortingFeature.Process(builder, nextFeature)
func (sf *SortingFeature) Process(builder *reveald.QueryBuilder, next reveald.FeatureFunc) (*reveald.Result, error) {
	sf.build(builder)

	r, err := next(builder)
	if err != nil {
		return nil, err
	}

	return sf.handle(builder.Request(), r)
}

// build applies sorting settings to the query builder.
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

	order := "asc"
	if !option.ascending {
		order = "desc"
	}

	builder.Selection().Update(reveald.WithSort(option.property, order))
}

// handle adds sorting information to the result.
func (sf *SortingFeature) handle(req *reveald.Request, result *reveald.Result) (*reveald.Result, error) {
	var options []*reveald.ResultSortingOption
	selectedOption := sf.defaultOption

	// Read the selected option from request
	if req.Has(sf.param) {
		p, err := req.Get(sf.param)
		if err == nil {
			selectedOption = p.Value()
		}
	}

	for k := range sf.options {
		option := &reveald.ResultSortingOption{
			Label: k,
			Value: k, // Use the option key as the value
		}

		// Mark as selected if it matches the selected option
		if k == selectedOption {
			// Note: The ResultSortingOption struct doesn't have a Selected field in the current code.
			// We'll rely on having the correct Value to identify it in tests
		}

		options = append(options, option)
	}

	result.Sorting = &reveald.ResultSorting{
		Param:   sf.param,
		Options: options,
	}

	return result, nil
}

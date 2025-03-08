package reveald

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// RangeMinParameterName is the default prefix for a minimum range bound
	RangeMinParameterName string = "min"
	// RangeMaxParameterName is the default prefix for a maximum range bound
	RangeMaxParameterName string = "max"
)

// Parameter is used for filtering documents in a search query request.
//
// It defines a document property name along with any possible values.
// Parameters can represent simple values, boolean values, or range values.
//
// Example:
//
//	// Create a simple parameter
//	param := reveald.NewParameter("category", "electronics")
//
//	// Create a boolean parameter
//	param := reveald.NewParameter("active", "true")
//
//	// Create range parameters
//	minParam := reveald.NewParameter("price.min", "50")
//	maxParam := reveald.NewParameter("price.max", "100")
type Parameter struct {
	name   string
	values []string
	min    float64
	max    float64
	wmin   bool
	wmax   bool
}

// NewParameter creates a Parameter based on the specified function arguments.
//
// It includes logic for handling special cases such as range query parameters.
// If the parameter name has a ".min" or ".max" suffix, it will be treated as a range parameter.
//
// Example:
//
//	// Create a simple parameter
//	param := reveald.NewParameter("category", "electronics")
//
//	// Create a range parameter
//	minParam := reveald.NewParameter("price.min", "50")
//	// The actual name will be "price" and min will be 50
func NewParameter(name string, values ...string) Parameter {
	pv := Parameter{}
	pv.name = name
	pv.values = values

	var err error

	for _, v := range values {
		if strings.HasSuffix(name, "."+RangeMinParameterName) {
			pv.min, err = strconv.ParseFloat(v, 64)
			pv.wmin = err == nil
			pv.name = name[:len(name)-len("."+RangeMinParameterName)]
		}
		if strings.HasSuffix(name, "."+RangeMaxParameterName) {
			pv.max, err = strconv.ParseFloat(v, 64)
			pv.wmax = err == nil
			pv.name = name[:len(name)-len("."+RangeMaxParameterName)]
		}
	}

	return pv
}

// IsRangeValue returns true when the parameter includes at least one range suffix (min/max).
//
// Example:
//
//	param := reveald.NewParameter("price.min", "50")
//	if param.IsRangeValue() {
//	    // Handle as a range parameter
//	}
func (pv Parameter) IsRangeValue() bool {
	return pv.wmin || pv.wmax
}

// IsTruthy returns true for a boolean value that is true.
//
// This is useful for checking if a parameter represents a boolean true value.
//
// Example:
//
//	param := reveald.NewParameter("active", "true")
//	if param.IsTruthy() {
//	    // Handle active items
//	}
func (pv Parameter) IsTruthy() bool {
	value := pv.Value()
	if value == "" {
		return false
	}

	b, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}

	return b
}

// Min returns the lower range bound for a range parameter.
//
// It returns the minimum value and a boolean indicating if a minimum value is set.
//
// Example:
//
//	param := reveald.NewParameter("price.min", "50")
//	min, hasMin := param.Min()
//	if hasMin {
//	    fmt.Printf("Minimum price: %f\n", min)
//	}
func (pv Parameter) Min() (float64, bool) {
	return pv.min, pv.wmin
}

// Max returns the higher range bound for a range parameter.
//
// It returns the maximum value and a boolean indicating if a maximum value is set.
//
// Example:
//
//	param := reveald.NewParameter("price.max", "100")
//	max, hasMax := param.Max()
//	if hasMax {
//	    fmt.Printf("Maximum price: %f\n", max)
//	}
func (pv Parameter) Max() (float64, bool) {
	return pv.max, pv.wmax
}

// Merge combines a parameter with another parameter.
//
// This is useful when you have multiple parameters with the same name
// that need to be combined.
//
// Example:
//
//	param1 := reveald.NewParameter("price.min", "50")
//	param2 := reveald.NewParameter("price.max", "100")
//	merged := param1.Merge(param2)
//	// merged will have both min and max values
func (pv Parameter) Merge(m Parameter) Parameter {
	pv.values = append(pv.values, m.values...)

	if pv.min == 0 && m.wmin {
		pv.min = m.min
		pv.wmin = true
	}
	if pv.max == 0 && m.wmax {
		pv.max = m.max
		pv.wmax = true
	}

	return pv
}

// Name returns the parameter name.
//
// Example:
//
//	param := reveald.NewParameter("category", "electronics")
//	fmt.Printf("Parameter name: %s\n", param.Name())
func (pv Parameter) Name() string {
	return pv.name
}

// Value returns the first value for a parameter.
//
// This is a convenience method for accessing the first value when
// you expect a parameter to have only one value.
//
// Example:
//
//	param := reveald.NewParameter("category", "electronics")
//	fmt.Printf("Category: %s\n", param.Value())
func (pv Parameter) Value() string {
	if len(pv.values) == 0 {
		return ""
	}

	return pv.values[len(pv.values)-1]
}

// Values returns all values for a parameter.
//
// Example:
//
//	param := reveald.NewParameter("tags", "premium", "featured")
//	fmt.Printf("Tags: %v\n", param.Values())
func (pv Parameter) Values() []string {
	return pv.values
}

// Request is a container for parameters used in a search query.
//
// It manages a collection of parameters that can be used to filter documents.
//
// Example:
//
//	// Create a new request
//	request := reveald.NewRequest(
//	    reveald.NewParameter("category", "electronics"),
//	    reveald.NewParameter("active", "true"),
//	    reveald.NewParameter("price.min", "50"),
//	)
type Request struct {
	params map[string]Parameter
}

// NewRequest creates a new request with the specified parameters.
//
// Example:
//
//	// Create a request with multiple parameters
//	request := reveald.NewRequest(
//	    reveald.NewParameter("category", "electronics"),
//	    reveald.NewParameter("active", "true"),
//	)
func NewRequest(params ...Parameter) *Request {
	q := &Request{
		params: make(map[string]Parameter),
	}

	for _, p := range params {
		q = q.Append(p)
	}

	return q
}

// Append adds a parameter to the request.
//
// If a parameter with the same name already exists, the values are merged.
//
// Example:
//
//	// Add a parameter to an existing request
//	request.Append(reveald.NewParameter("tags", "premium"))
func (q *Request) Append(param Parameter) *Request {
	if _, ok := q.params[param.name]; ok {
		param = param.Merge(q.params[param.name])
	}

	q.params[param.name] = param
	return q
}

// Has checks if a parameter with the specified name exists in the request.
//
// Example:
//
//	// Check if a parameter exists
//	if request.Has("category") {
//	    // Handle category parameter
//	}
func (q *Request) Has(name string) bool {
	_, ok := q.params[name]
	return ok
}

// HasParam checks if a parameter exists in the request.
//
// Example:
//
//	param := reveald.NewParameter("category", "electronics")
//	if request.HasParam(param) {
//	    // Handle category parameter
//	}
func (q *Request) HasParam(param Parameter) bool {
	_, ok := q.params[param.name]
	return ok
}

// Get retrieves a parameter by name.
//
// Example:
//
//	// Get a parameter and check its value
//	param, err := request.Get("category")
//	if err == nil {
//	    fmt.Printf("Category: %s\n", param.Value())
//	}
func (q *Request) Get(name string) (Parameter, error) {
	p, ok := q.params[name]
	if !ok {
		return Parameter{}, fmt.Errorf("no such parameter: %s", name)
	}

	return p, nil
}

// GetAll returns all parameters in the request.
//
// Example:
//
//	// Get all parameters
//	allParams := request.GetAll()
//	for name, param := range allParams {
//	    fmt.Printf("%s: %v\n", name, param.Values())
//	}
func (q *Request) GetAll() map[string]Parameter {
	return q.params
}

// Set adds or replaces a parameter with the specified name and values.
//
// Example:
//
//	// Set a parameter
//	request.Set("category", "electronics")
//
//	// Set a parameter with multiple values
//	request.Set("tags", "premium", "featured")
func (q *Request) Set(name string, values ...string) {
	q.params[name] = NewParameter(name, values...)
}

// SetParam adds or replaces a parameter.
//
// Example:
//
//	// Set a parameter
//	param := reveald.NewParameter("category", "electronics")
//	request.SetParam(param)
func (q *Request) SetParam(param Parameter) {
	q.params[param.name] = param
}

// Del removes a parameter by name.
//
// Example:
//
//	// Remove a parameter
//	request.Del("category")
func (q *Request) Del(name string) {
	delete(q.params, name)
}

// DelParam removes a parameter.
//
// Example:
//
//	// Remove a parameter
//	param := reveald.NewParameter("category", "electronics")
//	request.DelParam(param)
func (q *Request) DelParam(param Parameter) {
	delete(q.params, param.name)
}

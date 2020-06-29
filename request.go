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

// Parameter is used for filtering documents
// in a search query request, defining a document
// property name along with any possible values
type Parameter struct {
	name   string
	values []string
	min    float64
	max    float64
	wmin   bool
	wmax   bool
}

// NewParameter creates a Parameter based on the
// specified function arguments, including some logic
// for handling special cases such as range query
// parameters
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

// IsRangeValue returns truthy when the parameter
// name includes at least one range suffix (min/max)
func (pv Parameter) IsRangeValue() bool {
	return pv.wmin || pv.wmax
}

// IsTruthy returns truthy for a boolean value
// for the parameter, and that boolean is true
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

// Min returns the lower range bound for a range parameter
func (pv Parameter) Min() (float64, bool) {
	return pv.min, pv.wmin
}

// Max returns the higher range bound for a range parameter
func (pv Parameter) Max() (float64, bool) {
	return pv.max, pv.wmax
}

// Merge a parameter with another parameter
func (pv Parameter) Merge(m Parameter) Parameter {
	pv.values = append(pv.values, m.values...)

	if pv.min == 0 && m.min > 0 {
		pv.min = m.min
		pv.wmin = true
	}
	if pv.max == 0 && m.max > 0 {
		pv.max = m.max
		pv.wmax = true
	}

	return pv
}

// Name returns the parameter name
func (pv Parameter) Name() string {
	return pv.name
}

// Value returns the first specified value
func (pv Parameter) Value() string {
	if len(pv.values) == 0 {
		return ""
	}

	return pv.values[len(pv.values)-1]
}

// Values returns all values for the parameter
func (pv Parameter) Values() []string {
	return pv.values
}

// Request is a set of Parameter
type Request struct {
	params map[string]Parameter
}

// NewRequest create a new typed set of the
// specified parameters
func NewRequest(params ...Parameter) *Request {
	q := &Request{
		params: make(map[string]Parameter),
	}

	for _, p := range params {
		q = q.Append(p)
	}

	return q
}

// Append a parameter to the search request
func (q *Request) Append(param Parameter) *Request {
	if _, ok := q.params[param.name]; ok {
		param = param.Merge(q.params[param.name])
	}

	q.params[param.name] = param
	return q
}

// Has returns truthy when a parameter with the
// specified name exist on the request
func (q *Request) Has(name string) bool {
	_, ok := q.params[name]
	return ok
}

// HasParam returns truthy when a parameter with
// the same name as the specified parameter exist
// on the request
func (q *Request) HasParam(param Parameter) bool {
	_, ok := q.params[param.name]
	return ok
}

// Get returns the parameter with the specified name,
// or an error if no such parameter exist
func (q *Request) Get(name string) (Parameter, error) {
	p, ok := q.params[name]
	if !ok {
		return Parameter{}, fmt.Errorf("no such parameter: %s", name)
	}

	return p, nil
}

// GetAll returns all parameters as a map
func (q *Request) GetAll() map[string]Parameter {
	return q.params
}

// Set creates or replaces an existing parameter,
// with the specified name and values
func (q *Request) Set(name string, values ...string) {
	q.params[name] = NewParameter(name, values...)
}

// SetParam creates or replaces an existing parameter
func (q *Request) SetParam(param Parameter) {
	q.params[param.name] = param
}

// Del removes a parameter with the specified name,
// if it exist
func (q *Request) Del(name string) {
	delete(q.params, name)
}

// DelParam removes a parameter if it exist
func (q *Request) DelParam(param Parameter) {
	delete(q.params, param.name)
}

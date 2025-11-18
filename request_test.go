package reveald

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewParameter(t *testing.T) {
	table := []struct {
		name   string
		min    bool
		max    bool
		values []string
	}{
		{"param", false, false, []string{}},
		{"param", false, false, []string{"value1", "value2"}},
		{"param", true, false, []string{"1"}},
		{"param", false, true, []string{"1"}},
	}

	for _, tt := range table {
		suf := ""
		if tt.min {
			suf = "." + RangeMinParameterName
		} else if tt.max {
			suf = "." + RangeMaxParameterName
		}

		p := NewParameter(tt.name+suf, tt.values...)
		name := fmt.Sprintf("%s%s: %s", tt.name, suf, strings.Join(tt.values, ", "))
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.name, p.Name())
			assert.Equal(t, tt.values, p.Values())
		})
	}
}

func Test_IsRangeValue(t *testing.T) {
	table := []struct {
		param  Parameter
		result bool
	}{
		{NewParameter("param", "1"), false},
		{NewParameter("param."+RangeMinParameterName, "1"), true},
		{NewParameter("param."+RangeMaxParameterName, "1"), true},
		{NewParameter("param."+RangeMinParameterName, "random-string"), true},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%s: %s", tt.param.Name(), tt.param.Value())
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.result, tt.param.IsRangeValue())
		})
	}
}

func Test_Min(t *testing.T) {
	table := []struct {
		param   Parameter
		succeed bool
		result  float64
	}{
		{NewParameter("param."+RangeMinParameterName, fmt.Sprintf("%f", 10.0)), true, 10.0},
		{NewParameter("param."+RangeMinParameterName, "random-string"), false, 0.0},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%s: %s", tt.param.Name(), tt.param.Value())
		t.Run(name, func(t *testing.T) {
			v, ok := tt.param.Min()
			assert.Equal(t, tt.succeed, ok)

			if tt.succeed {
				assert.Equal(t, tt.result, v)
			}
		})
	}
}

func Test_Max(t *testing.T) {
	table := []struct {
		param   Parameter
		succeed bool
		result  float64
	}{
		{NewParameter("param."+RangeMaxParameterName, fmt.Sprintf("%f", 10.0)), true, 10.0},
		{NewParameter("param."+RangeMaxParameterName, "random-string"), false, 0.0},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%s: %s", tt.param.Name(), tt.param.Value())
		t.Run(name, func(t *testing.T) {
			v, ok := tt.param.Max()
			assert.Equal(t, tt.succeed, ok)

			if tt.succeed {
				assert.Equal(t, tt.result, v)
			}
		})
	}
}

func Test_IsTruthy(t *testing.T) {
	table := []struct {
		param  Parameter
		result bool
	}{
		{NewParameter("param", "true"), true},
		{NewParameter("param", "false"), false},
		{NewParameter("param", "random-string"), false},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%s: %s", tt.param.Name(), tt.param.Value())
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.result, tt.param.IsTruthy())
		})
	}
}

func Test_Merge_Values(t *testing.T) {
	v1 := []string{"value1", "value2"}
	p1 := NewParameter("p1", v1...)

	v2 := []string{"value3", "value4", "value5"}
	p2 := NewParameter("p2", v2...)

	p := p1.Merge(p2)

	expected := append(v1, v2...)
	actual := p.Values()

	assert.Equal(t, expected, actual)
}

func Test_Merge_Ranges(t *testing.T) {
	table := []struct {
		name string
		min  float64
		max  float64
	}{
		{
			name: "Positive values",
			min:  2.0,
			max:  10.0,
		},
		{
			name: "Negative values",
			min:  -10.0,
			max:  -2.0,
		},
		{
			name: "Min and max zero",
			min:  0.0,
			max:  0.0,
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			p1 := NewParameter("p1."+RangeMinParameterName, fmt.Sprintf("%f", tt.min))
			p2 := NewParameter("p1."+RangeMaxParameterName, fmt.Sprintf("%f", tt.max))

			merged := p1.Merge(p2)
			mergedReversed := p2.Merge(p1)

			// Check that merging order doesn't affect results
			minVal1, minSet1 := merged.Min()
			maxVal1, maxSet1 := merged.Max()
			minVal2, minSet2 := mergedReversed.Min()
			maxVal2, maxSet2 := mergedReversed.Max()

			assert.Equal(t, minVal1, minVal2, "Min values should be equal regardless of merge order")
			assert.Equal(t, maxVal1, maxVal2, "Max values should be equal regardless of merge order")
			assert.Equal(t, minSet1, minSet2, "Min set flags should match regardless of merge order")
			assert.Equal(t, maxSet1, maxSet2, "Max set flags should match regardless of merge order")

			// Check against expected test values
			assert.Equal(t, tt.min, minVal1, "Min value does not match expected value")
			assert.Equal(t, tt.max, maxVal1, "Max value does not match expected value")
			assert.True(t, minSet1, "Min should be set")
			assert.True(t, maxSet1, "Max should be set")
		})
	}

}

func Test_NewRequest(t *testing.T) {
	table := []struct {
		name       string
		parameters []Parameter
		validate   func(*Request) bool
	}{
		{"sets params", []Parameter{NewParameter("param", "value")}, func(r *Request) bool {
			v, ok := r.params["param"]
			if !ok {
				return false
			}
			return v.Value() == "value"
		}},
		{"merges params", []Parameter{NewParameter("param", "value1"), NewParameter("param", "value2")}, func(r *Request) bool {
			v, ok := r.params["param"]
			if !ok {
				return false
			}
			return assert.ElementsMatch(t, v.Values(), []string{"value1", "value2"})
		}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRequest(tt.parameters...)
			valid := tt.validate(r)
			assert.True(t, valid)
		})
	}
}

func Test_Append(t *testing.T) {
	r := NewRequest(NewParameter("param1", "value1"))
	r.Append(NewParameter("param1", "value2"))
	r.Append(NewParameter("param2", "value3"))

	assert.ElementsMatch(t, r.params["param1"].Values(), []string{"value1", "value2"})
	assert.Equal(t, r.params["param2"].Value(), "value3")
}

func Test_Has(t *testing.T) {
	table := []struct {
		req    *Request
		param  string
		result bool
	}{
		{NewRequest(NewParameter("param", "value")), "param", true},
		{NewRequest(NewParameter("param."+RangeMinParameterName, "1")), "param", true},
		{NewRequest(NewParameter("param."+RangeMaxParameterName, "1")), "param", true},
		{NewRequest(NewParameter("param", "value")), "random-string", false},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v / %s", tt.req.params, tt.param)
		t.Run(name, func(t *testing.T) {
			actual := tt.req.Has(tt.param)
			assert.Equal(t, tt.result, actual)
		})
	}
}

func Test_HasParam(t *testing.T) {
	table := []struct {
		req    *Request
		param  Parameter
		result bool
	}{
		{NewRequest(NewParameter("param", "value")), NewParameter("param"), true},
		{NewRequest(NewParameter("param."+RangeMinParameterName, "1")), NewParameter("param"), true},
		{NewRequest(NewParameter("param."+RangeMaxParameterName, "1")), NewParameter("param"), true},
		{NewRequest(NewParameter("param", "value")), NewParameter("random-string"), false},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v / %s", tt.req.params, tt.param.Name())
		t.Run(name, func(t *testing.T) {
			actual := tt.req.HasParam(tt.param)
			assert.Equal(t, tt.result, actual)
		})
	}
}

func Test_Get(t *testing.T) {
	table := []struct {
		req    *Request
		name   string
		values []string
		err    bool
	}{
		{NewRequest(NewParameter("param", "value")), "param", []string{"value"}, false},
		{NewRequest(NewParameter("param", "value1", "value2")), "param", []string{"value1", "value2"}, false},
		{NewRequest(NewParameter("param", "value")), "random-string", nil, true},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v / %s", tt.req.params, tt.name)
		t.Run(name, func(t *testing.T) {
			v, err := tt.req.Get(tt.name)
			if !tt.err {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.values, v.Values())
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_GetAll(t *testing.T) {
	table := []struct {
		req    *Request
		values []string
	}{
		{NewRequest(NewParameter("param", "value")), []string{"value"}},
		{NewRequest(NewParameter("param", "value1", "value2")), []string{"value1", "value2"}},
		{NewRequest(NewParameter("param1", "value1"), NewParameter("param2", "value2")), []string{"value1", "value2"}},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v", tt.req.params)
		t.Run(name, func(t *testing.T) {
			var values []string
			params := tt.req.GetAll()
			for _, p := range params {
				values = append(values, p.Values()...)
			}

			assert.ElementsMatch(t, tt.values, values)
		})
	}
}

func Test_Set(t *testing.T) {
	table := []struct {
		params []Parameter
		values []string
	}{
		{[]Parameter{NewParameter("param1", "value1")}, []string{"value1"}},
		{[]Parameter{NewParameter("param1", "value1"), NewParameter("param2", "value2")}, []string{"value1", "value2"}},
		{[]Parameter{NewParameter("param1", "value1"), NewParameter("param1", "value2")}, []string{"value2"}},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v", tt.params)
		t.Run(name, func(t *testing.T) {
			r := NewRequest()
			for _, p := range tt.params {
				r.Set(p.Name(), p.Values()...)
			}

			var values []string
			for _, v := range r.params {
				values = append(values, v.Values()...)
			}

			assert.ElementsMatch(t, tt.values, values)
		})
	}
}

func Test_SetParam(t *testing.T) {
	table := []struct {
		params []Parameter
		values []string
	}{
		{[]Parameter{NewParameter("param1", "value1")}, []string{"value1"}},
		{[]Parameter{NewParameter("param1", "value1"), NewParameter("param2", "value2")}, []string{"value1", "value2"}},
		{[]Parameter{NewParameter("param1", "value1"), NewParameter("param1", "value2")}, []string{"value2"}},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v", tt.params)
		t.Run(name, func(t *testing.T) {
			r := NewRequest()
			for _, p := range tt.params {
				r.SetParam(p)
			}

			var values []string
			for _, v := range r.params {
				values = append(values, v.Values()...)
			}

			assert.ElementsMatch(t, tt.values, values)
		})
	}
}

func Test_Del(t *testing.T) {
	table := []struct {
		req    *Request
		param  string
		values []string
	}{
		{NewRequest(NewParameter("param", "value")), "param", []string{}},
		{NewRequest(NewParameter("param1", "value1"), NewParameter("param2", "value2")), "param1", []string{"value2"}},
		{NewRequest(NewParameter("param", "value")), "random-string", []string{"value"}},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v / %s", tt.req.params, tt.param)
		t.Run(name, func(t *testing.T) {
			tt.req.Del(tt.param)

			var values []string
			for _, v := range tt.req.params {
				values = append(values, v.Values()...)
			}

			assert.ElementsMatch(t, tt.values, values)
		})
	}
}

func Test_DelParam(t *testing.T) {
	table := []struct {
		req    *Request
		param  Parameter
		values []string
	}{
		{NewRequest(NewParameter("param", "value")), NewParameter("param"), []string{}},
		{NewRequest(NewParameter("param1", "value1"), NewParameter("param2", "value2")), NewParameter("param1"), []string{"value2"}},
		{NewRequest(NewParameter("param", "value")), NewParameter("random-string"), []string{"value"}},
	}

	for _, tt := range table {
		name := fmt.Sprintf("%v / %s", tt.req.params, tt.param.Name())
		t.Run(name, func(t *testing.T) {
			tt.req.DelParam(tt.param)

			var values []string
			for _, v := range tt.req.params {
				values = append(values, v.Values()...)
			}

			assert.ElementsMatch(t, tt.values, values)
		})
	}
}

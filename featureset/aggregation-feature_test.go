package featureset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildAggregationFeature(t *testing.T) {
	table := []struct {
		name                 string
		options              []AggregationOption
		expectedSize         int
		expectedMissingValue string
	}{
		{
			name:                 "default options",
			options:              []AggregationOption{},
			expectedSize:         defaultAggregationSize,
			expectedMissingValue: "",
		},
		{
			name:                 "custom size",
			options:              []AggregationOption{WithAggregationSize(20)},
			expectedSize:         20,
			expectedMissingValue: "",
		},
		{
			name:                 "include missing with custom label",
			options:              []AggregationOption{WithMissingValueAs("no-value")},
			expectedSize:         defaultAggregationSize,
			expectedMissingValue: "no-value",
		},
		{
			name:                 "include missing with empty string",
			options:              []AggregationOption{WithMissingValueAs("")},
			expectedSize:         defaultAggregationSize,
			expectedMissingValue: "",
		},
		{
			name: "custom size and include missing",
			options: []AggregationOption{
				WithAggregationSize(50),
				WithMissingValueAs("missing-data"),
			},
			expectedSize:         50,
			expectedMissingValue: "missing-data",
		},
		{
			name: "multiple options in different order",
			options: []AggregationOption{
				WithMissingValueAs("unknown"),
				WithAggregationSize(100),
			},
			expectedSize:         100,
			expectedMissingValue: "unknown",
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			agg := buildAggregationFeature(tt.options...)
			assert.Equal(t, tt.expectedSize, agg.size)
			assert.Equal(t, tt.expectedMissingValue, agg.missingValue)
		})
	}
}

func TestWithAggregationSize(t *testing.T) {
	table := []struct {
		name         string
		size         int
		expectedSize int
	}{
		{
			name:         "size 1",
			size:         1,
			expectedSize: 1,
		},
		{
			name:         "size 10",
			size:         10,
			expectedSize: 10,
		},
		{
			name:         "size 100",
			size:         100,
			expectedSize: 100,
		},
		{
			name:         "size 1000",
			size:         1000,
			expectedSize: 1000,
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			agg := AggregationFeature{}
			opt := WithAggregationSize(tt.size)
			opt(&agg)
			assert.Equal(t, tt.expectedSize, agg.size)
		})
	}
}

func TestWithMissingValueAs(t *testing.T) {
	table := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "custom missing label",
			value:    "no-data",
			expected: "no-data",
		},
		{
			name:     "another custom label",
			value:    "unknown",
			expected: "unknown",
		},
		{
			name:     "empty string",
			value:    "",
			expected: "",
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			agg := AggregationFeature{}
			opt := WithMissingValueAs(tt.value)
			opt(&agg)
			assert.Equal(t, tt.expected, agg.missingValue)
		})
	}
}

func TestAggregationFeatureDefaults(t *testing.T) {
	// Test that buildAggregationFeature without options returns correct defaults
	agg := buildAggregationFeature()
	assert.Equal(t, defaultAggregationSize, agg.size)
	assert.Empty(t, agg.missingValue)
}

func TestAggregationFeatureOptionComposition(t *testing.T) {
	// Test that options can be applied in sequence and last one wins for same property
	agg := AggregationFeature{}

	// Apply size twice, last one should win
	WithAggregationSize(10)(&agg)
	assert.Equal(t, 10, agg.size)

	WithAggregationSize(20)(&agg)
	assert.Equal(t, 20, agg.size)

	// Apply missingValue twice, last one should win
	WithMissingValueAs("first-label")(&agg)
	assert.Equal(t, "first-label", agg.missingValue)

	WithMissingValueAs("second-label")(&agg)
	assert.Equal(t, "second-label", agg.missingValue)
}

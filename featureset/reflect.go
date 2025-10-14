package featureset

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/reveald/reveald"
)

// tagOptions represents parsed options from a reveald struct tag
type tagOptions struct {
	ignore           bool
	noSort           bool
	dynamic          bool
	histogram        bool
	histogramInterval float64
	dateHistogram    bool
	dateHistogramInterval DateHistogramInterval
}

// parseTagOptions parses a reveald tag into structured options
// Supports formats like: "ignore", "dynamic,no-sort", "histogram,interval=100", "date-histogram,interval=day"
func parseTagOptions(tag string) tagOptions {
	opts := tagOptions{
		histogramInterval: 100, // default
		dateHistogramInterval: Day, // default
	}

	if tag == "" {
		return opts
	}

	parts := strings.Split(tag, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check for key=value pairs
		if strings.Contains(part, "=") {
			kv := strings.SplitN(part, "=", 2)
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])

			switch key {
			case "interval":
				if opts.histogram {
					if interval, err := strconv.ParseFloat(value, 64); err == nil {
						opts.histogramInterval = interval
					}
				} else if opts.dateHistogram {
					opts.dateHistogramInterval = DateHistogramInterval(value)
				}
			}
		} else {
			// Simple flags
			switch part {
			case "ignore":
				opts.ignore = true
			case "no-sort":
				opts.noSort = true
			case "dynamic":
				opts.dynamic = true
			case "histogram":
				opts.histogram = true
			case "date-histogram":
				opts.dateHistogram = true
			}
		}
	}

	return opts
}

func Reflect(t reflect.Type) []reveald.Feature {

	sortOpts := make([]SortingOption, 0)
	featureOpts := make([]reveald.Feature, 0)

	for _, f := range reflect.VisibleFields(t) {
		rtag := f.Tag.Get("reveald")
		opts := parseTagOptions(rtag)

		if opts.ignore {
			continue
		}

		fieldName := f.Name
		jsonTag := f.Tag.Get("json")
		if jsonTag != "" {
			fieldName = strings.Split(jsonTag, ",")[0]
		}

		// Handle histogram features for numeric types
		if opts.histogram {
			switch f.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Float32, reflect.Float64:
				featureOpts = append(featureOpts, NewHistogramFeature(f.Name, WithInterval(opts.histogramInterval)))
			}
		}

		// Handle date histogram features for time.Time
		if opts.dateHistogram && f.Type == reflect.TypeOf(time.Time{}) {
			featureOpts = append(featureOpts, NewDateHistogramFeature(f.Name, opts.dateHistogramInterval))
		}

		// Add default features for types
		switch f.Type.Kind() {
		case reflect.String:
			fieldName += ".keyword"

		case reflect.Bool:
			featureOpts = append(featureOpts, NewDynamicBooleanFilterFeature(f.Name))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if !opts.histogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
			}
		case reflect.Float32, reflect.Float64:
			if !opts.histogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
			}
		case reflect.TypeOf(time.Time{}).Kind():
			if !opts.dateHistogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
			}
		}

		// Add sorting options
		if !opts.noSort {
			sortOpts = append(sortOpts, WithSortOption(f.Name+"-desc", fieldName, false))
			sortOpts = append(sortOpts, WithSortOption(f.Name+"-asc", fieldName, true))
		}

		// Handle dynamic tag
		if opts.dynamic {
			switch f.Type.Kind() {
			case reflect.String:
				fieldName += ".keyword"
				featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if !opts.histogram {
					featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
				}
			case reflect.Float32, reflect.Float64:
				if !opts.histogram {
					featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
				}
			}
		}
	}
	if len(sortOpts) > 0 {
		featureOpts = append(featureOpts, NewSortingFeature("sort", sortOpts...))
	}
	return featureOpts
}

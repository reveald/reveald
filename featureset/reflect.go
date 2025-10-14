package featureset

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/reveald/reveald"
)

// tagOptions represents parsed options from a reveald struct tag.
//
// These options control how the Reflect function generates features
// for struct fields.
type tagOptions struct {
	ignore                bool                  // Skip this field entirely
	noSort                bool                  // Don't generate sorting options
	dynamic               bool                  // Create dynamic filter for this field
	histogram             bool                  // Create histogram aggregation (numeric fields only)
	histogramInterval     float64               // Histogram bucket interval
	dateHistogram         bool                  // Create date histogram (time.Time fields only)
	dateHistogramInterval DateHistogramInterval // Date histogram interval
	aggSize               int                   // Aggregation size for dynamic filters
}

// parseTagOptions parses a reveald tag into structured options.
//
// Supports formats like:
//   - "ignore" - skip field entirely
//   - "dynamic,no-sort" - multiple comma-separated options
//   - "histogram,interval=100" - option with parameter
//   - "date-histogram,interval=day" - date histogram with interval
//   - "dynamic,agg-size=50" - custom aggregation size
func parseTagOptions(tag string) tagOptions {
	opts := tagOptions{
		histogramInterval:     100, // default
		dateHistogramInterval: Day, // default
		aggSize:               100, // default
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
			case "agg-size":
				if size, err := strconv.Atoi(value); err == nil {
					opts.aggSize = size
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

// fieldInfo holds information about a field including its path
type fieldInfo struct {
	field     reflect.StructField
	fieldPath string // e.g., "Details.Price"
	jsonPath  string // e.g., "details.price.keyword"
}

// collectFields recursively collects all fields including nested struct fields
func collectFields(t reflect.Type, prefix string, jsonPrefix string) []fieldInfo {
	var fields []fieldInfo

	for _, f := range reflect.VisibleFields(t) {
		fieldPath := f.Name
		if prefix != "" {
			fieldPath = prefix + "." + f.Name
		}

		jsonName := f.Name
		jsonTag := f.Tag.Get("json")
		if jsonTag != "" {
			jsonName = strings.Split(jsonTag, ",")[0]
		}

		jsonPath := jsonName
		if jsonPrefix != "" {
			jsonPath = jsonPrefix + "." + jsonName
		}

		// Check if this is a struct (but not time.Time)
		fieldType := f.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}

		if fieldType.Kind() == reflect.Struct && fieldType != reflect.TypeOf(time.Time{}) {
			// Recursively process nested struct
			nestedFields := collectFields(fieldType, fieldPath, jsonPath)
			fields = append(fields, nestedFields...)
		} else {
			// Regular field
			fields = append(fields, fieldInfo{
				field:     f,
				fieldPath: fieldPath,
				jsonPath:  jsonPath,
			})
		}
	}

	return fields
}

// Reflect automatically generates Elasticsearch features from a struct type using reflection.
//
// It analyzes struct fields and their tags to create appropriate features for filtering,
// aggregation, and sorting. Nested structs are processed recursively, with field paths
// constructed using dot notation (e.g., "Details.Price").
//
// # Supported Field Types
//
// The following Go types are automatically processed:
//
//   - string: Creates dynamic filter features (requires reveald:"dynamic" tag)
//   - bool: Creates DynamicBooleanFilterFeature automatically
//   - int, int8, int16, int32, int64: Creates DynamicFilterFeature automatically
//   - uint, uint8, uint16, uint32, uint64: Creates DynamicFilterFeature automatically
//   - float32, float64: Creates DynamicFilterFeature automatically
//   - time.Time: Creates DynamicFilterFeature automatically
//   - struct: Recursively processes nested fields with dotted paths
//   - *T: Pointers to basic types are unwrapped and treated as their base type
//   - []T: Slices of basic types create features for the element type
//
// Pointer and slice support details:
//   - *string, *int, *bool, *float64, *time.Time: Treated as optional fields
//   - []string, []int, []float64: Multi-valued fields (Elasticsearch handles arrays natively)
//   - Slices of strings do NOT get .keyword suffix (arrays work directly in ES)
//
// # Struct Tags
//
// The reveald struct tag controls feature generation with the following options:
//
// ## Basic Tags
//
//   - ignore: Skip this field entirely (no features generated)
//   - dynamic: Create dynamic filter for string fields
//   - no-sort: Don't create sorting options for this field
//
// ## Aggregation Tags
//
//   - histogram: Create histogram aggregation for numeric/float fields
//   - histogram,interval=N: Histogram with custom interval (default: 100)
//   - date-histogram: Create date histogram for time.Time fields
//   - date-histogram,interval=I: Date histogram with interval (default: day)
//     Valid intervals: second, minute, hour, day, week, month, quarter, year
//   - agg-size=N: Set aggregation size for dynamic filters (default: 100)
//     Controls the maximum number of buckets returned in aggregations
//
// ## Combining Tags
//
// Multiple options can be combined with commas:
//
//	Name string `reveald:"dynamic,no-sort"` // Dynamic filter but no sorting
//
// # JSON Tag Support
//
// The json struct tag is respected for field naming at all nesting levels:
//
//	Price float64 `json:"product_price"` // Uses "product_price" in Elasticsearch
//
// For nested structs, json tags are applied at each level:
//
//	type Details struct {
//	    Price float64 `json:"price_amount"`
//	}
//	type Product struct {
//	    Details Details `json:"product_details"`
//	}
//	// Creates path: "product_details.price_amount" in Elasticsearch
//
// # Examples
//
// Basic usage with flat struct:
//
//	type Product struct {
//	    Name     string  `reveald:"dynamic"`
//	    Price    float64 `reveald:"histogram,interval=50"`
//	    Active   bool
//	    Category string  `reveald:"dynamic"`
//	    Internal string  `reveald:"ignore"`
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Product{}))
//	// Creates:
//	// - DynamicFilterFeature for Name (requires dynamic tag for strings)
//	// - HistogramFeature for Price with interval 50
//	// - DynamicBooleanFilterFeature for Active (automatic for bool)
//	// - DynamicFilterFeature for Category
//	// - SortingFeature with options for all non-ignored fields
//	// - Internal field is completely skipped
//
// Nested struct example:
//
//	type Address struct {
//	    City    string `reveald:"dynamic"`
//	    ZipCode string `reveald:"dynamic"`
//	}
//
//	type Person struct {
//	    Name    string
//	    Address Address  // Nested struct
//	    Age     int
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Person{}))
//	// Creates features with paths:
//	// - "Address.City" (dynamic filter)
//	// - "Address.ZipCode" (dynamic filter)
//	// - "Age" (automatic dynamic filter for int)
//	// - Sorting options for all fields including "Address.City-asc", etc.
//
// Date histogram example:
//
//	type Event struct {
//	    Name      string    `reveald:"dynamic"`
//	    Timestamp time.Time `reveald:"date-histogram,interval=hour"`
//	    Count     int       `reveald:"histogram,interval=10"`
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Event{}))
//	// Creates:
//	// - DynamicFilterFeature for Name
//	// - DateHistogramFeature for Timestamp with hourly buckets
//	// - HistogramFeature for Count with interval 10
//	// - SortingFeature for all fields
//
// Custom aggregation size example:
//
//	type Catalog struct {
//	    Category string `reveald:"dynamic,agg-size=50"`  // Return up to 50 category buckets
//	    Brand    string `reveald:"dynamic,agg-size=200"` // Return up to 200 brand buckets
//	    Status   string `reveald:"dynamic"`              // Uses default size 100
//	    Price    float64 `reveald:"agg-size=30"`         // Price buckets limited to 30
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Catalog{}))
//	// Creates DynamicFilterFeatures with custom aggregation sizes
//	// Useful when fields have different cardinality needs
//
// Pointer and slice example:
//
//	type Article struct {
//	    Title      string     `reveald:"dynamic"`
//	    ViewCount  *uint64    // Optional field (nil = not viewed)
//	    Tags       []string   `reveald:"dynamic"`  // Multi-valued field
//	    Categories []string   `reveald:"dynamic,agg-size=50"`
//	    Author     *string    `reveald:"dynamic"`  // Optional author
//	    Ratings    []float64  // Array of ratings
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Article{}))
//	// Pointers are treated as optional versions of their base types
//	// Slices create aggregations over array elements
//	// []string fields work without .keyword suffix
//
// # Feature Types Generated
//
//   - DynamicFilterFeature: For filterable fields (strings with dynamic tag, numerics, time)
//   - DynamicBooleanFilterFeature: For boolean fields
//   - HistogramFeature: For numeric fields with histogram tag
//   - DateHistogramFeature: For time.Time fields with date-histogram tag
//   - SortingFeature: One feature with sort options for all non-ignored, non-no-sort fields
//
// # Notes
//
//   - String fields require the "dynamic" tag to create filters (unlike other types)
//   - Histogram/date-histogram tags replace the default dynamic filter for that field
//   - time.Time fields are special-cased and not treated as regular structs
//   - All fields (including nested) get sorting options unless "ignore" or "no-sort" is specified
//   - Field paths use Go field names (e.g., "Details.Price")
//   - Elasticsearch field names use json tags at all levels (e.g., "product_details.price_amount")
//   - If no json tag is present, the Go field name is used as-is for Elasticsearch
//   - Unsigned integers (uint, uint32, etc.) are treated identically to signed integers
//   - Pointers (*string, *int, etc.) are unwrapped automatically - useful for optional fields
//   - Slices ([]string, []int) are treated as multi-valued fields - no .keyword suffix for []string
//   - Nil pointer values are handled by Elasticsearch as missing fields
func Reflect(t reflect.Type) []reveald.Feature {
	sortOpts := make([]SortingOption, 0)
	featureOpts := make([]reveald.Feature, 0)

	fields := collectFields(t, "", "")

	for _, fieldInfo := range fields {
		f := fieldInfo.field
		rtag := f.Tag.Get("reveald")
		opts := parseTagOptions(rtag)

		if opts.ignore {
			continue
		}

		fieldPath := fieldInfo.fieldPath
		jsonPath := fieldInfo.jsonPath

		// Unwrap pointer and slice types for basic type checking
		fieldType := f.Type
		isSlice := false
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Slice {
			isSlice = true
			fieldType = fieldType.Elem() // Get element type
		}

		// Handle histogram features for numeric types
		if opts.histogram {
			switch fieldType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				featureOpts = append(featureOpts, NewHistogramFeature(fieldPath, WithInterval(opts.histogramInterval)))
			}
		}

		// Handle date histogram features for time.Time
		if opts.dateHistogram && (fieldType == reflect.TypeOf(time.Time{}) || f.Type == reflect.TypeOf(&time.Time{})) {
			featureOpts = append(featureOpts, NewDateHistogramFeature(fieldPath, opts.dateHistogramInterval))
		}

		// Add default features for types (check unwrapped type)
		switch fieldType.Kind() {
		case reflect.String:
			// For slices of strings, don't add .keyword as Elasticsearch handles arrays natively
			if !isSlice {
				jsonPath += ".keyword"
			}

		case reflect.Bool:
			featureOpts = append(featureOpts, NewDynamicBooleanFilterFeature(fieldPath))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if !opts.histogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
			}
		case reflect.Float32, reflect.Float64:
			if !opts.histogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
			}
		case reflect.TypeOf(time.Time{}).Kind():
			if !opts.dateHistogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
			}
		}

		// Add sorting options
		if !opts.noSort {
			sortOpts = append(sortOpts, WithSortOption(fieldPath+"-desc", jsonPath, false))
			sortOpts = append(sortOpts, WithSortOption(fieldPath+"-asc", jsonPath, true))
		}

		// Handle dynamic tag (use unwrapped type)
		if opts.dynamic {
			switch fieldType.Kind() {
			case reflect.String:
				featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if !opts.histogram {
					featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
				}
			case reflect.Float32, reflect.Float64:
				if !opts.histogram {
					featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
				}
			}
		}
	}
	if len(sortOpts) > 0 {
		featureOpts = append(featureOpts, NewSortingFeature("sort", sortOpts...))
	}
	return featureOpts
}

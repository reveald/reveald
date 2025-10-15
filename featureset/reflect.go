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
	ignore            bool   // Skip this field entirely
	noSort            bool   // Don't generate sorting options
	dynamic           bool   // Create dynamic filter for this field
	histogram         bool   // Create histogram aggregation (numeric or time fields)
	histogramInterval string // Histogram bucket interval (numeric value or date interval like "day")
	aggSize           int    // Aggregation size for dynamic filters
	searchable        bool   // Add field to multi-field search query
}

// parseTagOptions parses a reveald tag into structured options.
//
// Supports formats like:
//   - "ignore" - skip field entirely
//   - "dynamic,no-sort" - multiple comma-separated options
//   - "histogram,interval=100" - histogram with numeric interval
//   - "histogram,interval=day" - histogram with date interval
//   - "dynamic,agg-size=50" - custom aggregation size
//   - "searchable" - add field to full-text search
func parseTagOptions(tag string) tagOptions {
	opts := tagOptions{
		histogramInterval: "100", // default (works for both numeric and date histograms)
		aggSize:           100,   // default
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
				opts.histogramInterval = value
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
			case "searchable":
				opts.searchable = true
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

		// Parse json tag according to encoding/json rules
		jsonName := f.Name
		jsonTag := f.Tag.Get("json")
		if jsonTag != "" {
			// Split on comma to separate name from options
			parts := strings.Split(jsonTag, ",")
			name := parts[0]

			// Handle special cases per encoding/json spec:
			// - json:"-" means skip this field entirely
			// - json:"-," means use "-" as the field name
			if name == "-" && len(parts) == 1 {
				// Skip this field (json:"-")
				continue
			}
			if name == "-" && len(parts) > 1 {
				// Use "-" as field name (json:"-,...")
				jsonName = "-"
			} else if name != "" {
				// Use the specified name (json:"customName" or json:"customName,omitempty")
				jsonName = name
			}
			// If name is empty (json:",omitempty"), keep the default field name
		}

		jsonPath := jsonName
		if jsonPrefix != "" {
			jsonPath = jsonPrefix + "." + jsonName
		}

		// Check if this is a struct (but not time.Time)
		fieldType := f.Type
		if fieldType.Kind() == reflect.Pointer {
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
// Embedded struct support:
//   - Embedded structs are fully supported via reflect.VisibleFields()
//   - Fields are accessible via both promoted and qualified paths
//   - Example: embedded field A.Value creates both "A.Value" and "Value" (promoted)
//   - Shadowing works correctly: outer fields shadow embedded fields with same name
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
//   - searchable: Add field to full-text search (creates QueryFilterFeature)
//
// ## Aggregation Tags
//
//   - histogram: Create histogram aggregation (numeric fields or time.Time)
//   - histogram,interval=N: Numeric histogram with custom interval (default: 100)
//   - histogram,interval=I: Date histogram with time interval (default: day)
//     Valid time intervals: second, minute, hour, day, week, month, quarter, year
//   - agg-size=N: Set aggregation size for dynamic filters (default: 100)
//     Controls the maximum number of buckets returned in aggregations
//
// The histogram tag automatically detects the field type:
//   - For int/uint/float: Creates HistogramFeature with numeric interval
//   - For time.Time: Creates DateHistogramFeature with date interval
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
// Histogram example (type-aware):
//
//	type Event struct {
//	    Name      string    `reveald:"dynamic"`
//	    Timestamp time.Time `reveald:"histogram,interval=hour"`  // Date histogram
//	    Count     int       `reveald:"histogram,interval=10"`    // Numeric histogram
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Event{}))
//	// Creates:
//	// - DynamicFilterFeature for Name
//	// - DateHistogramFeature for Timestamp with hourly buckets (auto-detected from time.Time)
//	// - HistogramFeature for Count with interval 10 (auto-detected from int)
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
// Embedded struct example:
//
//	type BaseModel struct {
//	    ID        uint64    `reveald:"dynamic"`
//	    CreatedAt time.Time `reveald:"histogram,interval=day"`
//	}
//
//	type Product struct {
//	    BaseModel              // Embedded struct
//	    Name      string `reveald:"dynamic"`
//	    ID        uint64 `reveald:"dynamic"` // Shadows BaseModel.ID
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Product{}))
//	// Creates features for:
//	// - BaseModel.ID (qualified path to embedded field)
//	// - BaseModel.CreatedAt (qualified path)
//	// - ID (promoted, but refers to Product.ID which shadows BaseModel.ID)
//	// - CreatedAt (promoted from BaseModel)
//	// - Name (from Product)
//	// Both qualified and promoted paths are available for filtering/sorting
//
// Full-text search example:
//
//	type Article struct {
//	    Title       string `reveald:"dynamic,searchable"`
//	    Description string `reveald:"searchable"`
//	    Body        string `reveald:"searchable"`
//	    Author      string `reveald:"dynamic"`
//	    Published   time.Time
//	}
//
//	features := featureset.Reflect(reflect.TypeOf(Article{}))
//	// Creates:
//	// - DynamicFilterFeature for Title and Author
//	// - QueryFilterFeature with fields: [Title, Description, Body]
//	//   Enables full-text search across these fields using the "q" parameter
//	// - SortingFeature for all fields
//
// # Feature Types Generated
//
//   - DynamicFilterFeature: For filterable fields (strings with dynamic tag, numerics, time)
//   - DynamicBooleanFilterFeature: For boolean fields
//   - HistogramFeature: For numeric fields with histogram tag
//   - DateHistogramFeature: For time.Time fields with histogram tag
//   - SortingFeature: One feature with sort options for all non-ignored, non-no-sort fields
//   - QueryFilterFeature: Created when any field has searchable tag (full-text search)
//
// # Notes
//
//   - String fields require the "dynamic" tag to create filters (unlike other types)
//   - The histogram tag is type-aware: creates numeric or date histogram based on field type
//   - Histogram tags replace the default dynamic filter for that field
//   - time.Time fields are special-cased and not treated as regular structs
//   - All fields (including nested) get sorting options unless "ignore" or "no-sort" is specified
//   - Field paths use Go field names (e.g., "Details.Price")
//   - Elasticsearch field names use json tags at all levels (e.g., "product_details.price_amount")
//   - If no json tag is present, the Go field name is used as-is for Elasticsearch
//   - Unsigned integers (uint, uint32, etc.) are treated identically to signed integers
//   - Pointers (*string, *int, etc.) are unwrapped automatically - useful for optional fields
//   - Slices ([]string, []int) are treated as multi-valued fields - no .keyword suffix for []string
//   - Nil pointer values are handled by Elasticsearch as missing fields
//   - Embedded structs create both qualified (Embedded.Field) and promoted (Field) paths
//   - Field shadowing in embedded structs works correctly per Go semantics
//   - The searchable tag collects fields into a QueryFilterFeature for full-text search
//   - QueryFilterFeature responds to the "q" request parameter by default
func Reflect(t reflect.Type) []reveald.Feature {
	sortOpts := make([]SortingOption, 0)
	featureOpts := make([]reveald.Feature, 0)
	searchableFields := make([]string, 0)

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
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Slice {
			isSlice = true
			fieldType = fieldType.Elem() // Get element type
		}

		// Handle histogram features (type-aware)
		if opts.histogram {
			switch fieldType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				// Parse interval as float64 for numeric histograms
				interval := 100.0
				if val, err := strconv.ParseFloat(opts.histogramInterval, 64); err == nil {
					interval = val
				}
				featureOpts = append(featureOpts, NewHistogramFeature(fieldPath, WithInterval(interval)))
			}

			// For time.Time, create date histogram
			if fieldType == reflect.TypeOf(time.Time{}) || f.Type == reflect.TypeOf(&time.Time{}) {
				featureOpts = append(featureOpts, NewDateHistogramFeature(fieldPath, DateHistogramInterval(opts.histogramInterval)))
			}
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
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64, reflect.TypeOf(time.Time{}).Kind():
			if !opts.histogram {
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
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				if !opts.histogram {
					featureOpts = append(featureOpts, NewDynamicFilterFeature(fieldPath, WithAggregationSize(opts.aggSize)))
				}
			}
		}

		// Collect searchable fields
		if opts.searchable {
			searchableFields = append(searchableFields, jsonPath)
		}
	}
	if len(sortOpts) > 0 {
		featureOpts = append(featureOpts, NewSortingFeature("sort", sortOpts...))
	}
	if len(searchableFields) > 0 {
		featureOpts = append(featureOpts, NewQueryFilterFeature(WithFields(searchableFields...)))
	}
	return featureOpts
}

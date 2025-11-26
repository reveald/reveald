package featureset

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/reveald/reveald/v2"
)

// reflectionDefaults holds default configuration for the Reflect function.
type reflectionDefaults struct {
	defaultAggSize           int    // Default aggregation size
	defaultHistogramInterval string // Default histogram interval
	defaultDateInterval      string // Default date histogram interval
	stringsDynamicByDefault  bool   // Whether string fields get dynamic filters by default
	sortableByDefault        bool   // Whether fields are sortable by default
	sortDescSuffix           string // Suffix for descending sort (e.g., "-desc")
	sortAscSuffix            string // Suffix for ascending sort (e.g., "-asc")
	searchParamName          string // Query parameter name for search (default "q")
}

// ReflectionOption is a functional option for configuring reflection defaults.
type ReflectionOption func(*reflectionDefaults)

// WithDefaultAggSize sets the default aggregation size for dynamic filters.
func WithDefaultAggSize(size int) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.defaultAggSize = size
	}
}

// WithDefaultHistogramInterval sets the default interval for numeric histograms.
func WithDefaultHistogramInterval(interval string) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.defaultHistogramInterval = interval
	}
}

// WithDefaultDateInterval sets the default interval for date histograms.
func WithDefaultDateInterval(interval string) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.defaultDateInterval = interval
	}
}

// WithStringsDynamicByDefault controls whether string fields get dynamic filters automatically.
func WithStringsDynamicByDefault(enabled bool) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.stringsDynamicByDefault = enabled
	}
}

// WithSortableByDefault controls whether fields are sortable by default.
func WithSortableByDefault(enabled bool) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.sortableByDefault = enabled
	}
}

// WithSortSuffixes sets custom suffixes for sort options.
func WithSortSuffixes(descSuffix, ascSuffix string) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.sortDescSuffix = descSuffix
		rd.sortAscSuffix = ascSuffix
	}
}

// WithSearchParamName sets the query parameter name for full-text search.
func WithSearchParamName(name string) ReflectionOption {
	return func(rd *reflectionDefaults) {
		rd.searchParamName = name
	}
}

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
	defaultSortAsc    bool   // Mark this field as the default sort option (ascending)
	defaultSortDesc   bool   // Mark this field as the default sort option (descending)
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
//   - "defaultSort-asc" - mark as default sort option (ascending)
//   - "defaultSort-desc" - mark as default sort option (descending)
func parseTagOptions(tag string, defaults *reflectionDefaults) tagOptions {
	opts := tagOptions{
		histogramInterval: defaults.defaultHistogramInterval,
		aggSize:           defaults.defaultAggSize,
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
			case "defaultSort-asc":
				opts.defaultSortAsc = true
			case "defaultSort-desc":
				opts.defaultSortDesc = true
			}
		}
	}

	return opts
}

// fieldInfo holds information about a field including its path
type fieldInfo struct {
	field    reflect.StructField
	jsonPath string // e.g., "details.price.keyword"
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
				field:    f,
				jsonPath: jsonPath,
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
// Configuration options can be passed to customize default behavior using functional options
// like WithDefaultAggSize, WithStringsDynamicByDefault, etc.
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
//   - defaultSort-asc: Mark this field as the default sort option (ascending order)
//   - defaultSort-desc: Mark this field as the default sort option (descending order)
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
// ## Default Sort Options
//
// The defaultSort-asc and defaultSort-desc tags mark a field as the default sort option.
// When creating a SortingFeature with the returned sort options, this field will be
// used as the default when no sort parameter is provided in the request.
//
//   - defaultSort-asc: Sort by this field in ascending order by default
//   - defaultSort-desc: Sort by this field in descending order by default
//
// Important notes:
//   - Only the first field with a defaultSort tag will be used (subsequent ones are ignored)
//   - If the field also has no-sort tag, the defaultSort tag is ignored
//   - The defaultSort tag respects json tags for field naming
//
// ## Combining Tags
//
// Multiple options can be combined with commas:
//
//	Name string `reveald:"dynamic,no-sort"` // Dynamic filter but no sorting
//	Price float64 `reveald:"defaultSort-desc"` // Default sort by price descending
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
// Default sort example:
//
//	type Product struct {
//	    Name     string    `reveald:"dynamic"`
//	    Price    float64   `reveald:"defaultSort-desc"` // Default sort by price descending
//	    Created  time.Time `reveald:"defaultSort-asc"`  // This is ignored (first field wins)
//	    Category string    `reveald:"dynamic"`
//	}
//
//	features, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))
//	sortingFeature := featureset.NewSortingFeature("sort", sortOpts...)
//	// Creates:
//	// - DynamicFilterFeatures for Name, Price, Created, Category
//	// - SortingFeature with Price-desc as the default option
//	// When no "sort" parameter is provided in requests, results will be sorted by Price descending
//
// Custom defaults example:
//
//	type Product struct {
//	    Name  string
//	    Price float64
//	    Stock int
//	}
//
//	features := featureset.Reflect(
//	    reflect.TypeOf(Product{}),
//	    featureset.WithDefaultAggSize(50),                // Custom aggregation size
//	    featureset.WithStringsDynamicByDefault(true),     // Strings get filters automatically
//	    featureset.WithSortSuffixes(".desc", ".asc"),     // Custom sort naming
//	    featureset.WithSearchParamName("search"),         // Use "search" instead of "q"
//	    featureset.WithDefaultHistogramInterval("50"),    // Default histogram interval
//	)
//	// Creates dynamic filters for Name (string), Price, and Stock
//	// Uses custom sort suffixes and aggregation sizes
//	// Any searchable fields would use "search" parameter instead of "q"
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
// # Configuration Options
//
// The following ReflectionOption functions can be passed to customize defaults:
//
//   - WithDefaultAggSize(size int): Set default aggregation size (default: 100)
//   - WithDefaultHistogramInterval(interval string): Set numeric histogram interval (default: "100")
//   - WithDefaultDateInterval(interval string): Set date histogram interval (default: "day")
//   - WithStringsDynamicByDefault(enabled bool): Auto-create filters for strings (default: false)
//   - WithSortableByDefault(enabled bool): Enable/disable sorting by default (default: true)
//   - WithSortSuffixes(desc, asc string): Customize sort option suffixes (default: "-desc", "-asc")
//   - WithSearchParamName(name string): Set query parameter for search (default: "q")
//
// # Notes
//
//   - String fields require the "dynamic" tag to create filters (unless WithStringsDynamicByDefault is true)
//   - The histogram tag is type-aware: creates numeric or date histogram based on field type
//   - Histogram tags replace the default dynamic filter for that field
//   - time.Time fields are special-cased and not treated as regular structs
//   - All fields get sorting options by default unless "ignore" or "no-sort" tag is used
//   - Sorting can be disabled globally with WithSortableByDefault(false)
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
//   - QueryFilterFeature responds to configured search parameter (default "q")
func Reflect(t reflect.Type, options ...ReflectionOption) ([]reveald.Feature, []SortingOption) {
	// Initialize defaults
	defaults := &reflectionDefaults{
		defaultAggSize:           100,
		defaultHistogramInterval: "100",
		defaultDateInterval:      "day",
		stringsDynamicByDefault:  false,
		sortableByDefault:        true,
		sortDescSuffix:           "-desc",
		sortAscSuffix:            "-asc",
		searchParamName:          "q",
	}

	// Apply custom options
	for _, opt := range options {
		opt(defaults)
	}

	sortOpts := make([]SortingOption, 0)
	featureOpts := make([]reveald.Feature, 0)
	searchableFields := make([]string, 0)
	var defaultSortOptionName string

	fields := collectFields(t, "", "")

	for _, fieldInfo := range fields {
		f := fieldInfo.field
		rtag := f.Tag.Get("reveald")
		opts := parseTagOptions(rtag, defaults)

		if opts.ignore {
			continue
		}

		jsonPath := fieldInfo.jsonPath

		// Unwrap pointer and slice types for basic type checking
		fieldType := f.Type

		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Slice {
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
				featureOpts = append(featureOpts, NewHistogramFeature(jsonPath, WithInterval(interval)))
			}

			// For time.Time, create date histogram
			if fieldType == reflect.TypeOf(time.Time{}) || f.Type == reflect.TypeOf(&time.Time{}) {
				featureOpts = append(featureOpts, NewDateHistogramFeature(jsonPath, DateHistogramInterval(opts.histogramInterval)))
			}
		}

		// Add default features for types (check unwrapped type)
		switch fieldType.Kind() {
		case reflect.String:

			// Add dynamic filter if enabled by default
			if defaults.stringsDynamicByDefault && !opts.dynamic {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(jsonPath, WithAggregationSize(opts.aggSize)))
			}

		case reflect.Bool:
			featureOpts = append(featureOpts, NewDynamicBooleanFilterFeature(jsonPath))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64, reflect.TypeOf(time.Time{}).Kind():
			if !opts.histogram {
				featureOpts = append(featureOpts, NewDynamicFilterFeature(jsonPath, WithAggregationSize(opts.aggSize)))
			}
		}

		// Add sorting options (respect both tag and defaults)
		shouldSort := defaults.sortableByDefault && !opts.noSort
		if shouldSort {
			descOptionName := jsonPath + defaults.sortDescSuffix
			ascOptionName := jsonPath + defaults.sortAscSuffix

			sortOpts = append(sortOpts, WithSortOption(descOptionName, jsonPath, false))
			sortOpts = append(sortOpts, WithSortOption(ascOptionName, jsonPath, true))

			// Check if this field should be the default sort option
			if opts.defaultSortDesc && defaultSortOptionName == "" {
				defaultSortOptionName = descOptionName
			}
			if opts.defaultSortAsc && defaultSortOptionName == "" {
				defaultSortOptionName = ascOptionName
			}
		}

		// Handle dynamic tag (use unwrapped type)
		if opts.dynamic {
			switch fieldType.Kind() {
			case reflect.String:
				featureOpts = append(featureOpts, NewDynamicFilterFeature(jsonPath, WithAggregationSize(opts.aggSize)))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
				if !opts.histogram {
					featureOpts = append(featureOpts, NewDynamicFilterFeature(jsonPath, WithAggregationSize(opts.aggSize)))
				}
			}
		}

		// Collect searchable fields
		if opts.searchable {
			searchableFields = append(searchableFields, jsonPath)
		}
	}

	if len(searchableFields) > 0 {
		featureOpts = append(featureOpts, NewQueryFilterFeature(
			WithQueryParam(defaults.searchParamName),
			WithFields(searchableFields...),
		))
	}

	// Add default sort option if one was specified
	if defaultSortOptionName != "" {
		sortOpts = append(sortOpts, WithDefaultSortOption(defaultSortOptionName))
	}

	return featureOpts, sortOpts
}

func ReflectType[T any](options ...ReflectionOption) ([]reveald.Feature, []SortingOption) {
	return Reflect(reflect.TypeOf((*T)(nil)).Elem(), options...)
}

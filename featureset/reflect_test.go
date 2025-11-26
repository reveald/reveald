package featureset_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/reveald/reveald/v2"
	"github.com/reveald/reveald/v2/featureset"
)

func Test_ReflectionFeature(t *testing.T) {

	type TTarget struct {
		Name     string `json:"name-overridden" reveald:"dynamic"`
		Active   bool
		Category string `reveald:"dynamic"`
		Count    int
		Ignored  string `reveald:"ignore"`
		Created  time.Time
		Updated  time.Time `reveald:"no-sort"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(TTarget{}))
	if len(features) != 6 {
		t.Fatal("expected 6 features, got", len(features))
	}

	// Check sorting options: All fields except Ignored and Updated (no-sort)
	// Name, Active, Category, Count, Created = 5 fields * 2 directions = 10 options
	if len(sortOpts) != 10 {
		t.Fatal("expected 10 sorting options, got", len(sortOpts))
	}

	for _, f := range features {
		switch f.(type) {
		case *featureset.DynamicFilterFeature:
			// pass
		case *featureset.DynamicBooleanFilterFeature:
			// pass
		default:
			t.Fatal("unexpected feature type", reflect.TypeOf(f))
		}
	}
}

func Test_ReflectDynamicStringFeatures(t *testing.T) {
	type TTarget struct {
		Name     string `json:"name-overridden" reveald:"dynamic"`
		Category string `reveald:"dynamic"`
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 2 {
		t.Fatalf("expected 2 dynamic filter features, got %d", len(dynamicFeatures))
	}

	// Just verify we got the expected number of dynamic features
	// The features don't expose their internal property names publicly
}

func Test_ReflectBooleanFeatures(t *testing.T) {
	type TTarget struct {
		Active  bool
		Enabled bool
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	boolFeatures := []*featureset.DynamicBooleanFilterFeature{}
	for _, f := range features {
		if bf, ok := f.(*featureset.DynamicBooleanFilterFeature); ok {
			boolFeatures = append(boolFeatures, bf)
		}
	}

	if len(boolFeatures) != 2 {
		t.Fatalf("expected 2 boolean filter features, got %d", len(boolFeatures))
	}
}

func Test_ReflectIntegerFeatures(t *testing.T) {
	type TTarget struct {
		Count int
		Age   int32
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 2 {
		t.Fatalf("expected 2 dynamic filter features for integers, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectFloatFeatures(t *testing.T) {
	type TTarget struct {
		Price  float64
		Rating float32
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 2 {
		t.Fatalf("expected 2 dynamic filter features for floats, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectDynamicFloatFeatures(t *testing.T) {
	type TTarget struct {
		Price  float64 `reveald:"dynamic"`
		Rating float32 `reveald:"dynamic"`
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	// Each float field gets a feature automatically (2), and the dynamic tag adds another (2)
	// So we get 4 dynamic filter features (this is redundant but consistent with int behavior)
	if len(dynamicFeatures) != 4 {
		t.Fatalf("expected 4 dynamic filter features for dynamic floats, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectTimeFeatures(t *testing.T) {
	type TTarget struct {
		Created time.Time
		Updated time.Time
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 2 {
		t.Fatalf("expected 2 dynamic filter features for time fields, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectIgnoreTag(t *testing.T) {
	type TTarget struct {
		Name    string
		Ignored string `reveald:"ignore"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(TTarget{}))

	// Should have 0 features (no dynamic filter for Name without tag), sorting is separate
	if len(features) != 0 {
		t.Fatalf("expected 0 features, got %d", len(features))
	}

	// Should have 2 sort options for Name field (asc and desc)
	if len(sortOpts) != 2 {
		t.Fatalf("expected 2 sorting options, got %d", len(sortOpts))
	}
}

func Test_ReflectNoSortTag(t *testing.T) {
	type TTarget struct {
		Name    string
		Updated time.Time `reveald:"no-sort"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(TTarget{}))

	// Updated should have a dynamic filter (time.Time gets it automatically)
	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	if dynamicFilterCount != 1 {
		t.Fatalf("expected 1 dynamic filter for Updated, got %d", dynamicFilterCount)
	}

	// Name should have sorting options (2: asc and desc)
	// Updated should not have sorting options (no-sort tag)
	// Total should be 2 sort options
	if len(sortOpts) != 2 {
		t.Fatalf("expected 2 sort options (Name-asc, Name-desc), got %d", len(sortOpts))
	}

	// Create a SortingFeature with the options to test them
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify that Updated field sorting options are not present
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Updated") {
			t.Errorf("unexpected sort option for Updated field: %s", opt.Name)
		}
	}
}

func Test_ReflectSortingFeature(t *testing.T) {
	type TTarget struct {
		Name     string `json:"name-overridden"`
		Active   bool
		Count    int
		Category string
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(TTarget{}))

	// 4 fields * 2 directions = 8 sort options
	if len(sortOpts) != 8 {
		t.Fatalf("expected 8 sort options, got %d", len(sortOpts))
	}

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify Name field uses json tag override
	hasNameDesc := false
	hasNameAsc := false
	for _, opt := range result.Sorting.Options {
		if opt.Name == "name-overridden-desc" {
			hasNameDesc = true
			if opt.Value != "name-overridden" {
				t.Errorf("expected name-overridden-desc to use json tag 'name-overridden', got %s", opt.Value)
			}
		}
		if opt.Name == "name-overridden-asc" {
			hasNameAsc = true
		}
	}

	if !hasNameDesc || !hasNameAsc {
		t.Error("expected both name-overridden-desc and name-overridden-asc sort options")
	}
}

func Test_ReflectHistogramFeature(t *testing.T) {
	type TTarget struct {
		Price  float64 `reveald:"histogram,interval=50"`
		Count  int     `reveald:"histogram,interval=10"`
		Rating float32 `reveald:"histogram"`
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	histogramFeatures := []*featureset.HistogramFeature{}
	for _, f := range features {
		if hf, ok := f.(*featureset.HistogramFeature); ok {
			histogramFeatures = append(histogramFeatures, hf)
		}
	}

	if len(histogramFeatures) != 3 {
		t.Fatalf("expected 3 histogram features, got %d", len(histogramFeatures))
	}

	// Verify that DynamicFilterFeature is NOT created for histogram fields
	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 0 {
		t.Fatalf("expected 0 dynamic filter features for histogram fields, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectDateHistogramFeature(t *testing.T) {
	type TTarget struct {
		Created time.Time `reveald:"histogram,interval=day"`
		Updated time.Time `reveald:"histogram,interval=month"`
	}

	features, _ := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dateHistogramFeatures := []*featureset.DateHistogramFeature{}
	for _, f := range features {
		if dhf, ok := f.(*featureset.DateHistogramFeature); ok {
			dateHistogramFeatures = append(dateHistogramFeatures, dhf)
		}
	}

	if len(dateHistogramFeatures) != 2 {
		t.Fatalf("expected 2 date histogram features, got %d", len(dateHistogramFeatures))
	}

	// Verify that DynamicFilterFeature is NOT created for histogram fields
	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 0 {
		t.Fatalf("expected 0 dynamic filter features for histogram fields, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectCombinedTags(t *testing.T) {
	type TTarget struct {
		Name     string    `reveald:"dynamic,no-sort"`
		Price    float64   `reveald:"histogram,interval=100"`
		Created  time.Time `reveald:"histogram,interval=day"`
		Category string    `reveald:"dynamic"`
		Ignored  string    `reveald:"ignore"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(TTarget{}))

	// Count each feature type
	histogramCount := 0
	dateHistogramCount := 0
	dynamicFilterCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.HistogramFeature:
			histogramCount++
		case *featureset.DateHistogramFeature:
			dateHistogramCount++
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		}
	}

	if histogramCount != 1 {
		t.Errorf("expected 1 histogram feature, got %d", histogramCount)
	}

	if dateHistogramCount != 1 {
		t.Errorf("expected 1 date histogram feature, got %d", dateHistogramCount)
	}

	// Name and Category should have dynamic filters
	if dynamicFilterCount != 2 {
		t.Errorf("expected 2 dynamic filter features, got %d", dynamicFilterCount)
	}

	// Verify Name field has no-sort respected
	// Price, Created, Category should have sort options (6 total)
	// Name should not (no-sort tag), Ignored should not (ignore tag)
	if len(sortOpts) != 6 {
		t.Fatalf("expected 6 sort options, got %d", len(sortOpts))
	}

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that Name field doesn't have sort options (no-sort tag)
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Name") {
			t.Errorf("Name field should not have sort options due to no-sort tag, found: %s", opt.Name)
		}
	}
}

func Test_ReflectNestedStruct(t *testing.T) {
	type Details struct {
		Price    float64 `reveald:"histogram,interval=50"`
		Currency string  `reveald:"dynamic"`
	}

	type Product struct {
		Name    string
		Details Details
		Active  bool
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Product{}))

	// Count feature types
	histogramCount := 0
	dynamicFilterCount := 0
	boolFilterCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.HistogramFeature:
			histogramCount++
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		case *featureset.DynamicBooleanFilterFeature:
			boolFilterCount++
		}
	}

	// Details.Price should create histogram
	if histogramCount != 1 {
		t.Errorf("expected 1 histogram feature for Details.Price, got %d", histogramCount)
	}

	// Details.Currency should create dynamic filter
	if dynamicFilterCount != 1 {
		t.Errorf("expected 1 dynamic filter for Details.Currency, got %d", dynamicFilterCount)
	}

	// Active should create boolean filter
	if boolFilterCount != 1 {
		t.Errorf("expected 1 boolean filter for Active, got %d", boolFilterCount)
	}
}

func Test_ReflectDeeplyNestedStruct(t *testing.T) {
	type Address struct {
		City    string `json:"city"`
		ZipCode string `reveald:"dynamic"`
	}

	type Contact struct {
		Email   string
		Address Address
	}

	type Person struct {
		Name    string
		Contact Contact
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(Person{}))

	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	// Contact.Address.ZipCode should have dynamic filter
	if dynamicFilterCount != 1 {
		t.Errorf("expected 1 dynamic filter for nested ZipCode, got %d", dynamicFilterCount)
	}

	// Verify sorting options have nested field paths
	// All fields should be sortable: Name, Contact.Email, Contact.Address.city, Contact.Address.ZipCode
	// 4 fields * 2 directions = 8 options
	if len(sortOpts) != 8 {
		t.Fatalf("expected 8 sort options, got %d", len(sortOpts))
	}

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check for nested field sort options (now jsonPath is used for sort names)
	// The nested path will be "Contact.Address.city" since json tags build up the path
	hasContactAddressCity := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Contact.Address.city") {
			hasContactAddressCity = true
			// Verify json path is correct
			if opt.Value != "Contact.Address.city" {
				t.Errorf("expected Contact.Address.City to use json path 'Contact.Address.city', got: %s", opt.Value)
			}
		}
	}

	if !hasContactAddressCity {
		t.Error("expected sort options for Contact.Address.City (Contact.Address.city)")
	}
}

func Test_ReflectNestedStructWithPointer(t *testing.T) {
	type Metadata struct {
		Version int
		Tags    string `reveald:"dynamic"`
	}

	type Document struct {
		Title    string
		Metadata *Metadata // Pointer to nested struct
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Document{}))

	dynamicFilterCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		}
	}

	// Should process nested struct even through pointer
	// Metadata.Tags should have dynamic filter (1) + Metadata.Version automatic (1)
	if dynamicFilterCount != 2 {
		t.Errorf("expected 2 dynamic filters for nested fields through pointer, got %d", dynamicFilterCount)
	}
}

func Test_ReflectCustomAggregationSize(t *testing.T) {
	type Product struct {
		Category string `reveald:"dynamic,agg-size=50"`
		Brand    string `reveald:"dynamic,agg-size=200"`
		Status   string `reveald:"dynamic"` // Uses default size 100
		Price    int    `reveald:"agg-size=25"`
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Product{}))

	// We can't directly inspect the aggregation size from DynamicFilterFeature
	// but we can verify the features were created
	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	// Category, Brand, Status (all dynamic strings) + Price (int)
	if dynamicFilterCount != 4 {
		t.Errorf("expected 4 dynamic filter features, got %d", dynamicFilterCount)
	}
}

func Test_ReflectAggSizeCombinations(t *testing.T) {
	type Data struct {
		Count     int       `reveald:"agg-size=500"`
		Score     float64   `reveald:"agg-size=30"`
		Name      string    `reveald:"dynamic,agg-size=75,no-sort"`
		Timestamp time.Time `reveald:"agg-size=150"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(Data{}))

	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	// Count, Score, Name, Timestamp should all have dynamic filters
	if dynamicFilterCount != 4 {
		t.Errorf("expected 4 dynamic filter features with custom agg sizes, got %d", dynamicFilterCount)
	}

	// Verify Name field doesn't have sorting due to no-sort tag
	// Count, Score, Timestamp should have sort options (3 * 2 = 6)
	// Name should not (no-sort tag)
	if len(sortOpts) != 6 {
		t.Fatalf("expected 6 sort options, got %d", len(sortOpts))
	}

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that Name field doesn't have sort options
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Name") {
			t.Errorf("Name field should not have sort options due to no-sort tag, found: %s", opt.Name)
		}
	}
}

func Test_ReflectNestedStructJsonTags(t *testing.T) {
	type Pricing struct {
		Amount   float64 `json:"price_amount" reveald:"histogram,interval=10"`
		Currency string  `json:"currency_code" reveald:"dynamic"`
	}

	type Details struct {
		SKU     string  `json:"product_sku" reveald:"dynamic"`
		Pricing Pricing `json:"pricing_info"`
	}

	type Product struct {
		Name    string  `json:"product_name" reveald:"dynamic"`
		Details Details `json:"product_details"`
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify top-level field uses json tag (now jsonPath is used for sort names too)
	foundTopLevel := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "product_name") {
			foundTopLevel = true
			if opt.Value != "product_name" {
				t.Errorf("expected Name to use json tag 'product_name', got: %s", opt.Value)
			}
		}
	}
	if !foundTopLevel {
		t.Error("expected sort option for Name field (product_name)")
	}

	// Verify nested struct field uses json tags at all levels
	// JSON path should be: product_details.product_sku
	foundNested := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "product_details.product_sku") {
			foundNested = true
			if opt.Value != "product_details.product_sku" {
				t.Errorf("expected Details.SKU to use json path 'product_details.product_sku', got: %s", opt.Value)
			}
		}
	}
	if !foundNested {
		t.Error("expected sort option for Details.SKU field (product_details.product_sku)")
	}

	// Verify deeply nested field uses json tags at all levels
	// JSON path should be: product_details.pricing_info.currency_code
	foundDeeplyNested := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "product_details.pricing_info.currency_code") {
			foundDeeplyNested = true
			if opt.Value != "product_details.pricing_info.currency_code" {
				t.Errorf("expected Details.Pricing.Currency to use json path 'product_details.pricing_info.currency_code', got: %s", opt.Value)
			}
		}
	}
	if !foundDeeplyNested {
		t.Error("expected sort option for Details.Pricing.Currency field (product_details.pricing_info.currency_code)")
	}
}

func Test_ReflectUnsignedIntegers(t *testing.T) {
	type Stats struct {
		Count uint `reveald:"agg-size=50"`
		Port  uint16
		ID    uint64 `reveald:"histogram,interval=100"`
		Flags uint8
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Stats{}))

	dynamicFilterCount := 0
	histogramCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		case *featureset.HistogramFeature:
			histogramCount++
		}
	}

	// Count, Port, Flags should have dynamic filters (ID has histogram instead)
	if dynamicFilterCount != 3 {
		t.Errorf("expected 3 dynamic filter features for unsigned ints, got %d", dynamicFilterCount)
	}

	// ID should have histogram
	if histogramCount != 1 {
		t.Errorf("expected 1 histogram feature for ID, got %d", histogramCount)
	}
}

func Test_ReflectPointers(t *testing.T) {
	type Optional struct {
		Name   *string `reveald:"dynamic"`
		Count  *int
		Price  *float64 `reveald:"histogram,interval=10"`
		Active *bool
		When   *time.Time `reveald:"histogram,interval=day"`
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Optional{}))

	dynamicFilterCount := 0
	boolFilterCount := 0
	histogramCount := 0
	dateHistogramCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		case *featureset.DynamicBooleanFilterFeature:
			boolFilterCount++
		case *featureset.HistogramFeature:
			histogramCount++
		case *featureset.DateHistogramFeature:
			dateHistogramCount++
		}
	}

	// Name and Count should have dynamic filters
	if dynamicFilterCount != 2 {
		t.Errorf("expected 2 dynamic filter features for pointer fields, got %d", dynamicFilterCount)
	}

	// Active should have boolean filter
	if boolFilterCount != 1 {
		t.Errorf("expected 1 boolean filter for *bool, got %d", boolFilterCount)
	}

	// Price should have histogram
	if histogramCount != 1 {
		t.Errorf("expected 1 histogram for *float64, got %d", histogramCount)
	}

	// When should have date histogram
	if dateHistogramCount != 1 {
		t.Errorf("expected 1 date histogram for *time.Time, got %d", dateHistogramCount)
	}
}

func Test_ReflectSlices(t *testing.T) {
	type Tagged struct {
		Tags       []string `reveald:"dynamic"`
		Categories []string `reveald:"dynamic,agg-size=200"`
		Scores     []int
		Ratings    []float64 `reveald:"agg-size=30"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(Tagged{}))

	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	// Tags, Categories, Scores, Ratings should all have dynamic filters
	if dynamicFilterCount != 4 {
		t.Errorf("expected 4 dynamic filter features for slice fields, got %d", dynamicFilterCount)
	}

	// Verify sorting works for slices (check that Tags doesn't incorrectly get .keyword)
	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that Tags field doesn't have .keyword suffix (slices are handled natively)
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Tags") {
			// Slice of strings should NOT have .keyword
			if strings.Contains(opt.Value, "Tags.keyword") {
				t.Errorf("slice field Tags should not have .keyword suffix, got: %s", opt.Value)
			}
		}
	}
}

func Test_ReflectCombinedNewTypes(t *testing.T) {
	type Complex struct {
		IDs      []uint64 `reveald:"agg-size=500"`
		OptName  *string  `reveald:"dynamic"`
		OptCount *int     `reveald:"histogram,interval=50"`
		Tags     []string `reveald:"dynamic,no-sort"`
		Port     uint16
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Complex{}))

	dynamicFilterCount := 0
	histogramCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		case *featureset.HistogramFeature:
			histogramCount++
		}
	}

	// IDs, OptName, Tags, Port should have dynamic filters
	if dynamicFilterCount != 4 {
		t.Errorf("expected 4 dynamic filter features, got %d", dynamicFilterCount)
	}

	// OptCount should have histogram
	if histogramCount != 1 {
		t.Errorf("expected 1 histogram feature, got %d", histogramCount)
	}
}

func Test_ReflectJsonTagSkip(t *testing.T) {
	type Data struct {
		Title    string `json:"title" reveald:"dynamic"`
		Internal string `json:"-" reveald:"dynamic"` // Should be skipped entirely
		Regular  string `json:"regular" reveald:"dynamic"`
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(Data{}))

	// Should only have 2 dynamic filters (Title and Regular), Internal should be skipped
	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	if dynamicFilterCount != 2 {
		t.Errorf("expected 2 dynamic filter features (Internal should be skipped), got %d", dynamicFilterCount)
	}

	// Verify sorting options don't include Internal field
	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that Internal field doesn't appear in sorting options
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Internal") {
			t.Errorf("json:\"-\" field Internal should be completely skipped, but found in sorting: %s", opt.Name)
		}
	}
}

func Test_ReflectJsonTagDash(t *testing.T) {
	type Data struct {
		DashField string `json:"-," reveald:"dynamic"` // Should use "-" as field name
		Normal    string `json:"normal" reveald:"dynamic"`
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Data{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that DashField uses "-" in Elasticsearch (now jsonPath is used for sort names)
	foundDash := false
	for _, opt := range result.Sorting.Options {
		if strings.HasPrefix(opt.Name, "-") {
			// The Elasticsearch field path should be "-"
			if opt.Value != "-" {
				t.Errorf("json:\"-,\" field should use '-' as name, got: %s", opt.Value)
			}
			foundDash = true
		}
	}

	if !foundDash {
		t.Error("field with json:\"-,\" tag not found in sorting options")
	}
}

func Test_ReflectJsonTagOmitEmpty(t *testing.T) {
	type Data struct {
		DefaultName string `json:",omitempty" reveald:"dynamic"` // Should keep "DefaultName"
		CustomName  string `json:"custom,omitempty" reveald:"dynamic"`
		NoOptions   string `reveald:"dynamic"` // No json tag at all
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Data{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify field names (now using jsonPath for sort option names)
	foundDefault := false
	foundCustom := false
	foundNoOptions := false

	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "DefaultName") {
			// json:",omitempty" should keep the Go field name "DefaultName"
			if opt.Value != "DefaultName" {
				t.Errorf("json:\",omitempty\" should keep Go field name, got: %s", opt.Value)
			}
			foundDefault = true
		}
		if strings.Contains(opt.Name, "custom") {
			// json:"custom,omitempty" should use "custom"
			if opt.Value != "custom" {
				t.Errorf("json:\"custom,omitempty\" should use 'custom' as name, got: %s", opt.Value)
			}
			foundCustom = true
		}
		if strings.Contains(opt.Name, "NoOptions") {
			// No json tag should keep the Go field name
			if opt.Value != "NoOptions" {
				t.Errorf("no json tag should keep Go field name, got: %s", opt.Value)
			}
			foundNoOptions = true
		}
	}

	if !foundDefault {
		t.Error("DefaultName field not found in sorting options")
	}
	if !foundCustom {
		t.Error("CustomName field not found in sorting options")
	}
	if !foundNoOptions {
		t.Error("NoOptions field not found in sorting options")
	}
}

func Test_ReflectEmbeddedStruct(t *testing.T) {
	type A struct {
		Value string `reveald:"dynamic"`
		Count int
	}

	type B struct {
		A            // Embedded struct
		Name  string `reveald:"dynamic"`
		Value string `reveald:"dynamic"` // Shadows A.Value
	}

	features, sortOpts := featureset.Reflect(reflect.TypeOf(B{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check what fields are visible
	foundFields := make(map[string]bool)
	for _, opt := range result.Sorting.Options {
		// Extract field name (remove -asc/-desc suffix)
		name := opt.Name
		name = strings.TrimSuffix(name, "-asc")
		name = strings.TrimSuffix(name, "-desc")
		foundFields[name] = true
	}

	// reflect.VisibleFields promotes embedded fields, so we should see:
	// - Value (from B, shadows A.Value)
	// - Count (promoted from A)
	// - Name (from B)

	if !foundFields["Value"] {
		t.Error("Expected Value field from B (should shadow A.Value)")
	}
	if !foundFields["Count"] {
		t.Error("Expected Count field promoted from embedded A")
	}
	if !foundFields["Name"] {
		t.Error("Expected Name field from B")
	}

	// reflect.VisibleFields returns both promoted and qualified paths for embedded fields:
	// - A.Value (qualified path to embedded field)
	// - Value (promoted, but shadowed by B.Value)
	// - A.Count (qualified path)
	// - Count (promoted from A)
	// - Name (from B)

	// Check that all expected fields exist
	expectedFields := []string{"A.Value", "A.Count", "Count", "Value", "Name"}
	for _, expected := range expectedFields {
		if !foundFields[expected] {
			t.Errorf("Expected field %s not found", expected)
		}
	}

	// Count dynamic filters
	// Should have 5: A.Value (dynamic tag), A.Count (int auto), Count (promoted, int auto), Name (dynamic tag), Value (dynamic tag, shadows A.Value)
	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	if dynamicFilterCount != 5 {
		t.Errorf("expected 5 dynamic filters (both qualified and promoted paths), got %d", dynamicFilterCount)
	}
}

func Test_ReflectSearchable(t *testing.T) {
	type Article struct {
		Title       string `reveald:"dynamic,searchable"`
		Description string `reveald:"searchable"`
		Body        string `reveald:"searchable"`
		Author      string `reveald:"dynamic"`
		Published   time.Time
	}

	features, _ := featureset.Reflect(reflect.TypeOf(Article{}))

	// Count dynamic filters and query filters
	dynamicFilterCount := 0
	queryFilterCount := 0
	var queryFilter *featureset.QueryFilterFeature

	for _, f := range features {
		switch ft := f.(type) {
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		case *featureset.QueryFilterFeature:
			queryFilterCount++
			queryFilter = ft
		}
	}

	// Should have 3 dynamic filters (Title, Author, Published)
	// Published time.Time gets an automatic dynamic filter
	if dynamicFilterCount != 3 {
		t.Errorf("expected 3 dynamic filters, got %d", dynamicFilterCount)
	}

	// Should have 1 query filter
	if queryFilterCount != 1 {
		t.Errorf("expected 1 query filter, got %d", queryFilterCount)
	}

	// Verify query filter was created
	if queryFilter == nil {
		t.Fatal("expected QueryFilterFeature to be created")
	}
}

func Test_ReflectWithCustomDefaults(t *testing.T) {
	type Product struct {
		Name        string
		Description string
		Price       float64
		Count       int
	}

	// Test with custom defaults
	features, sortOpts := featureset.Reflect(
		reflect.TypeOf(Product{}),
		featureset.WithDefaultAggSize(50),
		featureset.WithStringsDynamicByDefault(true),
		featureset.WithSortSuffixes(".desc", ".asc"),
	)

	// Count dynamic filters - with stringsDynamicByDefault, should have Name, Description, Price, Count
	dynamicFilterCount := 0
	for _, f := range features {
		if _, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFilterCount++
		}
	}

	// Should have 4 dynamic filters (all string fields + numeric fields)
	if dynamicFilterCount != 4 {
		t.Errorf("expected 4 dynamic filters with stringsDynamicByDefault, got %d", dynamicFilterCount)
	}

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check that custom suffixes are used
	foundCustomDesc := false
	foundCustomAsc := false
	for _, opt := range result.Sorting.Options {
		if strings.HasSuffix(opt.Name, ".desc") {
			foundCustomDesc = true
		}
		if strings.HasSuffix(opt.Name, ".asc") {
			foundCustomAsc = true
		}
	}

	if !foundCustomDesc {
		t.Error("expected to find sort options with custom .desc suffix")
	}
	if !foundCustomAsc {
		t.Error("expected to find sort options with custom .asc suffix")
	}
}

func Test_ReflectWithNoSortByDefault(t *testing.T) {
	type Simple struct {
		Name string `reveald:"dynamic"`
		Age  int
	}

	_, sortOpts := featureset.Reflect(
		reflect.TypeOf(Simple{}),
		featureset.WithSortableByDefault(false),
	)

	// Should not have any sorting options when sortable is disabled
	if len(sortOpts) != 0 {
		t.Error("expected no sorting options when sortableByDefault is false")
	}
}

func Test_ReflectWithCustomSearchParam(t *testing.T) {
	type Article struct {
		Title string `reveald:"searchable"`
		Body  string `reveald:"searchable"`
	}

	features, _ := featureset.Reflect(
		reflect.TypeOf(Article{}),
		featureset.WithSearchParamName("search"),
	)

	// Find the query filter feature
	var queryFilter *featureset.QueryFilterFeature
	for _, f := range features {
		if qf, ok := f.(*featureset.QueryFilterFeature); ok {
			queryFilter = qf
			break
		}
	}

	if queryFilter == nil {
		t.Fatal("expected QueryFilterFeature to be created")
	}

	// The query filter should use the custom param name
	// We can't directly test the internal field, but we can verify it was created
}

func Test_ReflectDefaultSortAsc(t *testing.T) {
	type Product struct {
		Name  string `reveald:"dynamic,defaultSort-asc"`
		Price float64
		Stock int
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the default sort option
	var defaultOption *reveald.ResultSortingOption
	for _, opt := range result.Sorting.Options {
		if opt.Selected {
			defaultOption = opt
			break
		}
	}

	if defaultOption == nil {
		t.Fatal("expected a default sort option to be selected")
	}

	// Verify the default is Name-asc
	if defaultOption.Name != "Name-asc" {
		t.Errorf("expected default sort option to be 'Name-asc', got '%s'", defaultOption.Name)
	}

	if !defaultOption.Ascending {
		t.Error("expected default sort option to be ascending")
	}
}

func Test_ReflectDefaultSortDesc(t *testing.T) {
	type Product struct {
		Name  string
		Price float64 `reveald:"defaultSort-desc"`
		Stock int
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the default sort option
	var defaultOption *reveald.ResultSortingOption
	for _, opt := range result.Sorting.Options {
		if opt.Selected {
			defaultOption = opt
			break
		}
	}

	if defaultOption == nil {
		t.Fatal("expected a default sort option to be selected")
	}

	// Verify the default is Price-desc
	if defaultOption.Name != "Price-desc" {
		t.Errorf("expected default sort option to be 'Price-desc', got '%s'", defaultOption.Name)
	}

	if defaultOption.Ascending {
		t.Error("expected default sort option to be descending")
	}
}

func Test_ReflectDefaultSortFirstFieldWins(t *testing.T) {
	type Product struct {
		Name  string  `reveald:"dynamic,defaultSort-asc"`
		Price float64 `reveald:"defaultSort-desc"` // This should be ignored
		Stock int
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the default sort option
	var defaultOption *reveald.ResultSortingOption
	for _, opt := range result.Sorting.Options {
		if opt.Selected {
			defaultOption = opt
			break
		}
	}

	if defaultOption == nil {
		t.Fatal("expected a default sort option to be selected")
	}

	// Verify the default is Name-asc (first field with defaultSort wins)
	if defaultOption.Name != "Name-asc" {
		t.Errorf("expected default sort option to be 'Name-asc' (first field wins), got '%s'", defaultOption.Name)
	}
}

func Test_ReflectDefaultSortWithJsonTag(t *testing.T) {
	type Product struct {
		Name  string  `json:"product_name" reveald:"dynamic,defaultSort-asc"`
		Price float64 `json:"price"`
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find the default sort option
	var defaultOption *reveald.ResultSortingOption
	for _, opt := range result.Sorting.Options {
		if opt.Selected {
			defaultOption = opt
			break
		}
	}

	if defaultOption == nil {
		t.Fatal("expected a default sort option to be selected")
	}

	// Verify the default uses the json tag name
	if defaultOption.Name != "product_name-asc" {
		t.Errorf("expected default sort option to be 'product_name-asc', got '%s'", defaultOption.Name)
	}

	if defaultOption.Value != "product_name" {
		t.Errorf("expected default sort option value to be 'product_name', got '%s'", defaultOption.Value)
	}
}

func Test_ReflectDefaultSortWithNoSort(t *testing.T) {
	type Product struct {
		Name  string `reveald:"dynamic,defaultSort-asc,no-sort"` // Conflicting tags
		Price float64
	}

	_, sortOpts := featureset.Reflect(reflect.TypeOf(Product{}))

	// Create a SortingFeature to test the options
	sortFeature := featureset.NewSortingFeature("sort", sortOpts...)
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify no default is selected (no-sort takes precedence)
	for _, opt := range result.Sorting.Options {
		if opt.Selected {
			t.Errorf("expected no default sort option when no-sort tag is used, but '%s' is selected", opt.Name)
		}
	}

	// Verify Name field doesn't appear in sort options
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Name") {
			t.Errorf("Name field should not have sort options due to no-sort tag, found: %s", opt.Name)
		}
	}
}

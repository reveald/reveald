package featureset_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/reveald/reveald"
	"github.com/reveald/reveald/featureset"
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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))
	if len(features) != 7 {
		t.Fatal("expected 7 features, got", len(features))
	}

	for _, f := range features {
		switch f.(type) {
		case *featureset.DynamicFilterFeature:
			// pass
		case *featureset.DynamicBooleanFilterFeature:
			// pass
		case *featureset.SortingFeature:
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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

	// Should have 1 sorting feature, no features for Ignored field
	if len(features) != 1 {
		t.Fatalf("expected 1 feature (sorting), got %d", len(features))
	}

	_, ok := features[0].(*featureset.SortingFeature)
	if !ok {
		t.Error("expected only a sorting feature")
	}
}

func Test_ReflectNoSortTag(t *testing.T) {
	type TTarget struct {
		Name    string
		Updated time.Time `reveald:"no-sort"`
	}

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

	// Name should have sorting options (2: asc and desc)
	// Updated should not have sorting options
	// Total should be 2 sort options
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

	if result.Sorting == nil {
		t.Fatal("expected sorting information in result")
	}

	if len(result.Sorting.Options) != 2 {
		t.Fatalf("expected 2 sort options (Name-asc, Name-desc), got %d", len(result.Sorting.Options))
	}

	// Verify that Updated field sorting options are not present
	for _, opt := range result.Sorting.Options {
		if opt.Name == "Updated-asc" || opt.Name == "Updated-desc" {
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

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

	// Test by executing the feature with a mock request/builder
	req := reveald.NewRequest()
	builder := reveald.NewQueryBuilder(req, "test-index")

	// Execute the feature to get the result with sorting options
	result, err := sortFeature.Process(builder, func(qb *reveald.QueryBuilder) (*reveald.Result, error) {
		return &reveald.Result{
			Aggregations: make(map[string][]*reveald.ResultBucket),
		}, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Sorting == nil {
		t.Fatal("expected sorting information in result")
	}

	// 4 fields * 2 directions = 8 sort options
	if len(result.Sorting.Options) != 8 {
		t.Fatalf("expected 8 sort options, got %d", len(result.Sorting.Options))
	}

	// Verify Name field uses json tag override
	hasNameDesc := false
	hasNameAsc := false
	for _, opt := range result.Sorting.Options {
		if opt.Name == "Name-desc" {
			hasNameDesc = true
			if opt.Value != "name-overridden.keyword" {
				t.Errorf("expected Name-desc to use json tag 'name-overridden.keyword', got %s", opt.Value)
			}
		}
		if opt.Name == "Name-asc" {
			hasNameAsc = true
		}
	}

	if !hasNameDesc || !hasNameAsc {
		t.Error("expected both Name-desc and Name-asc sort options")
	}
}

func findSortingFeature(features []reveald.Feature) *featureset.SortingFeature {
	for _, f := range features {
		if sf, ok := f.(*featureset.SortingFeature); ok {
			return sf
		}
	}
	return nil
}

func Test_ReflectHistogramFeature(t *testing.T) {
	type TTarget struct {
		Price  float64 `reveald:"histogram,interval=50"`
		Count  int     `reveald:"histogram,interval=10"`
		Rating float32 `reveald:"histogram"`
	}

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

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
		Created time.Time `reveald:"date-histogram,interval=day"`
		Updated time.Time `reveald:"date-histogram,interval=month"`
	}

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

	dateHistogramFeatures := []*featureset.DateHistogramFeature{}
	for _, f := range features {
		if dhf, ok := f.(*featureset.DateHistogramFeature); ok {
			dateHistogramFeatures = append(dateHistogramFeatures, dhf)
		}
	}

	if len(dateHistogramFeatures) != 2 {
		t.Fatalf("expected 2 date histogram features, got %d", len(dateHistogramFeatures))
	}

	// Verify that DynamicFilterFeature is NOT created for date-histogram fields
	dynamicFeatures := []*featureset.DynamicFilterFeature{}
	for _, f := range features {
		if df, ok := f.(*featureset.DynamicFilterFeature); ok {
			dynamicFeatures = append(dynamicFeatures, df)
		}
	}

	if len(dynamicFeatures) != 0 {
		t.Fatalf("expected 0 dynamic filter features for date-histogram fields, got %d", len(dynamicFeatures))
	}
}

func Test_ReflectCombinedTags(t *testing.T) {
	type TTarget struct {
		Name     string    `reveald:"dynamic,no-sort"`
		Price    float64   `reveald:"histogram,interval=100"`
		Created  time.Time `reveald:"date-histogram,interval=day"`
		Category string    `reveald:"dynamic"`
		Ignored  string    `reveald:"ignore"`
	}

	features := featureset.Reflect(reflect.TypeOf(TTarget{}))

	// Count each feature type
	histogramCount := 0
	dateHistogramCount := 0
	dynamicFilterCount := 0
	sortingCount := 0

	for _, f := range features {
		switch f.(type) {
		case *featureset.HistogramFeature:
			histogramCount++
		case *featureset.DateHistogramFeature:
			dateHistogramCount++
		case *featureset.DynamicFilterFeature:
			dynamicFilterCount++
		case *featureset.SortingFeature:
			sortingCount++
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

	if sortingCount != 1 {
		t.Errorf("expected 1 sorting feature, got %d", sortingCount)
	}

	// Verify Name field has no-sort respected
	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

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

	features := featureset.Reflect(reflect.TypeOf(Product{}))

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

	features := featureset.Reflect(reflect.TypeOf(Person{}))

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

	// Verify sorting feature has nested field paths
	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

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

	// Check for nested field sort options
	hasContactAddressCity := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Contact.Address.City") {
			hasContactAddressCity = true
			// Verify json path is correct
			if !strings.Contains(opt.Value, "city.keyword") {
				t.Errorf("expected Contact.Address.City to use json path with 'city.keyword', got: %s", opt.Value)
			}
		}
	}

	if !hasContactAddressCity {
		t.Error("expected sort options for Contact.Address.City")
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

	features := featureset.Reflect(reflect.TypeOf(Document{}))

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

	features := featureset.Reflect(reflect.TypeOf(Product{}))

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
		Count      int     `reveald:"agg-size=500"`
		Score      float64 `reveald:"agg-size=30"`
		Name       string  `reveald:"dynamic,agg-size=75,no-sort"`
		Timestamp  time.Time `reveald:"agg-size=150"`
	}

	features := featureset.Reflect(reflect.TypeOf(Data{}))

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
	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

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

	features := featureset.Reflect(reflect.TypeOf(Product{}))

	// Verify sorting feature uses json tags at all nesting levels
	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

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

	// Verify top-level field uses json tag
	foundTopLevel := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Name") {
			foundTopLevel = true
			if !strings.Contains(opt.Value, "product_name.keyword") {
				t.Errorf("expected Name to use json tag 'product_name.keyword', got: %s", opt.Value)
			}
		}
	}
	if !foundTopLevel {
		t.Error("expected sort option for Name field")
	}

	// Verify nested struct field uses json tags at all levels
	// Field path: Details.SKU
	// JSON path should be: product_details.product_sku.keyword
	foundNested := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Details.SKU") {
			foundNested = true
			if opt.Value != "product_details.product_sku.keyword" {
				t.Errorf("expected Details.SKU to use json path 'product_details.product_sku.keyword', got: %s", opt.Value)
			}
		}
	}
	if !foundNested {
		t.Error("expected sort option for Details.SKU field")
	}

	// Verify deeply nested field uses json tags at all levels
	// Field path: Details.Pricing.Currency
	// JSON path should be: product_details.pricing_info.currency_code.keyword
	foundDeeplyNested := false
	for _, opt := range result.Sorting.Options {
		if strings.Contains(opt.Name, "Details.Pricing.Currency") {
			foundDeeplyNested = true
			if opt.Value != "product_details.pricing_info.currency_code.keyword" {
				t.Errorf("expected Details.Pricing.Currency to use json path 'product_details.pricing_info.currency_code.keyword', got: %s", opt.Value)
			}
		}
	}
	if !foundDeeplyNested {
		t.Error("expected sort option for Details.Pricing.Currency field")
	}
}

func Test_ReflectUnsignedIntegers(t *testing.T) {
	type Stats struct {
		Count   uint    `reveald:"agg-size=50"`
		Port    uint16
		ID      uint64  `reveald:"histogram,interval=100"`
		Flags   uint8
	}

	features := featureset.Reflect(reflect.TypeOf(Stats{}))

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
		Name   *string  `reveald:"dynamic"`
		Count  *int
		Price  *float64 `reveald:"histogram,interval=10"`
		Active *bool
		When   *time.Time `reveald:"date-histogram,interval=day"`
	}

	features := featureset.Reflect(reflect.TypeOf(Optional{}))

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

	features := featureset.Reflect(reflect.TypeOf(Tagged{}))

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
	sortFeature := findSortingFeature(features)
	if sortFeature == nil {
		t.Fatal("expected a sorting feature")
	}

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
		IDs        []uint64  `reveald:"agg-size=500"`
		OptName    *string   `reveald:"dynamic"`
		OptCount   *int      `reveald:"histogram,interval=50"`
		Tags       []string  `reveald:"dynamic,no-sort"`
		Port       uint16
	}

	features := featureset.Reflect(reflect.TypeOf(Complex{}))

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

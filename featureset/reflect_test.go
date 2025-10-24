package featureset_test

import (
	"reflect"
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

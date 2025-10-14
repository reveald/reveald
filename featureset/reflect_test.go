package featureset_test

import (
	"reflect"
	"testing"
	"time"

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
		case *featureset.SortingFeature:
			// pass
		default:
			t.Fatal("unexpected feature type", reflect.TypeOf(f))
		}
	}
}

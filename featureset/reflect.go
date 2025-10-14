package featureset

import (
	"reflect"
	"strings"
	"time"

	"github.com/reveald/reveald"
)

func Reflect(t reflect.Type) []reveald.Feature {

	sortOpts := make([]SortingOption, 0)
	featureOpts := make([]reveald.Feature, 0)

	for _, f := range reflect.VisibleFields(t) {
		rtag := f.Tag.Get("reveald")

		if rtag != "ignore" {

			fieldName := f.Name
			switch f.Type.Kind() {
			case reflect.String:
				fieldName += ".keyword"

			case reflect.Bool:
				featureOpts = append(featureOpts, NewDynamicBooleanFilterFeature(f.Name))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
			case reflect.TypeOf(time.Time{}).Kind():
				featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
			}

			if !strings.Contains(rtag, "no-sort") {
				sortOpts = append(sortOpts, WithSortOption(f.Name+"-desc", fieldName, false))
				sortOpts = append(sortOpts, WithSortOption(f.Name+"-asc", fieldName, true))
			}

			if strings.Contains(rtag, "dynamic") {
				switch f.Type.Kind() {
				case reflect.String:
					fieldName += ".keyword"
					featureOpts = append(featureOpts, NewDynamicFilterFeature(f.Name, WithAggregationSize(100)))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
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

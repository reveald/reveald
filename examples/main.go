package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/reveald/reveald/v1"
	"github.com/reveald/reveald/v1/featureset"
)

func main() {
	b, err := reveald.NewElasticBackend(
		[]string{"http://127.0.0.1:9200/"})
	if err != nil {
		panic(err)
	}

	e := reveald.NewEndpoint(b, reveald.WithIndices("the-idx"))
	err = e.Register(
		featureset.NewStaticFilterFeature(featureset.WithRequiredProperty("maybe_field")),
		featureset.NewStaticFilterFeature(featureset.WithRequiredValue("status.keyword", "Active")),
		featureset.NewDynamicFilterFeature("text_field"),
		featureset.NewHistogramFeature("range_field", featureset.WithInterval(1000)))
	if err != nil {
		panic(err)
	}

	req := reveald.NewRequest(
		reveald.NewParameter("text_field", "Third"))

	res, err := e.Execute(context.TODO(), req)
	if err != nil {
		panic(err)
	}

	d, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(d))
}

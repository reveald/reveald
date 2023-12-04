package featureset

const defaultAggregationSize = 10

type AggregationFeature struct {
	size int
}

type AggregationOption func(*AggregationFeature)

func WithAggregationSize(size int) AggregationOption {
	return func(af *AggregationFeature) {
		af.size = size
	}
}

func buildAggregationFeature(opts ...AggregationOption) AggregationFeature {
	agg := AggregationFeature{
		size: defaultAggregationSize,
	}

	for _, opt := range opts {
		opt(&agg)
	}

	return agg
}

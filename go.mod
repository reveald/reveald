module github.com/reveald/reveald

go 1.18

require (
	github.com/olivere/elastic/v7 v7.0.32
	github.com/stretchr/testify v1.8.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

exclude (
	github.com/aws/aws-sdk-go v1.43.21
	github.com/jmespath/go-jmespath v0.4.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/smartystreets/go-aws-auth v0.0.0-20180515143844-0c1422d1fdb9
	go.opencensus.io v0.23.0
	go.opentelemetry.io/otel v1.5.0
	go.opentelemetry.io/otel/trace v1.5.0
)

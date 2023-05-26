module github.com/stoewer/go-nakadi

go 1.19

require (
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/jarcoal/httpmock v1.3.0
	github.com/opentracing/basictracer-go v1.1.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.3
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/gogo/protobuf v1.3.1 => github.com/gogo/protobuf v1.3.2

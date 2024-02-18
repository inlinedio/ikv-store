module github.com/inlinedio/ikv-store

go 1.21.6

require (
	github.com/stretchr/testify v1.8.4
	google.golang.org/grpc v1.61.1
	google.golang.org/protobuf v1.32.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.18.0 // indirect
	golang.org/x/sys v0.14.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231106174013-bbf56f31fb17 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/inlinedio/ikv-store/ikv-go-client => ./ikv-go-client
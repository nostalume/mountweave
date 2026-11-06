module github.com/nostalume/mountweave/protocols/s3

go 1.24.0

require (
	github.com/aws/aws-sdk-go-v2 v1.47.5
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.6
	github.com/nostalume/mountweave v0.1.0
	github.com/nostalume/mountweave/drivers/s3 v0.1.0
	go.etcd.io/bbolt v1.4.3
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.25 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.8 // indirect
	github.com/aws/smithy-go v1.28.7 // indirect
	golang.org/x/sys v0.29.0 // indirect
)

replace github.com/nostalume/mountweave => ../..

replace github.com/nostalume/mountweave/drivers/s3 => ../../drivers/s3

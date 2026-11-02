module github.com/nostalume/mountweave/drivers/pool

go 1.24.0

require (
	github.com/klauspost/reedsolomon v1.13.3
	go.etcd.io/bbolt v1.4.3
	github.com/nostalume/mountweave v0.1.0
)

require (
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/nostalume/mountweave => ../..

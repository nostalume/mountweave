module github.com/nostalume/mountweave/protocols/sftp

go 1.25.0

require (
	github.com/nostalume/mountweave v0.2.0
	github.com/pkg/sftp v1.13.12
	github.com/pkg/sftp/v2 v2.0.0-alpha2
)

require (
	github.com/kr/fs v0.1.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/nostalume/mountweave => ../..

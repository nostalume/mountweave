module github.com/nostalume/mountweave/drivers/sftp

go 1.25.0

require (
	github.com/nostalume/mountweave v0.1.0
	github.com/nostalume/mountweave/protocols/sftp v0.0.0
	github.com/pkg/sftp/v2 v2.0.0-alpha2
	golang.org/x/crypto v0.54.0
)

require golang.org/x/sys v0.47.0 // indirect

replace github.com/nostalume/mountweave => ../..

replace github.com/nostalume/mountweave/protocols/sftp => ../../protocols/sftp

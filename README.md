# Mountweave

Mountweave is a Go library for mounting local storage and cloud drives through
one file API. Browse directories, stream files and copy data between mounts in
your own application.

Inspired by OpenList, Mountweave gives the host control over identity and
access. Core uses Go's standard library. Add the Drivers and protocol endpoints
your application needs.

## Install a release

From your application's module, install Core with Go 1.24 or newer.

```sh
go get github.com/nostalume/mountweave@v0.2.0
```

Optional modules have independent versions and Go requirements.
[Module tags](https://github.com/nostalume/mountweave/tags) · [Core API](https://pkg.go.dev/github.com/nostalume/mountweave@v0.2.0)

## Mount a local directory

Create a Core with `core.New` and supply your authorization policy in
`core.Options`. The helper mounts a directory at `/files`.

```go
package host

import (
	"errors"

	core "github.com/nostalume/mountweave"
	"github.com/nostalume/mountweave/drivers/local"
)

func mountDirectory(files *core.Core, directory string) error {
	driver, err := local.New(directory)
	if err != nil {
		return err
	}
	if _, err := files.Install("/files", driver); err != nil {
		return errors.Join(err, driver.Close())
	}
	return nil
}
```

After a successful mount, Core owns the Driver. Use `List` to browse the mount. Pass
an authenticated principal and an address such as `core.Path("/files/report.txt")`
to `Open` to read a file. Close the returned body and call `Core.Close` when
the application shuts down.

## Documentation

- Find a storage service for your workload. [Driver recommendations](docs/driver-recommendations.md)
- Check which operations and guarantees a Driver supports. [Driver capabilities](docs/drivers.md)
- Understand the file model, mount ownership and optional components. [Architecture](docs/architecture.md)
- Integrate with the JSON HTTP endpoint. [OpenAPI](api/openapi.json)

## Contributing

Run the checks for the modules you change. Keep test services local and use
synthetic credentials. [Contributing guide](CONTRIBUTING.md)

## License

Mountweave is available under the [MIT License](LICENSE).

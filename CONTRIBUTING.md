# Contributing

Use Go 1.24 or newer for Core and Go 1.27 or newer for a full repository check.
Each optional module declares its minimum Go version in `go.mod`.

## Verify a change

Run the existing test that exercises the changed behavior through a public
entry point. Use `-count=1` for a regression probe, then run `go test ./...` and
`go vet ./...` from the affected module. Exercise consumers when a shared
contract changes.

Run `go test -race ./...` for changes to concurrency, cancellation or resource
ownership. Use verbose or JSON output to check whether native filesystem cases
ran. For a module dependency change, verify that a separate Go module resolves
the candidate versions without local replacements.

Before integration, check all modules from the repository root in a POSIX shell.

```sh
export GOWORK=off
modules=$(git ls-files --cached --others --exclude-standard -- 'go.mod' ':(glob)**/go.mod') || exit
while IFS= read -r mod; do
    dir=$(dirname "$mod")
    go -C "$dir" test ./... || exit
    go -C "$dir" vet ./... || exit
done <<EOF
$modules
EOF
```

Use temporary storage, loopback services and synthetic credentials in functional
tests. Extend existing end-to-end cases for conflict, cancellation and partial
effects. For an injected fault, check the error, effect and resource cleanup.
Retire a duplicate case when a surviving test detects the same fault and
checks the same effects. Record the setup when reporting a benchmark.

## Prepare a module release

Use the batch tool to plan and verify candidate versions. [Release guide](docs/releases.md)

## Prepare a contribution

- Keep optional dependencies in the modules that use them.
- Use English for code, comments, schemas and public documentation.
- Document behavioral contracts with the owning Go package.
- Update the relevant reference for native options or wire changes. [Driver capabilities](docs/drivers.md) · [OpenAPI](api/openapi.json)
- Explain API changes and their compatibility impact. Keep credentials, private notes and generated caches out of the contribution.

Submit material you can contribute under the project's MIT license. Keep required
attribution and license text with adapted material. When distributing a build,
include the license and notice files for the dependencies it contains. [Project license](LICENSE)

# Release Go modules

Each Go module has its own version. The root tag is `vX.Y.Z`. A submodule
tag starts with its directory, as in `drivers/s3/v0.2.0` or
`protocols/sftp/v0.1.0`. Use a patch version for compatible fixes. Use a
minor version for new public features, compatibility breaks during v0, or
an increase in the minimum Go version. Release a dependency in the same
batch when a module needs a version that has yet to ship.
[Go module versioning](https://go.dev/ref/mod#mapping-versions-to-commits)

## Prepare a candidate

Run from the repository root with committed `go.mod` files, Git, and Go 1.27
or newer for the full batch. Set `GOWORK=off` if you use a local Go workspace.
The redirection below saves UTF-8 in Bash and PowerShell 7. Keep the plan
outside the checkout or under the ignored `work/` directory.

```sh
git fetch origin --tags
go run ./internal/release plan -bump patch > ../release-plan.json
go run ./internal/release prepare ../release-plan.json
```

The planner selects changed modules and starts untagged modules at `v0.1.0`.
Use `-all` to include every module or `-bump minor` for a minor bump. You can
also pass explicit targets such as `plan drivers/dropbox@v0.1.1`.

Review the `go.mod` changes. When releasing Core, update the README installation
command and API link to the planned root tag. Check the README's Go requirement
and example against the candidate. Commit these changes together, then verify
that commit with the same plan.

```sh
go run ./internal/release verify ../release-plan.json > ../release-receipt.json
```

The verifier creates a separate temporary Go project for each module.
Each project resolves its planned version from an isolated Git repository,
checks the selected internal dependencies, and builds the module's packages.
These projects share download and build caches within the batch. The JSON
receipt records the verified commit and each module's checksums.

The checkout's relative replacements support development. Go ignores them
when a module is used as a dependency. CI runs tests and vet on Windows and
Linux, with race checks on Linux. The verifier builds with the Go toolchain
that started it. Verify each declared minimum Go version before publishing.
[Go replacement rules](https://go.dev/ref/mod#go-mod-file-replace)

## Publish a verified batch

Create all planned tags at the verified commit as part of one release
operation. Push the planned tag refs together with `git push --atomic`
when the remote supports it. After an interrupted push, check where the
remote tags point before retrying. Keep published tags on their original
commits.

Once the refs are visible, use `go mod download -json` for each planned
module and version. Use a fresh cache, the intended public Go proxy and
normal checksum verification. Match the downloaded `Sum` and `GoModSum` to the
candidate receipt before announcing availability. A proxy can
take time to serve a new tag. Retry the lookup if needed and keep the
verified commit and tags fixed.
[Git atomic push](https://git-scm.com/docs/git-push#Documentation/git-push.txt---atomic) ·
[Go release workflow](https://go.dev/doc/modules/release-workflow)

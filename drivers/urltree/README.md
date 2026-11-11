# URL tree source

Mount a configured set of HTTPS files with the standard Core path API.

```go
source, err := urltree.New(urltree.Options{Files: []urltree.File{
    {Path: "/docs/report.pdf", URL: "https://downloads.example/report.pdf"},
}})
if err != nil {
    return err
}
mount, err := storage.Install("/downloads", source)
if err != nil {
    return errors.Join(err, source.Close())
}
```

Paths are absolute within the mount. Parent directories are inferred, names
are exact and case-sensitive, and duplicate paths or file and directory
collisions fail during construction. An empty configuration mounts an empty
root. Construction, Stat, and List make no network requests. Stat and List
describe the configured view and leave remote size and modification time
unknown.

Open makes one streaming HTTPS GET to the selected URL. Core Copy uses the
same source. Ranges and length reflect the actual HTTP response. A URL can
contain a signed query, which is sent unchanged and omitted from metadata
and driver request errors. URLs containing user information or a fragment
are rejected, and redirects are refused. An optional borrowed Transport
must avoid injecting account credentials or exposing private URL details
from an adopted response body.

The directory view is fixed for the lifetime of the driver. Content and
availability depend on the configured endpoint. The driver supports
ordinary reads and does not import or modify remote resources. Verification
uses a sealed HTTPS fixture.

// Package control manages desired mount definitions using host-owned storage and factories.
//
// # Optional mount control
//
// control.New accepts a Core, a host Store, driver Factories and a separate
// management authorizer. The Store atomically compares revisions on Commit and
// owns durable acknowledgement; Load/List return independent definitions. No
// storage backend or database is required by Core. A Definition contains an ID,
// revision, prefix, driver kind, native JSON input, secret references, enabled
// intent and fixed aliases. Factories decode their own typed options and resolve
// secret references; configuration output must be redacted by its API consumer.
//
// Save commits desired input for the next explicit Apply/Restore. It does not
// change the current instance. Apply selects an exact revision, validates before
// retirement, waits for the old instance, then builds and installs the new one and
// restores aliases. Replacement accepts downtime and has no automatic rollback.
// An already active revision reuses its instance and resumes missing aliases.
// Activation distinguishes the requested revision from the last applied revision,
// observed Mount status and confirmed aliases. Root installation can succeed even
// when an alias fails. Observe reads Core's facts rather than maintaining a router.
// Load/List query desired definitions under separate load/list management actions; List filters each definition through load authority. Stop leaves saved intent unchanged; Restore loads latest desired definitions
// and returns per-definition activation results.
//
// Factory Validate must not acquire an exclusive refresh writer. Build loads the
// latest credential checkpoint after retirement; static configuration must not
// reset a rotated token. Successful Install transfers Driver ownership to Core.
// A failed Build must return any still-owned Driver together with its error;
// control closes an unadopted Driver and retains failed or unfinished cleanup.
// Build must retain/clean any partial resources that do not implement Driver.
// Borrowed clients remain host-owned. A factory sharing a rotating credential
// across instances must supply explicit leases or writer admission; sharing an
// owned credential.Source pointer between independently closing drivers is invalid.
// Use one controller for its managed instances; cross-controller/process writer
// coordination belongs to the host/native owner, not the definition revision CAS.
//
// Controller.Close stops admission and waits for managed retirement, leaving Core
// and Store host-owned. Context bounds cleanup waiting, not native cleanup itself.
// Observe and Stop remain available after Close. No background reconciler runs.
package control

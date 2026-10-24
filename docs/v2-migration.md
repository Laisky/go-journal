# Journal v2 source migration

This candidate declares module `github.com/Laisky/go-journal/v2`. It is not a v1 patch and no v2 release or tag is created by this PR. Published v1 tags and on-disk journal data remain unchanged.

Change imports to `/v2`. `WithLogger` and `NewLegacyLoader` now accept the journal-owned five-method `Logger` interface (`Debug`, `Info`, `Warn`, `Error`, `Panic`, with structured Zap fields). A plain concrete Zap logger, the legacy SDK logger, or a small custom implementation satisfies this interface. No logger factory or SDK-wide upgrade is required.

The former `WithLoggerInterface` and `NewLegacyLoaderWithLoggerInterface` overloads are removed; use the ordinary names. The exported mutable `journal.Logger` concrete default is removed: construct a caller-owned SDK logger and pass `WithLogger`. Exact function-variable signatures using `*utils.LoggerType` must also change. No logger factory, fluent return type, level API or runtime conversion is required by the journal.

Nil implementations are rejected, including typed nils. Reflection is confined to nil validation, never conversion or unwrapping. Storage, funding, retained replay, ACK/retirement and compression contracts are unchanged. Process crashes still assume intact persistent storage; ACKs do not establish sink durability.

The interface shape also matches current `go-utils/v6/log.LoggerT`, but combining that module with old v1 root utilities currently exposes a separate generic-deque dependency conflict. This PR does not claim that mixed dependency graph works.

Source integration uses immutable dependency revisions and local replacements only while qualifying this unmerged candidate. A coordinated version/branch and merge decision is required before release or consumer adoption.

The runtime module no longer imports the broad utility SDK. Replay locking uses the same compare-and-swap transitions directly; timestamps use UTC wall time without an SDK background clock; opt-in GC remains nonblocking with the same collection calls. A private gzip-member writer preserves the standard gzip algorithm, buffering and footer/reset boundaries. The existing 10 SDK/dependency-security fixtures have an independent test module and still execute in the same CI job (all original 31 tests plus three gzip regressions), without becoming consumer runtime dependencies.

The root module retains explicit security version floors for unused transitive metadata (Goldmark and golang.org/x modules). SCM scanners can inspect the complete module graph even when the vulnerable packages are not imported. The existing fast gate checks selected versions; removing unused modules is allowed, selecting affected old versions is not. `go mod tidy` may remove these deliberate metadata-only requirements: run the fast gate after dependency maintenance and preserve the security floors. The isolated SDK fixtures remain separate; no SDK or JWT runtime dependency is reintroduced.

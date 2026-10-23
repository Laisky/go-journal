# Journal v2 source migration

This candidate declares module `github.com/Laisky/go-journal/v2`. It is not a v1 patch and no v2 release or tag is created by this PR. Published v1 tags and on-disk journal data remain unchanged.

Change imports to `/v2`. `WithLogger` and `NewLegacyLoader` now accept the journal-owned five-method `Logger` interface (`Debug`, `Info`, `Warn`, `Error`, `Panic`, with structured Zap fields). A plain concrete Zap logger, the legacy SDK logger, or a small custom implementation satisfies this interface. No logger factory or SDK-wide upgrade is required.

The former `WithLoggerInterface` and `NewLegacyLoaderWithLoggerInterface` overloads are removed; use the ordinary names. The exported mutable `journal.Logger` concrete default is removed: construct a caller-owned SDK logger and pass `WithLogger`. Exact function-variable signatures using `*utils.LoggerType` must also change. No logger factory, fluent return type, level API or runtime conversion is required by the journal.

Nil implementations are rejected, including typed nils. Reflection is confined to nil validation, never conversion or unwrapping. Storage, funding, retained replay, ACK/retirement and compression contracts are unchanged. Process crashes still assume intact persistent storage; ACKs do not establish sink durability.

The interface shape also matches current `go-utils/v6/log.LoggerT`, but combining that module with old v1 root utilities currently exposes a separate generic-deque dependency conflict. This PR does not claim that mixed dependency graph works.

Source integration uses immutable dependency revisions and local replacements only while qualifying this unmerged candidate. A coordinated version/branch and merge decision is required before release or consumer adoption.

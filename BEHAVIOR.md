# Public journal behavior and process-level acceptance

## Contract, before implementation

The oracle is the application's original record manifest and an independent
synchronized downstream ledger. Internal maps, counters, encoder buffers and
message totals are not proof of delivery. Tests call exported APIs from
`package journal_test`; process cases launch a fresh consumer of the public
library and exchange commands over standard input/output.

A caller-supplied identity is a nonnegative int64 and identifies immutable
content. WriteData alone is not a durable receipt: a caller promising persistence
must wait for a successful Sync. A write or Sync error has an unknown outcome;
it does not prove rollback or absence. Close remains an idempotent resource
cleanup API, not a substitute for an error-checked Sync.

Replay is an ownership-transfer protocol, not a read-only iterator. Before
requesting the next record or EOF, the application must either complete a
replacement WriteData or durably deliver the record and write its ACK. It must
not advance past a failed transfer. LoadLegacyBuf synchronizes replacements and
ACKs before reclaiming old files. On a replay error the lease is released;
reacquire LockLegacy before retrying. An application abandoning a record must
retain and retry it, or restart the snapshot, rather than silently continuing.

Identical redelivery is allowed after interrupted processing or a snapshot reset.
Changed payloads, invented events and missing accepted events are not allowed.
Completed ACKs suppress replay, and out-of-order ACK completion must not roll the
recovered maximum ID backwards when old segments are reclaimed. Applications
still must not deliberately reuse IDs for new content.

One live Journal owns a directory. Start holds a persistent `.journal.lock` inode
until Close; SIGKILL releases the operating-system lock. Never unlink that file
to bypass an owner: doing so could create two independently locked inodes. A
failed/canceled Start is retryable; Close is terminal for that object. Writes,
Sync and rotation on unusable objects return errors instead of panicking.

## Behavioral matrix

| User journey | External assertion | Test family |
|---|---|---|
| Append, ACK none/some/all, rotate, restart repeatedly | Exact identities and full nested payloads survive; only pending records replay | RestartContract |
| Pause a replay lease, append, rotate, finish and reopen | Newly sealed, unread records are not reclaimed | PausedReplayAndRotationPreserveAllMessages |
| Highest ID ACKed in an older segment | Reclamation retains the recovered identity frontier | ReclaimPreservesIdentityFrontier |
| Switch gzip/plain across restarts | Each old segment remains readable with its own encoding | CompressionCanChangeAcrossRestart |
| ACK truncation/corruption, then repair | Neither a high-water result nor successful replay/EOF hides corruption; retry works | AcknowledgementCorruptionFailsClosedAndRetries, MalformedIDStreamIsRejected |
| Old-file deletion refused, then repaired | Error is not reported as successful EOF; cleanup remains retryable | CleanupFailureIsNotEOF |
| Replacement segment creation refused | Current writer remains usable; retrying rotation preserves both records | FailedRotationKeepsWriterUsable |
| Concurrent write, ACK, Flush, Sync, Rotate | Race-free operations and exact recovered pending records | ConcurrentPublicOperations |
| Duplicate owners in one/two processes | Second owner is refused; reopening after Close/SIGKILL succeeds | DirectoryHasSingleOwner, E2EProcessOwnership |
| Canceled startup, invalid configuration, closed APIs | Explicit errors; retry/idempotent cleanup without panic | CanceledStartCanRetry, InvalidOptionsRejected, LifecycleMisuseReturnsError, EncoderClosedOperations, ReplayAfterCloseReturnsError |
| Clock moves backwards; malformed or exhausted filenames | No collision, panic or undiscoverable generated sequence | FileNamesStayDiscoverable, FuzzBehaviorFileNames |
| Unrelated lookalike file in directory | Not interpreted as a WAL or reclaimed | UnrelatedFilesAreNotRecoveryInput |
| Fractional TTL and refresh | No early expiry caused by second truncation; normal hits remain non-consuming | FractionalTTLDoesNotExpireEarly |
| uint32 boundary and int64 ACK streams | MaxUint32 differs from zero; invalid bases/deltas and unrepresentable bitmap IDs are rejected | BitmapPreservesUint32Boundary, BitmapDecodeRejectsUnrepresentableIDs, MalformedIDStreamIsRejected |
| Repeated real process crashes | Independently delivered payloads reconcile with the original manifest | E2ECrashDelivery |
| Kernel-enforced file-size failure | Failed write is not acknowledged; earlier synchronized records survive a crash | E2EFileLimitFailurePreservesAcceptedPrefix |
| Many ACK segments under descriptor limit | Recovery works with 96 segments and RLIMIT_NOFILE=64 | E2ERecoveryWithDescriptorBudget |
| Crash before first record | Plain/gzip empty segments do not invent events or block recovery | E2EEmptyCrashSegments |
| Intentionally broken delivery evidence | Missing, extra, changed, identity-colliding and settled-ACK records fail; identical retries pass | DeliveryOracleRejectsFalseSuccess |

All names have the `TestBehavior` prefix, except the fuzz function. Process tests
are Linux-specific because they exercise SIGKILL and kernel resource limits; the
other contracts use public Go APIs and temporary files. A helper process is not a
real Elasticsearch/Kafka deployment, and no network server is substituted for
the journal's storage implementation.

## Changes made after reproducing failures

Startup now obtains exclusive directory ownership. Lifecycle checks and locked
Flush protect closed/uninitialized encoders and concurrent rotation. Rotation
prepares both replacement files and encoders before switching, synchronizes the
old writer, and never overwrites an existing segment. Filename validation also
prevents inherited gzip suffixes from disagreeing with a new compression mode.

ACK decoding errors are propagated rather than logged as success. Decoding is
scoped to one descriptor at a time. Cleanup errors retain the retry ledger;
data is removed before obsolete ACK files, with a directory Sync before cleanup
is reported complete. The newest ACK file and the file containing the highest
ACK are retained (at most two) so out-of-order completion cannot lower the
identity frontier. This requires reading ACK metadata during cleanup; it is a
correctness change, not an unmeasured performance claim.

A refreshed snapshot invalidates a partially consumed replay cursor. Restarting
that scan may repeat identical data, but never authorizes deletion of unread
newly sealed segments. The existing incomplete-tail evidence policy is retained;
this change does not convert arbitrary corruption into EOF.

ID decoding checks the nonnegative int64 domain and rejects underflow/overflow.
Bitmap decoding explicitly rejects IDs outside the bitmap's uint32 domain.
The uint32 set distinguishes zero from MaxUint32. Two-generation TTL retention
is retained, but deadline precision is nanoseconds rather than whole seconds.

## Red/green verification

`.scripts/verify_behavior_regressions.py` copies the same public test files onto
baseline `979ec19dde737bb4fec9ada2e39a99267c028706`, compiles both revisions, and
requires **20 named failures** on the baseline, **three independent controls**
passing on both, and all 23 cases passing on the candidate. A baseline test may
contain several subcases; 20 is not a claim of 20 distinct root causes.
Compilation errors, skipped tests and timeouts do not count as reproductions.
The old failed-rotation path can panic in its already-running flush worker;
that specific compiled execution and stack are recorded as a runtime failure.

An existing 40,000-entry TTL test relied on finishing all concurrent setup within
one wall-clock second. Race instrumentation exposed that assumption. It now uses
`testing/synctest`, retaining the original lifetime/expiration assertions while
removing machine speed from the contract. Fixture compilation mistakes and this
timing correction are not counted as newly discovered production defects.

## Commands and validation boundary

Use Go 1.27 with the repository's own dependency graph:

```sh
go mod verify
go build -mod=readonly ./...
go vet -mod=readonly ./...
go test -mod=readonly -count=1 -timeout=180s ./...
go test -mod=readonly -race -count=5 -shuffle=on -timeout=180s ./...
python3 .scripts/verify_behavior_regressions.py --artifacts /tmp/journal-red-green
go test -mod=readonly -run '^$' -fuzz '^FuzzBehaviorFileNames$' -fuzztime=15s -parallel=2 .
```

**Local acceptance limitation (2026-09-23):** the available offline cache lacked
several versions from this repository's own graph, including go-utils v1.12.9.
Tests were executed with Go 1.27.1 and an external modfile based on go-fluentd's
consumer graph (go-utils v1.14.6). The exact Linux files of directio v1.0.5 were
provided through an external local replacement. The repository's go.mod and
go.sum were not changed. Both red/green versions used the same external graph.
This is valid consumer-graph evidence, **not standalone-module or hosted-CI
acceptance**. The included read-only behavioral workflow is prepared to run the
native graph; its results must be checked before merge. The supplied validation
logs preserve the unavailable native-graph attempt as well as executed checks.

No physical power loss, arbitrary filesystem corruption, all platforms, unlimited
backlog or exactly-once delivery is claimed. SIGKILL leaves the OS page cache
intact, and Sync relies on the underlying storage honoring it. Tests establish
executable evidence for the listed contracts, not a proof of all interleavings.

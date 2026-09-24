# Go-Journal

[![Go checks](https://github.com/Laisky/go-journal/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/Laisky/go-journal/actions/workflows/go.yml)
[![Behavior and crash recovery](https://github.com/Laisky/go-journal/actions/workflows/behavior.yml/badge.svg?branch=master)](https://github.com/Laisky/go-journal/actions/workflows/behavior.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**An embedded, disk-backed journal for Go message pipelines.** Store records and
acknowledgements locally, recover pending work after a restart, and reclaim old
segments after ownership has been transferred safely.

Go-Journal was extracted from `go-utils/journal` and is used by
[go-fluentd](https://github.com/Laisky/go-fluentd). It is a library, not a running
service: your application supplies record IDs, delivers messages, and decides
when processing is complete.

> **Durability is explicit.** `WriteData` is not a durable receipt. Promise local
> persistence only after `Sync()` succeeds. Record an acknowledgement only after
> the required downstream work succeeds. Recovery can redeliver identical records;
> design consumers to be idempotent.

[Install](#install) · [Quick start](#quick-start) · [Delivery and replay](#delivery-and-replay) ·
[Configuration](#configuration) · [Operations](#operations) · [Testing](#testing) ·
[Contributing](#contributing-and-support)

## When to use it

Use Go-Journal for local retry buffers, message-forwarding pipelines, and
restartable workers that need a disk-backed record of unfinished work. Records
use MessagePack; data and acknowledged IDs are written to separate rotating
segments, optionally compressed with gzip.

It is **not** a replicated message broker, database transaction manager, or
multi-consumer queue. There is no network delivery adapter, distributed consensus,
exactly-once guarantee, or built-in total-backlog quota. Losing the underlying
volume is outside its recovery guarantee. Your application owns retry policy,
backpressure, downstream deduplication, and multi-destination completion rules.

## Install

The current source requires **Go 1.27.0 or newer**; CI targets Go 1.27.x on Linux.
The process-level fault tests use Linux signals and resource limits. Other
platforms and remote filesystems are not qualified by those tests.

From your application's Go module, install the revision documented here:

```sh
go get github.com/Laisky/go-journal@b0c35437293efa4319db1fc588ce18edcf6cdfd3
```

This commit includes the recovery and rejected-payload fixes described below.
Do not assume an older tag or `@latest` includes the same changes. For a newer
revision, review its changes and rerun your application's acceptance tests. Let
Go resolve the commit to a pseudo-version, and commit your application's `go.mod`
and `go.sum`. See the [Go dependency guide](https://go.dev/doc/modules/managing-dependencies#getting_specific_commit).

To try the example in a separate module:

```sh
mkdir journal-demo
cd journal-demo
go mod init example.com/journal-demo
go get github.com/Laisky/go-journal@b0c35437293efa4319db1fc588ce18edcf6cdfd3
```

## Quick start

Save the following as `main.go` in that module, then run `go run .`. Use
`go run . -gzip` to exercise the same lifecycle with compression.

The example writes two records, marks one complete, closes and reopens the
journal, and preserves the remaining record in the new active segment. It uses a
**disposable temporary directory**, which it deletes on exit. In production, use
a persistent application-owned directory and do not delete it during shutdown.
The acknowledgement simulates completed processing; this example does not send
to or verify a downstream service.

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	journal "github.com/Laisky/go-journal"
)

func main() {
	compressed := flag.Bool("gzip", false, "compress journal segments")
	flag.Parse()
	if err := run(*compressed); err != nil {
		log.Fatal(err)
	}
}

func run(compressed bool) error {
	dir, err := os.MkdirTemp("", "go-journal-example-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir) // Disposable example only, never a production WAL.

	open := func() (*journal.Journal, error) {
		j, err := journal.NewJournal(
			journal.WithBufDirPath(dir),
			journal.WithBufSizeByte(1<<20), // Small segments for this example.
			journal.WithIsCompress(compressed),
			journal.WithIsAggresiveGC(false),
		)
		if err != nil {
			return nil, err
		}
		if err := j.Start(context.Background()); err != nil {
			j.Close()
			return nil, err
		}
		return j, nil
	}

	j, err := open()
	if err != nil {
		return err
	}
	defer j.Close()
	for _, id := range []int64{1, 2} {
		if err := j.WriteData(&journal.Data{
			ID:   id,
			Data: map[string]interface{}{"message": fmt.Sprintf("task-%d", id)},
		}); err != nil {
			return err
		}
	}
	if err := j.Sync(); err != nil { // Both writes precede this shared barrier.
		return err
	}
	// A real application must complete the required downstream work FIRST.
	if err := j.WriteId(1); err != nil {
		return err
	}
	if err := j.Sync(); err != nil { // Persist the acknowledgement as well.
		return err
	}
	j.Close()

	recovered, err := open()
	if err != nil {
		return err
	}
	defer recovered.Close()
	high, err := recovered.LoadMaxId() // Recover IDs before accepting new work.
	if err != nil {
		return err
	}
	fmt.Printf("example: recovered maximum ID = %d\n", high)
	if !recovered.LockLegacy() {
		return fmt.Errorf("replay lease unavailable")
	}
	// This example has one replay owner. Close also cleans up on early failure.
	pending := 0
	for {
		d := new(journal.Data)
		err := recovered.LoadLegacyBuf(d)
		if err == io.EOF { // Replacements are synchronized before old-file cleanup.
			break
		}
		if err != nil {
			return err // Replay errors release the lease; do not advance blindly.
		}
		if d.ID != 2 || d.Data["message"] != "task-2" || pending != 0 {
			return fmt.Errorf("unexpected recovered record: %+v", d)
		}
		// Transfer ownership BEFORE requesting the next record or EOF.
		// Rewriting is not an ACK: the record must remain pending for delivery.
		if err := recovered.WriteData(d); err != nil {
			return err // Close and recover again instead of skipping this record.
		}
		pending++
		fmt.Printf("example: pending record = %d (%s)\n", d.ID, d.Data["message"])
	}
	if high != 2 || pending != 1 {
		return fmt.Errorf("unexpected recovery: maximum=%d pending=%d", high, pending)
	}
	return nil
}
```

Among the library's logs, expect:

```text
example: recovered maximum ID = 2
example: pending record = 2 (task-2)
```

The exact Go block is compiled and executed in both modes by
[`TestReadmeQuickstart`](readme_test.go). These are lifecycle checks, not a claim
that printing a message constitutes durable downstream delivery.

## Delivery and replay

### Durability boundaries

| Operation | Meaning of success |
|---|---|
| `WriteData(record)` | The record was accepted by the writer, or its ID was already acknowledged in the current index. Not a durable receipt by itself. |
| `WriteId(id)` | The completion ID was written and added to the acknowledgement index. It must describe work that really completed; call `Sync()` to persist it. |
| `Flush()` | Serializer/compression buffers were flushed. This is **not** a file/directory synchronization barrier. |
| `Sync()` | Completed writes were flushed and the journal files and directory synchronized. It does not wait for another goroutine's queued work or synchronize your downstream. |
| `Close()` | Idempotent resource cleanup. It has no error return and is **not** a substitute for an error-checked `Sync()`. The object cannot be restarted after closing. |

The application-level sequence is:

```text
assign immutable ID -> WriteData -> Sync -> report locally accepted
                                             |
                                             v
                              deliver to every required destination
                                             |
                                             v
                                        WriteId -> Sync
```

A crash between downstream completion and the persisted ACK can cause redelivery.
Use the same stable ID for retries and make downstream processing idempotent.
Never use one ID for different content, and never acknowledge a multi-destination
record merely because one destination succeeded.

IDs are caller-assigned, nonnegative `int64` values. After `Start`, call
`LoadMaxId()` **before new writes**, then allocate above the recovered maximum
with overflow protection. It scans retained sealed data and ACK segments, not the
active writer, so it is not a live ID allocator or backlog counter. A return value
of zero also covers an empty journal; starting fresh IDs at one is convenient.

### Replay is an ownership transfer

Only one application worker should own a replay pass. Acquire `LockLegacy()` and
call `LoadLegacyBuf` serially. Before advancing past each returned record, either
complete `WriteData(record)` to preserve a replacement, or successfully deliver
it and write its ACK. Completing a send into an in-memory channel is **not** a
completed replacement write.

`LoadLegacyBuf` synchronizes replacement writes and ACKs before reclaiming the
exhausted old snapshot. It releases the lease on EOF or a replay error; acquire
it again before retrying. If your own delivery or rewrite fails after a record
was returned, retain that record and retry its transfer, or close and recover
again. Do not skip to the next record. Release the lease explicitly with
`UnLockLegacy()` when voluntarily stopping a pass you still own.

Replay covers sealed predecessor files; it excludes the active writer. `Rotate`
can make new files eligible for a later pass, but **rotation alone does not drain
pending work or enforce a disk quota**. Pausing across a snapshot refresh may
repeat identical records. Cleanup retry may reach EOF without replaying a record
that was already safely transferred.

See [BEHAVIOR.md](BEHAVIOR.md) for the full public contract and
[RECOVERY.md](RECOVERY.md) for reclamation and incomplete-tail handling.

## Configuration

Pass functional options to `NewJournal`; no configuration file or background
service is required. Prefer an explicit directory rather than relying on the
default. Option names below match the public API, including `WithIsAggresiveGC`.

| Option | Default | Purpose |
|---|---|---|
| `WithBufDirPath(path)` | `/var/go-fluentd` | Select a writable journal directory. An explicit nonempty path is created/checked while applying the option. |
| `WithBufSizeByte(bytes)` | 200 MiB | Active data-file rotation threshold, **not** a record-size limit or total disk cap. |
| `WithRotateDuration(duration)` | 1 minute | Age threshold for rotation. |
| `WithRotateCheckInterval(duration)` | 1 second | Frequency of automatic rotation checks. |
| `WithFlushInterval(duration)` | 5 seconds | Serializer flush interval; not a periodic durability receipt. |
| `WithCommitIDTTL(duration)` | 5 minutes | Retention window for the in-memory ACK index, not a message deletion deadline. |
| `WithIsCompress(enabled)` | `false` | Write gzip segments; historical files are decoded using their own suffix/format. |
| `WithIsAggresiveGC(enabled)` | `true` | Request GC when resetting the legacy loader. Measure its cost for your workload. |
| `WithLogger(logger)` | Package console logger | Supply a non-nil application logger. |
| `WithName(name)` | `journal` | Set the option's name value; it does not create an independent storage namespace. Use separate directories for separate journals. |

Negative sizes/durations are rejected. Zero sizes/durations and empty path/name
values leave the existing option value unchanged; **zero does not disable a
worker**. The quick start disables aggressive GC explicitly; that is an example
choice, not a benchmark-backed universal tuning recommendation.

## Operations

**Ownership and shutdown.** One live journal owns one directory through an OS
lock. Never delete `.journal.lock` to bypass it. Stop new work, wait for the
application's in-flight writes, check `Sync()`, then `Close()`. Canceling the
startup context stops maintenance workers; it does not drain your application
queues or replace explicit synchronization and cleanup.

**Failure handling.** A serialization rejection leaves the live data stream
unchanged, so a corrected record can be retried. A storage error may have partial
or unknown persistence effects; it is not proof of rollback. After a data
append/flush/compression-finalization error, the data encoder fails subsequent
appends and barriers closed. Stop using that instance and reopen through normal
recovery. A failed file `Sync` also has an unknown outcome: do not return a
durable receipt or invent a rollback guarantee.

**Storage and security.** Keep the WAL on persistent local storage with reliable
file locking and synchronization. Restrict directory access and encrypt the
volume where required: MessagePack and gzip are not encryption. Apply disk-space
and backlog limits in the application; preallocation, compression and rotation do
not bound total retained data. Do not edit/delete live segments. For a portable
backup, quiesce the application, successfully synchronize and close the journal,
then copy the complete directory rather than isolated data or ACK files.

**Recovery evidence.** The narrowly supported incomplete newest-tail case retains
the original file through an `.incomplete` hard link before recovering its valid
prefix. Those evidence files are not replayed or automatically deleted; inspect
and archive them. Corrupt ACKs, invalid MessagePack, gzip checksum errors, and
older-segment corruption must not be treated as successful EOF. There is no
checksum-based arbitrary-corruption repair for plain segments.

**Observability.** `GetMetric()["idsSetLen"]` measures the in-memory ACK index,
not pending messages or verified downstream delivery. Track disk usage, oldest
pending work, append/Sync/replay errors, recovery time, duplicate deliveries, and
application acceptance-to-delivery latency separately.

`Sync` depends on the storage stack honoring it. The existing `SIGKILL` tests
leave the OS page cache intact; they do not qualify physical power loss, lost
volumes, all filesystems, or all possible concurrent interleavings.

## Performance

[PERFORMANCE.md](PERFORMANCE.md) contains reproducible before/after workloads,
raw measurements, rejected optimizations, and the cost of safer serialization.
The encoder stages a whole record before appending: large records need temporary
memory proportional to their encoded size. Its 128 KiB idle scratch-retention
threshold is neither a record-size limit nor a peak-memory bound.

Separate buffered append, append plus file synchronization, full `Journal.Sync`,
and end-to-end delivery measurements. They are not interchangeable. Applications
may share a `Sync` across completed writes, as in the example, but must not
acknowledge any member before that barrier succeeds. Automatic timed group
commit is not a configuration option in this library.

For diagnostic serializer/file benchmarks:

```sh
go test -mod=readonly -run '^$' -bench '^BenchmarkUserJournalAppend$' \
  -benchmem -benchtime=1s -count=6 .
```

Measure your payloads, concurrency, compression, storage, throughput and tail
latency before choosing production settings. Hosted or cache-hot benchmark
numbers are not a production disk-capacity estimate.

## Testing

From a repository checkout, use its native module graph. Go race builds require
a supported C toolchain; Python 3 is needed only for the additional verification
scripts.

```sh
go mod download
go mod verify
go build -mod=readonly ./...
go vet -mod=readonly ./...
go test -mod=readonly -count=1 -timeout=180s ./...
go test -mod=readonly -race -count=5 -shuffle=on -timeout=180s ./...
go test -mod=readonly -run '^TestReadme' -count=1 .
```

Public-API tests cover exact payloads and IDs, ACK suppression, codec changes,
rotation, lifecycle, ownership, rejected encodings and cleanup retries. Linux
subprocess tests add actual `SIGKILL`/restart, kernel file/descriptor limits and
independently reconciled delivery ledgers. Test counts and coverage are evidence,
not a proof of exactly-once delivery or universal correctness.

For retained failure-versus-control comparisons and bounded fuzzing:

```sh
python3 .scripts/verify_behavior_regressions.py --artifacts /tmp/journal-red-green
python3 .scripts/verify_pr5_regressions.py --artifacts /tmp/journal-append-safety --benchmark-pairs 6
go test -mod=readonly -run '^$' -fuzz '^FuzzBehaviorFileNames$' -fuzztime=15s -parallel=2 .
```

The comparison scripts need the historical commits named in their source; use a
full Git clone or fetch that history. [BEHAVIOR.md](BEHAVIOR.md) describes the
oracles, evidence directories and validation boundaries. The README tests also
check local links and execute the displayed example without a second copy of
its source.

## Contributing and support

Open an [issue](https://github.com/Laisky/go-journal/issues) with the exact revision,
Go version, OS/filesystem, relevant options, a minimal reproducer, and expected
versus observed behavior. Remove secrets and production payloads from reports.
For a sensitive security finding, arrange a private channel with the maintainer
before sharing details; do not publish raw journals or credentials.

Keep PRs focused. Reproduce defects with application-visible behavior and
independent assertions, retain passing controls, and run the native-module and
race suites. For performance changes, use identical workloads, repeated paired
measurements and unchanged durability semantics; report regressions as well as
improvements. Update the relevant contract and this README when behavior changes.
Do not weaken an oracle or count a compilation failure as a reproduced defect.

Maintained by [Laisky](https://github.com/Laisky), with contributions through
[pull requests](https://github.com/Laisky/go-journal/pulls).

## License

[MIT](LICENSE). Copyright and license terms are preserved in the license file.

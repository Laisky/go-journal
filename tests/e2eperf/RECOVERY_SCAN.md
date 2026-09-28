# Recovery scan hypotheses

Incremental baseline: `b4792253d0260b0cae0c5731c5dad4d755646c56`, which already
contains the accepted staging and all earlier reader/Sync improvements. This
iteration must not credit their effects again. Until the retained isolated
patches have been read and measured, the following integrations are experiments,
not adopted production improvements.

## Separately measured changes

**Buffered ACK maximum:** read/validate the absolute base through the existing
scalar path, then fold complete 8-byte signed deltas already buffered. No extra
read-ahead, global cache, borrowed output or unchecked skipping. Stop after an
invalid word; preserve unread suffix, partial tail, pending I/O and gzip checksum
errors. The scalar ReadFull path continues at each buffer boundary. Bitmap and
ACK-set loading are not changed. Differential tests compare the entire result,
fixed base, error and remaining stream against the old word-at-a-time algorithm.

**Directory preparation:** enumerate sorted names with os.ReadDir, then perform
the existing explicit os.Stat on EVERY entry. Do not also request FileInfo for
each directory entry first. Symlink resolution, dangling/unknown-entry rejection,
ordering, collision avoidance, new-file preparation and durability are retained.
There is no cross-operation metadata cache or claim that concurrent external
mutation has identical observation timing. The journal still owns its directory.

The helper-only local Go 1.23.2 race/differential checks are supplemental, not the
module's native Go 1.27.1 validation. The `Recovery scan experiments` workflow
builds each hypothesis separately against the exact pinned baseline and tests
combined behavior before measuring it.

## Measurements

`BenchmarkPublicACKFrontier` constructs real histories through WriteId, Sync,
Rotate, Close and reopen, scans through public Journal.LoadMaxId, then verifies
cleanup/reopen still preserves an old maximum. Timed work is the warm-cache
recovery scan, not fixture generation, new deliveries or application throughput.
The directory benchmark times exported PrepareNewBufFile, actual file creation,
close and removal, over fixed real directories. All counts and checks are fixed.

`bench_pairs.py` retains all process outcomes and raw output; it rejects missing,
changed-work or failed samples and recomputes descriptive paired intervals.
Identical-binary controls accompany both public benchmark suites. Independent
full E2E lifecycles retain the existing fsynced peer, crash/recovery oracle,
profiles-versus-timings split, before/after qualification and adverse samples.

CPU profiles label public ACK scan work to separate it from fixture generation.
Optional strace metadata counts include setup/cleanup and are diagnostic-only.
Neither diagnostic time nor unqualified microphase labels certify production
latency. Byte counts/allocations, retained heap, scan latency and complete durable
lifecycle throughput remain distinct. Sources and all failures are retained.

Primary contracts: https://pkg.go.dev/bufio#Reader.Peek,
https://pkg.go.dev/io#ReadFull, https://pkg.go.dev/os#ReadDir.

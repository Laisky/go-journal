# Measured journal optimizations

Companion measurement harness and reproducible reports: `Laisky/go-fluentd` PR #11, `tests/performance/` and `docs/performance.md`.

Baseline: `665e193ecd7b07886308614e4e4c4f44d28eef97`. Source iterations were committed separately:

1. `d61f56bc9db5bb85bad9d9b7de8861f31bda8228`: decoder buffers reduced from 4 MiB to 64 KiB, independently of writer/compressor buffers. Allocation profiling attributed about 94% of a 64-record high-water recovery run's allocated bytes to the old reader buffers. Large records remain supported; this is not a record-size limit.
2. `9201e2bfe2cb55de88886bce7747a7919904cf93`: confirmation deadline refresh uses one `sync.Map.Swap` under the existing generation read lock, replacing repeated map operations and the redundant refresh mutex. The two-generation TTL policy and non-consuming successful lookup are unchanged.
3. `f1fae29aa546c528f54e065e2554df39b1f05ca0`: reuse encoder-locked and decoder-local eight-byte ID scratch words. Encoding remains big-endian signed offsets; decoding still distinguishes EOF from partial-ID errors.

Initial repeated local measurements on Go 1.27.1 / Linux overlayfs showed the following allocation changes (timing values and all workloads are in the application report):

| Operation | Before | After |
|---|---:|---:|
| Plain 64-record recovery | about 8.57 MB/pass | about 0.30 MB/pass |
| Gzip 64-record recovery | about 8.74 MB/pass | about 0.47 MB/pass |
| Plain 1,024-record replay, half confirmed | about 11.00 MB/pass | about 2.73 MB/pass |
| Repeated confirmation refresh | 6 allocs/op | 2 allocs/op |
| Unique ID write, including index and flush | 4 allocs/op | 3 allocs/op |

These are not production SSD throughput claims. Small-scan/replay timings improved substantially locally; long-scan and individual ID-write timings varied between samples. The application harness performs counterbalanced baseline/candidate runs on one runner and retains all results, including unchanged/slower workloads. Buffered, per-record Sync and explicit batch-Sync operations are separate workloads, never substituted for one another.

## Correctness boundary

No write flush, file/directory Sync, durable acceptance, recovery validation, corruption handling, confirmation TTL or on-disk format has been weakened. The allocation changes are kept only with public-API controls for plain/gzip records exceeding 4 MiB, 64 KiB boundaries, byte-exact ID output, duplicate/out-of-order/extreme IDs, truncated IDs, concurrent refresh, stable cardinality and non-consuming lookup. Existing concurrent writers, failure injection, interrupted-tail evidence and crash recovery tests remain enabled.

Every source iteration was compiled, vetted, and tested with this repository's actual module graph, including five randomized race-suite runs, before its source commit was pushed. Temporary source-publication workflows are not part of the final tree. The application integration additionally reruns its full-process delivery contracts.

## Measured correction: adaptive plain-data read-ahead

The first hosted paired campaign (application Actions run 35858164130)
exposed a repeatable 14.7% slowdown for the 4,096-record uncompressed
recovery scan: 6.921 ms baseline versus 7.937 ms with fixed 64 KiB data
read-ahead, with non-overlapping observed ranges. That fixed-size policy
is superseded, not presented as a universal improvement.

Uncompressed regular data files at least 4 MiB now retain the original
4 MiB read-ahead. Smaller data files, compressed decoding and ID decoding
retain 64 KiB lookahead. The file stat is a buffer-size hint only; stat
failure does not hide the decoder's existing errors, and file size is
never a limit on accepted records or a substitute for decoding.

An alternating five-pair local comparison reduced the large plain scan
from 9.733 ms to 7.142 ms while the 64-record scan was essentially
unchanged (201 versus 202 microseconds). The final application campaign
reruns every workload and the whole durable pipeline with the corrected
policy. Its raw results include the rejected fixed-size candidate so
the unfavorable evidence is retained.

## Rejected-payload isolation: measured correctness cost

The PR #5 follow-up stages a complete encoded data record before appending it.
This prevents a rejected serialization from corrupting the live stream, while
preserving EncodeMsg-only values and single callback invocation. The change is
primarily a correctness fix, not an assertion of increased pipeline throughput.

Scratch is reused under the encoder mutex. The final idle retention threshold is
128 KiB; this is not a limit on accepted records or peak memory. Larger records
still require scratch proportional to their encoded size and that allocation is
released after the operation. They incur additional allocation and copying.

An initial safe implementation retained only 64 KiB. A 64 KiB payload plus its
MessagePack header exceeds that threshold, causing two allocations on every
append. Six alternating same-runner pairs, 2,000 messages per sample, compared
that safe implementation with the 128 KiB threshold. Every production file was
identical except the threshold; the benchmark source was identical.

| Buffered append, 64 KiB payload | Safe 64 KiB cap | Final 128 KiB cap |
|---|---:|---:|
| Plain median time | 26.846 us/op | 15.821 us/op |
| Plain observed time range | 24.645–54.166 us/op | 15.444–16.666 us/op |
| Gzip median time | 41.860 us/op | 25.121 us/op |
| Gzip observed time range | 28.634–109.158 us/op | 20.489–29.611 us/op |
| Allocations, both codecs | 2/op | 0/op |
| Allocated bytes, both codecs | about 73.8 KB/op | 0–2 amortized B/op |

These are buffered append measurements, not success-after-Sync throughput. They
were made on native Go 1.27.1, Linux amd64, GOMAXPROCS=4, on overlayfs reporting
`fsync=volatile`. They do not establish production storage capacity or physical
power-loss durability. Raw paired values, the exact command and source SHA-256
hashes are in [pr5-buffer-reuse.json](tests/performance/pr5-buffer-reuse.json).
The timing distributions are retained rather than claiming universal speedups.

### Final safe implementation versus merged master

A hosted native-module run on the final production implementation
(`74bef0b1325618e56588d25baae1b1e099ea8ffe`, tree
`afb47866140cce13ff23d6b65d61a1713103d678`) compared the exact same eight
workloads to merged master `2ad43212f66b3c04c64992940b7924288f1729f3`.
Six alternating pairs ran on one ext4 runner. The values below are medians;
[pr5-staging-cost.json](tests/performance/pr5-staging-cost.json) retains all 96
individual workload samples and test/artifact hashes.

| Payload / operation | Master | Safe staging | Interpretation |
|---|---:|---:|---|
| Plain 2 KiB append | 4.629 us | 4.977 us | Extra serialization copy has a cost |
| Plain 2 KiB append + Flush + file Sync | 232.247 us | 226.148 us | Overlapping ranges; no demonstrated durable gain |
| Plain 64 KiB append | 29.378 us | 30.240 us | Small median overhead |
| Plain >4 MiB append | 0.864 ms | 1.365 ms | About 58% slower; about 4.2 MB/op extra allocation |
| Gzip 2 KiB append | 2.097 us | 2.189 us | Buffered-only, not durable completion |
| Gzip 2 KiB append + Flush + file Sync | 241.080 us | 247.313 us | Overlapping ranges; no demonstrated durable gain |
| Gzip 64 KiB append | 18.407 us | 20.320 us | About 10% median overhead |
| Gzip >4 MiB append | 1.710 ms | 2.295 ms | About 34% slower; about 4.2 MB/op extra allocation |

This is the measured safety tradeoff, not a performance-neutrality claim. The
128 KiB reuse correction removes avoidable allocations for ordinary records;
it does not remove the full-record staging cost for large records. Durability
barriers and failure checking remain enabled. Hosted run:
[35933248486](https://github.com/Laisky/go-journal/actions/runs/35933248486).

`BenchmarkUserJournalAppend` also measures 2 KiB and >4 MiB payloads on plain and
gzip encoders. Its `sync=true` 2 KiB samples include encoder Flush followed by
file Sync on every timed operation; `sync=false` samples do not. This is a
serializer/file benchmark, not a whole Journal pipeline or directory-Sync
benchmark. Setup, warm-up, final cleanup and payload generation are excluded.
The read-only append-safety CI runs six alternating master/candidate pairs and
retains all eight workloads, including regressions, in its artifact. The 20
fixed iterations per cost sample are diagnostic; use longer repeated workloads
on the deployment filesystem before capacity or latency decisions.

```sh
python3 .scripts/verify_pr5_regressions.py --artifacts /tmp/pr5-evidence --benchmark-pairs 6
go test -mod=readonly -run '^$' -bench '^BenchmarkUserJournalAppend$' -benchtime=20x -benchmem -count=6 .
```

The cap comparison can be reproduced by building two copies with identical tests
and changing only `maxRetainedRecordBuffer` between `64 << 10` and `128 << 10`.
Run the command in the JSON evidence, alternating the copies for each pair.
Never compare this safe staging policy to a buffered-only or error-ignoring path
and label the result as a durability improvement.

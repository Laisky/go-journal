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

# Writer-buffer experiment: retain the current default

**Decision: do not adopt either smaller live writer buffer.** Keep both 4 MiB
writer buffers, the 4 MiB compressor buffers and the existing reader sizing.
The construction-memory improvement is real, but the broader workload exposed
GC/CPU and latency tradeoffs. The next experiment targets scan-local allocation
reuse instead of reducing the Journal's long-lived buffers.

## Frozen comparison and results

Incremental baseline: `df475e4f61c2028c579f9fe4b091c83823ff3184`.
Experiment source: `3bddf4c49988caca56f6585ef08fa1b16327e69e` plus the retained
`candidate.patch` (4 KiB) or `tiny.patch` (256 B). The patches change only four
live writer constructor arguments. Both sides use the same worker, Go 1.27.1,
module graph, GOMAXPROCS=4, fixed cases and five alternating pairs. No Sync,
flush, ACK, record-staging or compressor behavior was intentionally changed.

| Plain encoder-pair construction | Median B/op | allocations/op |
| --- | ---: | ---: |
| Existing 4 MiB buffers | 8,388,881 | 6 |
| 4 KiB experiment | 8,464 | 6 |
| 256 B experiment | 784 | 6 |

These measure constructor allocation, not fsync latency or sustained throughput.
For the 4 KiB candidate, plain worker open-phase allocation fell from roughly
8.427 MB to 36 KB. However, the 256 KiB-record seed workload changed as follows:

| Five matched trials | Existing buffers | 4 KiB candidate |
| --- | ---: | ---: |
| GC cycles per seed trial | 6, 6, 6, 6, 6 | 22, 21, 21, 23, 24 |
| Median seed CPU | 66.768 ms | 92.204 ms |
| Median seed allocation | 55,765,992 B | 59,989,736 B |

The 4 MiB-record replay workload also increased its median delivery CPU from
185.426 to 211.751 ms, with GC cycles moving from 10 to 17 per trial. A smaller
live heap changing GC pacing is a plausible explanation, not an independently
proved sole cause. The evidence does not justify adopting the constructor
benchmark's best case as a general improvement.

The next 256 B versus 4 KiB experiment had a gzip/large-record append-to-Sync
p99 signal of 2.094 → 2.981 ms; median paired ratio 1.3928, exploratory interval
[1.1469, 1.7300]. This remains visible, but the campaign did not qualify for
latency claims. Neither candidate is the production default.

## Qualification and failure accounting

Before starting the campaign, `qualify.py` fixed two requirements for both the
before and after identical-binary controls: the entire paired 95% interval for
lifecycle time and seed p99 must fit [0.9, 1.1], and each side's max/min sample
span must be at most 1.25. Both controls had p99 intervals outside the bound;
`timing_qualified` is **false**. The report keeps raw times and exploratory labels,
but no certified speedup, equivalence or universal regression follows from them.
The host observer separately retained CPU/IO pressure, disk counters, dirty pages
and cgroup counters. These observations are not proof of exclusive hardware.

The initial run [36353966237](https://github.com/Laisky/go-journal/actions/runs/36353966237)
failed new tests on the **unchanged baseline**, before any timed campaign. Two
new assertions were wrong: gzip visibility requires explicit Flush, and the
pinned MessagePack library's custom error wrapper does not support the assumed
`errors.Is` chain. Corrected tests preserve the old behavior rather than changing
production to satisfy the incorrect assertions. Failed artifact 10943282372 is
retained, ZIP SHA256 `5a4b9239e0e41897554a10eee058b5ec6128c3b0f7243cea44850aef10b9280c`.

A separate local supplemental comparison was interrupted by the execution
limit. Its partial trials were not accepted or substituted for hosted results.
Inspection found nested process groups could survive their owner; they were
terminated, and the framework now tests TERM/KILL parent-death cleanup. The
single-threaded exec guard arms Linux PR_SET_PDEATHSIG, checks the parent-race
window, and gives nested supervisors an opportunity to clean their groups.
This is intended for non-setuid workloads launched by the owning main thread,
not a general process-management service.

## Validation and retained evidence

Run [36354138413](https://github.com/Laisky/go-journal/actions/runs/36354138413)
completed 160 unprofiled lifecycle trials: 70 baseline/4 KiB, 70 4 KiB/256 B,
10 before A/A and 10 after A/A. All 60,320 source deliveries reconciled with zero
observed identical duplicates. Full suite: 456 test/subtest passes; race: 1,368;
ID-scan fuzz: 119,995 executions; Python: 19 methods. Boundary tests cover
high-entropy ASCII bodies, Unicode tails, plain/gzip, writer boundaries through
4 MiB, rejected custom encoders, retries and poisoned partial writes.

[Artifact 10943402519](https://github.com/Laisky/go-journal/actions/runs/36354138413/artifacts/10943402519)
ZIP SHA256: `0935e1d7f8efde055300a9a5a9653968eef8eb2ca89124b8124938d4d802362c`.
After download, all 4,941 manifest entries, 160 audits, assessments and displayed
medians were independently recomputed using the matching trusted driver source.
Keep that source when verifying: later worker/summary extensions have a different
frozen driver hash. Artifacts expire after fourteen days.

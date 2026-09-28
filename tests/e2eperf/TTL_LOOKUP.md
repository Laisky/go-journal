# Defer unnecessary TTL clock reads

Incremental production baseline: `7a754e0786bf1de591f3c7cdee69ea5a8db04385`.
The rejected ACK-only buffer experiment is separate; no memory-layout, allocation
budget, Sync or GC-environment change is part of this hypothesis.

Current-generation ACK membership is intentionally non-consumptive and does
not compare deadlines until a generation rotates. Yet CheckAndRemove acquires
a timestamp before it knows whether that comparison is needed. The candidate
moves the clock read inside the old-generation branch, after a current miss.
It preserves the generation read lock and reads time before old-map lookup.
Current hits and misses with no old generation avoid the clock entirely.
Old-generation expiration, refresh, cardinality, rotation and shutdown remain.

Differential contracts retain current membership even with a historical deadline,
old-generation expiration at the exact virtual-time boundary, shadowing, signed
ID extremes and concurrent expiration. A real expiry-bypass mutant must fail
its intended assertion. Full/race/TTL tests run in native Go 1.27.1.

Two public benchmark suites isolate current/missing/parallel membership and
actual recovery-loader scans over fsynced data/ACK files with exact pending
payload checks. Fixture creation and final cleanup are excluded. Each replay
operation resets and exhausts a snapshot; this is not new-delivery throughput.
Five alternating pairs and identical-binary controls accompany each suite.
Separate CPU profiles attribute clock and lookup work; profiled runs never
enter timing acceptance. Four complete durable lifecycle workloads provide
dense/sparse/gzip/pending guardrails, bracketed by unchanged A/A qualification.

## Initial isolated evidence and adoption boundary

Run 36420540486, source `3d5e0d39954b741403c8a99bca85b1f3eb7988bc`, artifact
10968504841. ZIP SHA256:
`52b0a56b7137ec3986d07f781e973868ad4df6b905b37a203823eb908aabc2fe`.
The complete 154-file source tree matched `4935833351f423eaaa4f0fccd7615d48e066e48a`.
After download, all 2,339 manifest files, 80 lifecycle audits (128,000 source
deliveries), two trace lifecycles, four raw benchmark reports and all paired
assessments/medians/qualification were independently recomputed. Full tests:
570 passes; three shuffled race runs: 1,710; baseline new boundaries: three;
Python: 65 methods. The real expiry-bypass mutant failed its intended assertion.

The code patch has now been copied exactly from that tested isolated candidate.
The workflow requires an empty candidate.patch and repeats the same tests,
workloads and profiles from actual checked-in HEAD. The PR body records that
final exact-head result separately; numbers below belong to the initial study.

| Public operation, fixed batch | Baseline median | Candidate median | Median paired time ratio |
| --- | ---: | ---: | ---: |
| 8,192 current-generation membership hits | 0.593092 ms | 0.183436 ms | 0.31613 |
| 8,192 misses with no old generation | 0.585733 ms | 0.191793 ms | 0.32596 |
| Exhaust real 8,192-record fully ACKed snapshot | 1.868177 ms | 1.518659 ms | 0.80420 |
| Exhaust real 8,192-record sparse-ACK snapshot | 2.130640 ms | 1.621039 ms | 0.75679 |

The two real replay intervals were [0.80128,0.83535] and [0.74191,0.76277]. Their
identical-binary controls were near one: [0.96390,1.01033] and [0.97080,1.01956].
Each row has five alternating pairs. Work differs across suites; compare within
a row only. Replay is a reset/exhaust scan of warm, sealed files with exact
pending-record checks, not newly delivered messages. Allocation and allocation-
count comparisons were inconclusive; no new memory benefit is claimed. Parallel
membership and gzip replay timings were also inconclusive, not universal gains.

The separate fixed-work current-hit profile attributed 0.67 CPU-seconds (55.37%
of its labeled samples) to time.runtimeNow in baseline and no samples there in
the candidate. Labeled current-hit totals were 1.21 versus 0.39 CPU-seconds.
Replay still refreshes ACK deadlines and legitimately reads the clock there;
its profile's time.runtimeNow samples were 0.30 versus 0.14 CPU-seconds. These
sampled profiles explain the mechanism, not precise per-query wall-clock time.

All four full-lifecycle comparisons remained inconclusive. There were no primary
exploratory regression flags, but the sparse lifecycle median worsened from
1.2724 to 1.3620 seconds (inconclusive) and remains visible. Post-campaign A/A
itself flagged dense seed-p99: 1.3596 to 1.8483 ms, paired interval
[1.20146,9.32590]. The unchanged timing qualification failed; neither this
partial speedup nor a clean correctness suite certifies full-lifecycle p99.

Current-generation hits still do not inspect deadlines; expiration is sampled
under the same generation read lock only when checking the old map. Clock
sampling now occurs after the current-generation miss rather than before it.
It may linearize expiry slightly later during concurrent scheduling, but adds
no stale-time cache and does not change Add/rotation/deadline arithmetic. The
virtual-time boundary and concurrency contracts are retained.


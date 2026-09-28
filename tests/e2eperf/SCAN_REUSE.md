# Scan-local reader reuse — incremental performance decision

## Accepted scope

Retain a small and a large plaintext decoder only for the lifetime of **one**
`LoadMaxId` call, then release them. Commit `f977eb4461722101a50587340f66e2f8812ce5eb`
adopts the exact `legacy.go` patch tested in the isolated campaign; Git blob
`8b27387aa4b247764fca96d799460f4cdd58111e` matches that experimental source.
This is an allocation/GC improvement, not a universal latency claim.

The incremental baseline is `df475e4f61c2028c579f9fe4b091c83823ff3184`, which
already includes ID-only scanning and overlapping Sync barriers. The earlier
[writer-buffer experiments](WRITER_RESULTS.md) are rejected: production writer,
compressor and reader sizes remain unchanged. There is no global reader pool,
Journal-owned cache, cached maximum ID, borrowed payload or dependency change.

For each file, reader offsets, pending errors and underlying file/seeker are
reset; MessagePack scratch is cleared on release. Small and large classes retain
the existing 64 KiB/4 MiB constructor policy. A dynamically enlarged reader is
not retained in a cache slot. Gzip decoder state is never reused. All records are
still validated by the same scanner and generated-decoder fallback; malformed
records, newest-tail preservation, ACK semantics and cleanup barriers are unchanged.
Concurrent calls own separate caches. Tests cover truncated-file error state,
missing/negative IDs, alternating size classes, growth, gzip independence and
concurrent public multi-segment scans followed by exact pending-record replay.

## Isolated native evidence

Run [36355483618](https://github.com/Laisky/go-journal/actions/runs/36355483618),
source `8055c1964a3e452a2833a12b5b263ac8d4319ca5` plus retained `candidate.patch`.
The same current external worker was compiled against the fixed baseline and
candidate with Go 1.27.1, the unchanged module graph and GOMAXPROCS=4.
Each case used five alternating pairs and independent lifecycle reconciliation.

| Plain multi-segment case | Scan allocation median, baseline → candidate | Median paired reduction | GC cycles per scan phase, baseline → candidate |
| --- | ---: | ---: | --- |
| 32 small segments, 256 B payloads | 61,808,048 → 28,969,536 B | 53.13% | 7 → 3 |
| 8 large segments, 64 KiB payloads | 555,719,048 → 85,298,056 B | 84.65% | 42–43 → 6 |
| 8 large segments, 256 KiB payloads | 578,813,224 → 108,404,928 B | 81.28% | 42–50 → 8 |
| Concurrent callers, 1 KiB payloads | 34,390,288 → 2,818,640 B | 91.81% | 3 → 1 |

These are **complete scan-phase totals** for the frozen case, not resident heap,
per-message latency or newly delivered messages. The first three cases perform
sixteen scans; the concurrent case performs thirty-two. Percentages are medians
of paired ratios, not ratios of the displayed medians. GC ranges retain every
observed trial; candidate counts above were the same in all five trials.

The corresponding observed CPU medians were 23.168 → 16.979 ms,
113.607 → 56.304 ms, 121.802 → 54.005 ms and 26.342 → 18.066 ms.
A fixed-work allocation profile attributed 96.22% of baseline allocation to
`fwd.NewReaderSize`; its profile-reported allocation fell from 2,080.70 MB to
260.09 MB. That profile includes the initial frontier plus 64 repeated scans of
eight large segments, and is diagnostic-only. It supports fewer repeated buffer
allocations rather than skipped validation or a changed read-ahead size.

Gzip and single-segment allocation/CPU controls were inconclusive, as expected
for paths with no opportunity to reuse plaintext buffers across files. The
concurrent case's process high-water RSS decreased 25.82 → 17.55 MiB, while the
other memory-high-water comparisons were inconclusive. Allocation churn must
not be presented as a uniform retained-memory reduction.

## Timing is not qualified

Both before and after A/A controls failed the predeclared seed-p99 stability
policy. `timing_qualified` is **false**. Whole-lifecycle duration was inconclusive
for every main and guardrail case; no production throughput or latency SLO is
claimed. Scan time observations and exploratory bootstrap labels remain in the
raw reports, but this decision primarily retains the repeatable allocation/GC
reduction and unchanged behavior, not certified wall-clock acceleration.

Two exploratory regression flags remain visible: empty final verification in
the 64 KiB/eight-segment case increased 0.810 → 0.914 ms; gzip/large-record delivery
frontier increased 0.191 → 0.376 ms. They are not erased or proved to be noise.
Intervals are exploratory and not multiplicity-corrected; inconclusive is not
equivalent. The final-head replication is recorded separately in the PR body.

## Expanded workloads and supervision

`--rotate-every N` executes real public Rotate calls after N completed seed
operations, up to 256 requested rotations. Rotation count is audited, and a
separate public behavior test checks actual nonempty segment files and replay.
Its executable no-op-Rotate mutant must fail the physical segment assertion,
even though the reported success counter still advances.

For rotation workloads, the buffer-file size hint is
`2 × rotate_every × (payload + 256)`; the library preallocates half that for each
data segment. This avoids reserving 512 MiB for every tiny segment. Baseline and
candidate use the same formula and 24-hour automatic-rotation check interval.
Single-segment controls retain the old 1 GiB hint. This configuration change is
not counted as a production code optimization. The Python controller separately
bounds synthetic source work to 2 GiB; that is not a total evidence-size guarantee.

`observe.py` retains host/cgroup CPU and IO pressure, disk counters, dirty pages,
quota and process placement. Missing data is explicit, not interpreted as zero
load. `qualify.py` reports A/A validity separately from behavior acceptance.
`summary.json` now includes GC and malloc counts for every worker phase.

The supervisor uses a fresh single-threaded Linux exec guard to arm a parent-death
signal, closes the fork/registration race, and cascades TERM/KILL cleanup through
nested session owners. Real subprocess tests cover both signals and orphaned
startup. This avoids the residual load found in an interrupted local supplemental
experiment; it is not a general daemon or setuid-process supervisor.

## Validation and artifact integrity

The isolated campaign completed **170 unprofiled lifecycle trials**:
60 multi-segment pairs, 70 single-segment/large-record guardrails, and 40 before/after
A/A controls. All 80,080 source deliveries reconciled; observed duplicates were
zero. Full suite: 471 test/subtest passes. Three randomized race repetitions:
1,413 passes. ID-scan fuzz: 111,981 executions. Python: 23 methods. The original
six executable negative controls and the new physical-rotation control passed
by detecting their intended faults. No correctness test failed or skipped.

[Artifact 10944391017](https://github.com/Laisky/go-journal/actions/runs/36355483618/artifacts/10944391017)
ZIP SHA256: `34000caac1b5c83d9181a962f6ee70addb2763d08e9fb2ead198c9edc3844e98`.
All 5,437 manifest entries, 170 audits, assessments and displayed medians were
independently recomputed after download. Whole-verifier local execution limits
were handled by verifying campaigns separately; no measurement was rerun,
replaced or omitted. Retain the matching trusted verifier/driver source and save
verification output outside the immutable evidence directory.

The PR body links final exact-head CI and its separate artifact. Neither campaign
certifies open-loop saturation, long-duration soak, physical power loss or global
optimality. See [README.md](README.md) for reproduction and interactive profiles.

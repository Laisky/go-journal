# Transactional staging: reassign, do not enlarge, the buffer budget

Incremental baseline: `bab91ebc1119a7e6443726fe9c400902ddd6fbbb`. Earlier
scan, Sync and observer changes are retained and not credited to this iteration.

## Hypothesis and preservation boundary

The baseline data encoder copies a complete record to private `bytes.Buffer`
scratch, then into a 4 MiB live MessagePack writer. Scratch over 128 KiB is dropped
after every append, so repeated large records allocate again despite an existing
4 MiB output buffer. Reassign that 4 MiB to private record staging and use a
4 KiB output writer. ACK/compressor/read buffers and every Flush/Sync/ACK operation
are unchanged. This is not the rejected experiment that simply shrank buffers.

The staging arena belongs to the encoder mutex, not a global pool. EncodeMsg is
still called exactly once via msgp.Encode; Encodable-only values and failures
preserve their semantics. No live append occurs before the complete encoding
succeeds. Valid records are immediately appended/flushed as before, and a live
append failure still poisons the encoder. Overflow beyond the 4 MiB arena is
accepted, but its storage is dropped after success, rejection or panic. The
second candidate stages only the excess bytes in a separate bytes.Buffer;
commit writes the fully validated prefix and spill without concatenation. A
second-part failure poisons the encoder just like a failed contiguous append.
The existing encoder mutex spans both writes and Flush; no filesystem-atomic
append guarantee is introduced. No unsafe or borrowed caller data.

Steady data buffering is approximately 4 MiB + 4 KiB, replacing 4 MiB output plus
up to 128 KiB scratch. This is a memory-budget reassignment, not a proof of lower
RSS under all loads. Fresh construction, overflow and single-writer controls
must remain visible alongside steady-state allocation savings.

## Production integration and exact-head acceptance

The segmented implementation is now enabled using the exact serializer diff from
the second isolated experiment. The workflow uses `CANDIDATE_MODE: adopted` and
requires an empty candidate.patch: no hidden source transformation is accepted
for the production candidate. The PR body identifies the final verified commit,
workflow and artifacts after full exact-head replication. Each case has five
alternating pairs. The isolated transform remains only for historical reproduction.

Nine fixed workloads cover 256 B through 4 MiB+envelope, single/concurrent writers,
plain/gzip, rotation and ACK-only cleanup. Existing behavior/race/fuzz and all
eight mutation controls run with the candidate. High-entropy boundary/rejection
tests compare generated encodings. The independent durable peer and restart oracle
are unchanged. Two before/after A/A controls retain the original qualification.

Prepared-payload /dev/null benchmarks isolate serialization allocations and are
not durability benchmarks. Separate fixed-work CPU/allocation/live-heap/trace
runs cover both versions. Retain unfavorable samples; do not use profiler timings
or unqualified intervals to claim production latency/capacity. The PR body names
the exact accepted commit and archived evidence when validation completes.

## First measured candidate and why a second iteration is necessary

The contiguous-stage experiment ran as source `cb2536d723a4e3e6b4cc665b498c6bc63d1b1aba`
plus its retained serializer patch in [36368790174](https://github.com/Laisky/go-journal/actions/runs/36368790174).
All 130 unprofiled lifecycle trials passed: ninety baseline/candidate and forty
before/after A/A trials, 26,000 source deliveries, zero observed duplicates.
Independent verification recomputed all 4,110 manifest files and all paired
assessments. Full native suite: 531 test/subtest passes; race: 1,593; baseline
boundaries: 51. No ordinary test failed or skipped. Timing qualification failed
and every whole-lifecycle comparison was inconclusive. The main campaign had no
exploratory regression labels; unfavorable/inconclusive medians remain in the raw data.

| First experiment seed allocation | Baseline median | Contiguous staging median | Median paired reduction |
| --- | ---: | ---: | ---: |
| Plain 256 KiB, 4 writers | 188,022,456 B | 153,581,016 B | 18.00% |
| Plain 1 MiB, 4 writers | 400,095,696 B | 328,095,344 B | 18.45% |
| Gzip 256 KiB, 8 writers | 187,726,048 B | 153,855,168 B | 18.24% |
| Gzip 1 MiB, 4 writers | 213,476,448 B | 179,620,608 B | 15.86% |

These are complete seed-phase allocations including the external worker's payload
and receipt work, not per-message heap or latency. The gzip 1 MiB case has only
32 source IDs, all in the configured initial-ACK subset; it has no pending replay.
The separate sampled seed allocation profile attributed 59.37 MB (21.02%) of
baseline allocations to bytes.Buffer.grow; that full-record growth disappeared
in the candidate. This is mechanism evidence, not a profiled timing comparison.

The prepared-payload /dev/null benchmark dropped repeated 256 KiB and 1 MiB
serialization from roughly 270,424 / 1,057,003 allocated bytes per call to zero. However,
a record just above 4 MiB still allocated an entire 4.2 MB contiguous copy.
Microbenchmark timings included an isolated large outlier; they are not promoted
to durability or latency claims. Therefore the second experiment uses a fixed
prefix plus overflow-only storage, and directly compares it with the pinned
contiguous prototype as well as the original accepted baseline.

The same nine first-round workloads are retained. Two additional five-pair cases
isolate the overflow correction versus the first prototype. A real bypass-staging
mutant must fail the public rejection-before-live-write assertion, not compilation
or timeout. Tail-write failure tests run against both old and new implementations.
The second candidate was isolated until its evidence was verified; production
integration requires an exact-head replication with an empty candidate patch.

First artifact [10948108727](https://github.com/Laisky/go-journal/actions/runs/36368790174/artifacts/10948108727),
ZIP SHA256 `11c2a2950c0e8a20fff3f4b354a37f364b3ec763cf3ac14357c95f8a33d25775`.


## Second measured iteration: overflow-only storage

Source `bd2df18f6e946a741d9d53162e6732f6b735d23d`, tree
`8a8958e8e74a0c114bf893aca3a5a5e1bb9bf819`, ran in
[36370007599](https://github.com/Laisky/go-journal/actions/runs/36370007599).
The original nine-case matrix stayed fixed. The additional two-case comparison
reconstructed the first contiguous prototype from `cb2536d7` with its own exact
helper/transform, then compared it with the new segmented implementation using
one current worker/controller and five alternating pairs.

| Direct overflow correction, 16 records | Contiguous median | Segmented median | Median paired reduction |
| --- | ---: | ---: | ---: |
| Seed cumulative allocation | 268,884,432 B | 201,656,640 B | 25.00% |
| Replay-transfer cumulative allocation | 340,371,112 B | 273,143,256 B | 19.75% |

Paired allocation-ratio intervals were [0.749926, 0.749982] and
[0.802463, 0.802519]. The 256 KiB direct control remained inconclusive for allocation
and lifecycle timing. Against the original accepted baseline, the eight-record
4 MiB case reduced seed allocations 134,449,392 to 100,829,160 B (25.00%) and
transfer allocations 172,310,296 to 138,693,848 B (19.51%). Plain/gzip 256 KiB–1 MiB
seed allocation savings replicated at roughly 17.5%–19.5%, alongside fewer GC
cycles in the targeted seed workloads. These are operation-phase totals, not
resident-memory or whole-lifecycle latency claims.

Prepared-payload overflow serialization allocated 65–96 B/op rather than an entire
4.2 MB record; ordinary 256 KiB/1 MiB cases recorded zero B/op in all five samples.
Constructor control remained near the same budget: the new data+ACK encoder pair
allocated 8,393,024 B per construction, approximately 4 KiB above the old 8 MiB
pair. Small-record/control gains are not assumed. These microbenchmarks use
/dev/null, exclude payload setup and do not time durable completion.

All 150 unprofiled trials reconciled 26,800 source deliveries with zero observed
duplicates: ninety primary, twenty direct overflow and forty A/A trials. Every
main/direct whole-lifecycle comparison remained inconclusive. The initial
qualification still failed, and no main/direct exploratory regression label was
produced. For example, the direct 256 KiB control had an unfavorable 1.0366 paired
lifecycle ratio and a wide [1.0052, 2.5339] interval. It remains visible rather than
being called equivalent or removed. Qualification thresholds are unchanged.

Native validation passed 533 full-suite test/subtest executions, 1,599 over three
shuffled race repetitions, 53 unchanged-baseline boundary executions, 192,500
ID-scan fuzz executions and 58 Python methods. Nine real faulty-program controls
were detected, including bypassing staging. Ordinary tests had no failure/skip.
All 4,795 manifest files and all 150 audits/paired reports were independently
verified after download; the 124-file source tree matches the published tree.

Artifact [10949052140](https://github.com/Laisky/go-journal/actions/runs/36370007599/artifacts/10949052140),
ZIP SHA256 `19861b24fc53c5a45174a81146da5c88ca67589b2e7d9805fd316ed66a28479c`.

## Retained tradeoffs and stopping boundary

A record exceeding 4 MiB can now require two live writes instead of one; the
encoder mutex spans both and any error poisons the stream. No single-write atomic
or physical-power-loss guarantee is added. Spill allocations above the arena still
exist and are never cached across records. The data writer's memory is reassigned,
not eliminated, and constructor allocation is slightly higher. Worker payload
construction, hashing, persistence and the independent peer remain in E2E costs.

The main acceptance target is repeatable allocation/GC reduction while preserving
correctness. Timing qualification and all unfavorable observations remain separate.
Previously observed empty-replay CPU and high-water RSS concerns are not declared
fixed by this write-path redesign. Representative entropy, offered-rate saturation
and long-duration soak coverage still require separate workloads; no global
optimality or production capacity claim follows from these fixed-work cases.

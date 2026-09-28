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

## Experiment status

The initial CI applies `staging_experiment.py` only in a detached candidate
worktree and retains candidate.patch. The main branch's serializer is unchanged
until matched results are read and accepted. All cases have five alternating
pairs; production adoption requires a subsequent exact-head run with empty patch.

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
The candidate remains isolated until evidence is read; the final production
integration requires an exact-head replication with an empty candidate patch.

First artifact [10948108727](https://github.com/Laisky/go-journal/actions/runs/36368790174/artifacts/10948108727),
ZIP SHA256 `11c2a2950c0e8a20fff3f4b354a37f364b3ec763cf3ac14357c95f8a33d25775`.

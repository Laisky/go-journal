# TTL generation storage: measured candidates

Incremental production baseline: `8092601fd19d816cb0cef229c04571047b6cf9c5`.
Production remains unchanged while these candidates are isolated. Earlier
accepted optimizations and rejected buffer changes are not credited again.

## Current candidate: stable atomic deadline cells, one lookup

Keep sync.Map and the existing outer generation lock. Prepare an initialized
atomic.Int64 deadline cell and use LoadOrStore; a losing insertion refreshes the
winning cell. Only unique insertion increments the existing count. Current
entries are never deleted, and old generations are never refreshed; the outer
lock protects these phase invariants. Cells/generations are not copied or pooled.
The old-generation expiration path, clock calls and deadline arithmetic remain.

This third candidate removes the second prototype's speculative Load. It costs
a small unpublished cell even for refreshes, trading some allocation savings
for one map lookup on first insertion. This is not allocation-free refresh and
must be measured against both cold and populated generations. The read path,
Sync/ACK ordering, memory/GC configuration and file format do not change.

## Previous hypotheses and unfavorable results

| Candidate | Measured benefit | Reason not adopted as default |
| --- | --- | --- |
| 32 locked integer-map shards | All-ACK recovery 2.419 to 1.654 ms; 674,355 to 144,798 B/op | Four-worker pure membership ratio 1.07179 (slower), with stable A/A; rotating seal CPU/elapsed and transfer-frontier RSS flags |
| Atomic cells with Load before LoadOrStore | Serial refresh 1.508 to 0.975 ms; all-ACK recovery 2.405 to 1.842 ms; 673,780 to 150,853 B/op | First-insertion serial batch 1.471 to 1.677 ms, paired ratio 1.13339; dense replay CPU and final empty replay elapsed flags |

Times are fixed-batch medians, not single-record latency. Recovery batches use
8192 real-file records; pure parallel membership uses 32768 total queries;
serial refresh is 8192 additions plus 8192 checks. Percentages/ratios use pairs,
not ratios of the displayed medians. The two structures are not directly
compared across separate runners. Both full-lifecycle qualifications failed;
all six complete lifecycle comparisons in each campaign remained inconclusive.
No adverse trial was deleted, filtered or replaced.

## Reproducible evidence for the first two designs

- Shards: source `4e35790e88e9fd86fd1923ae1c742d99f4438cb1`, run
  [36440424645](https://github.com/Laisky/go-journal/actions/runs/36440424645),
  artifact 10979485159; SHA256
  `86dac85d5a558d993856a7dff96dc79eedb98d70f9d610b42b600f290e7053e2`.
  Verified 2,931 manifest files and 162 source files; six public reports;
  100 audited lifecycles / 195,840 deliveries; two diagnostic lifecycles;
  573 full-suite and 1,719 race test/subtest passes; 67 Python methods.
- Preloaded cells: source `7fa86a26aa1439abbc35b73c8a9d78ed48d55463`, run
  [36442425833](https://github.com/Laisky/go-journal/actions/runs/36442425833),
  artifact 10979133265; SHA256
  `9ee081e8470558884a61d88c10f07b396ce41129d795fb144d652db59bf89a4b`.
  Verified 2,955 manifest files and 163 source files; eight public reports;
  100 audited lifecycles / 195,840 deliveries; two diagnostics; 573 full-suite
  and 1,719 race test/subtest passes; 68 Python methods.

Both archives retain their exact candidate.patch, source, binaries, profiles,
positive/expiry-bypass tests and all assessments. No performance prototype is
substituted for final checked-in-head acceptance. The PR body records the latest
verified head and decision; the third candidate is still unadopted here.

## Test and measurement boundaries

The candidate changes only two private test fixture factories, never assertions
or deadline/rotation/concurrency loops. Public expiry, signed-key, rotation and
concurrent refresh tests execute on both revisions without adapters. Cold
batches include construction, all 8192 first insertions and Close rather than
pre-populating keys outside timing. Read, refresh, hot-key and sealed-file
recovery suites use five alternating pairs and identical-binary controls.
Six durable workloads cover dense/sparse ACKs, gzip, pending-only, large messages
and rotation, with independent fsynced receipts, payload checks and SIGKILL.
Before/after A/A uses the unchanged timing policy. Profiles are diagnostic-only.

Apply `ttl_generation_experiment.py --source <detached-candidate>` to a separate
candidate checkout, keep the same benchmark source on both sides and retain
all evidence. Never patch the baseline or tune GOGC/Sync to claim improvement.
If adopted, pin the old clock-only campaign to its historical revision; do not
pretend the new data structure is another clock-only change. General full/race,
crash, staging and recovery controls remain required. No production capacity,
universal speedup or physical power-loss guarantee follows from these tests.

Primary atomic publication references: https://pkg.go.dev/sync/atomic and
https://pkg.go.dev/sync#Map.LoadOrStore. No unsafe or custom memory reclamation.

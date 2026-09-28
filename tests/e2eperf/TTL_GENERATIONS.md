# Typed TTL generation experiment

Incremental baseline: `8092601fd19d816cb0cef229c04571047b6cf9c5`.
The current production map implementation is unchanged until this hypothesis
passes measured acceptance. All previous accepted changes and rejected buffer
experiments remain intact. The PR body identifies the exact tested source.

The previous real-file recovery CPU profile attributed 23.42% cumulatively to
HashTrieMap.Swap and 12.66% to HashTrieMap.Load. Its cold insertions and repeated
refreshes also box integer keys/deadlines. The candidate replaces each sync.Map
generation with 32 independently locked map[int64]int64 shards. Original
per-operation clock/deadline calculations, generation RLock, rotation, atomic
counts, current-generation shadowing and old-generation expiry remain unchanged.
The shard locks may add read contention and the empty-generation allocation is
larger. These are tradeoffs to measure, not assumed improvements.

The experiment changes two private test fixture factories to instantiate the
candidate representation. Assertions, deadlines and concurrent loops are not
modified. Additional public-API expiry, signed-ID, rotation and concurrent
refresh tests execute without adapters on both revisions. A real expiry-bypass
mutant must fail the intended public assertion, not compilation or timeout.

Three fixed-work benchmark suites cover serial and parallel reads, refreshes,
a deliberately contended single key, and real sealed-file recovery snapshots.
Every suite has five alternating pairs plus identical-binary controls. Six full
lifecycle loads include dense/sparse ACKs, gzip, no ACKs, large messages and
rotation; before/after A/A uses the unchanged timing qualification. Profiles are
separate. A good serial result cannot excuse an unexamined parallel regression.

The previous clock-only workflow remains a historical-baseline guardrail while
this candidate is isolated. If the map design is adopted, its clock-only scope
must be pinned rather than reused for a new attribution. This new workflow
retains those public suites and expiry checks with a new incremental baseline.
General full/race/crash/staging/recovery workflows remain.

Reproduce with the same native Go version and dependencies, apply
`ttl_generation_experiment.py --source <detached-candidate>` only to the candidate,
and retain candidate.patch. Do not patch the baseline, change GOGC, suppress Sync
or mix profiled runs into timing comparisons. Checkpoints, payload hashes and
independent durable-receipt reconciliation remain mandatory. No capacity or
whole-lifecycle latency improvement is claimed before reading the evidence.

## First measured hypothesis: sharded typed maps, not adopted

Source `4e35790e88e9fd86fd1923ae1c742d99f4438cb1`, run
[36440424645](https://github.com/Laisky/go-journal/actions/runs/36440424645),
artifact 10979485159, SHA256
`86dac85d5a558d993856a7dff96dc79eedb98d70f9d610b42b600f290e7053e2`.
All 2,931 manifest files, 162-file source identity, six public reports, 100
unprofiled lifecycles (195,840 deliveries), two diagnostic lifecycles, all
assessments and qualification were independently recomputed. Full tests: 573
passes; race: 1,719 passes; 67 Python methods; expiry mutant detected.

Real-file all-ACK replay improved from 2.419 to 1.654 ms per batch (paired ratio
0.6848), with allocations 674,355 to 144,798 B/op. Sparse replay improved from
2.893 to 2.353 ms. However four-worker pure membership regressed: 1.156 to
1.280 ms per batch, paired ratio 1.07179, while its same-binary control was near
one. Rotating-workload final seal CPU/elapsed and transfer frontier high-water
RSS also had adverse flags. All complete lifecycles remained inconclusive and
timing qualification failed. The first structure is not adopted by default.
Its original helper and complete evidence remain pinned to that source.

## Second isolated hypothesis: stable atomic deadline cells

Keep sync.Map and the original outer generation lock, but store one stable
atomic.Int64 deadline cell per key. Existing-key refresh uses Load followed by
an atomic swap, avoiding new boxed deadlines and trie replacement nodes. Initial
publication uses LoadOrStore; losing inserters refresh the winning cell and only
the unique insertion increments the existing count. Current-generation entries
are never deleted; the old generation is not refreshed. These phase invariants
are protected by the existing generation lock, which remains externally visible.

This avoids the new shard read locks, but adds a cold-insertion lookup and cell
allocation. The new cold-serial and cold-parallel8 batches therefore include
constructor, all 8192 first insertions and Close rather than pre-populating them
outside timing. Existing read/refresh/hot-key/recovery suites and full lifecycle
loads remain unchanged. The two designs are not compared across separate hosts
to claim causality; each is compared against the same declared baseline.

The second design remains isolated until its native results are read. It must
not erase the first design's parallel regression or turn a microbenchmark into
a whole-lifecycle or capacity claim. The candidate follows standard atomic
publication semantics documented in https://pkg.go.dev/sync/atomic and
https://pkg.go.dev/sync#Map.LoadOrStore; no unsafe or custom memory reclamation.

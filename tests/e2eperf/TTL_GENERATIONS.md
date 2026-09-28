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

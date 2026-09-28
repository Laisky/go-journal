# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue this branch; no force-push,
replacement PR, automatic merge/deployment or unrelated branch deletion.

## Preserved implementation

ID-only scanning, overlapping Sync with lazy notification, operation-local
reader reuse, transactional prefix/overflow staging, single-pass directory
metadata, buffered ACK maxima and events-v2 supervision remain intact.
[TTL_LOOKUP.md](TTL_LOOKUP.md) records the accepted clock-read change at
`8092601fd19d816cb0cef229c04571047b6cf9c5`, this iteration's production baseline.

## Current iteration: stable TTL deadline cells

The interrupted experiments through `827d86de` were recovered, not overwritten.
[TTL_GENERATIONS.md](TTL_GENERATIONS.md) preserves all three designs and their
results. Sharded maps regressed concurrent read-only work. Preloaded atomic
cells regressed first insertion. The one-lookup atomic-cell design now replaces
per-refresh map-entry replacement while retaining sync.Map and the generation
lock. Cold/read controls, real-file replay, refreshed deadlines across rotation,
concurrent expiry and cardinality are all covered. No buffer, GC, persistence,
wire-format, clock policy or dependency change accompanies this adoption.

The exact isolated production diff is adopted. Native CI must require an empty
candidate patch, byte-match the helper, preserve test assertions and remeasure
checked-in HEAD. The PR body is the exact-head acceptance record; historical
prototype numbers are not final-head results. A prior observer test's ESRCH
read race is corrected with deterministic error/state tests, not a longer timeout
or a weaker live-process assertion.

## Rejected alternatives and evidence

[ACK_WRITER_BUDGET.md](ACK_WRITER_BUDGET.md), [HEADROOM.md](HEADROOM.md), and
[WRITER_RESULTS.md](WRITER_RESULTS.md) retain rejected default buffer changes.
[RECOVERY_SCAN.md](RECOVERY_SCAN.md), [STAGING.md](STAGING.md), [ACK_REUSE.md](ACK_REUSE.md),
[SCAN_REUSE.md](SCAN_REUSE.md), [SYNC_RESULTS.md](SYNC_RESULTS.md) and
[OBSERVER_RESULTS.md](OBSERVER_RESULTS.md) retain their distinct baselines and
unfavorable observations. Completed clock-only experiments are manual/pinned;
new structure work is measured by ttl-generation.yml instead.

## Acceptance limits

Read-only, cold-insertion, refresh, allocation and whole-lifecycle results must
remain separate. Keep all regression flags and the original A/A p99 policy.
No sample filtering, perf retries, GOGC tuning or weakened Sync/ACK semantics.
Old RSS/empty-replay concerns are not asserted solved by a new local gain.
Representative entropy, offered-rate saturation and long soaks remain distinct
scope. No universal speedup or global-optimality claim. Read exact-head artifacts
and recompute identity/audits/assessments before the final PR status update.

## Resumed follow-up: complete buffered ACK words

The c013e4b8 generation campaign was recovered and its 2,958 manifest entries,
100 lifecycle audits / 195,840 deliveries, eight public reports and source tree
were independently rechecked. Source tree a1d1c5c148917981941988097dd70b9c75225af5.
No unrelated remote branches or earlier reports were removed. The PR description
will record final exact-head acceptance, not infer it from recovered results.

The next isolated candidate targets per-word ReadFull dispatch/copy in ACK set
and bitmap imports. Only complete words already Buffered can use Peek/Discard;
partial input and pending I/O/checksum errors retain ReadFull. Each word is
consumed before invoking user code. Independent per-read state comparisons,
consumer-panic continuation, fuzzing and a real omitted-discard mutant protect
that boundary. Source, fixtures and timing policy stay frozen across paired runs.
No production adoption is claimed before the native experiment is read.

The completed typed-generation campaign is manual-only, pinned to c013e4b8;
its exact source and adverse results remain reproducible. All current generation
behavior tests still run in the active full/race suites.

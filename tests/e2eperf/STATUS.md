# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue the existing branch; no
force-push, replacement PR, automatic merge/deployment or unrelated branch deletion.

## Preserved implementation and rejected experiments

Retain ID-only scans, overlapping Sync with lazy notification, operation-local
data/ACK readers, transactional prefix/overflow staging, parent-death supervision
and events-v2 observation. The current incremental baseline is `b4792253d0260b0cae0c5731c5dad4d755646c56`.
The 1 MiB reader, simple 4 KiB/256 B writer reductions and 4 KiB framing-headroom
experiments remain rejected. Their reports and unfavorable observations remain
in [STAGING.md](STAGING.md), [HEADROOM.md](HEADROOM.md), [WRITER_RESULTS.md](WRITER_RESULTS.md),
[ACK_REUSE.md](ACK_REUSE.md), [SCAN_REUSE.md](SCAN_REUSE.md), [SYNC_RESULTS.md](SYNC_RESULTS.md)
and [OBSERVER_RESULTS.md](OBSERVER_RESULTS.md). Their gains are not counted again.

## Current production iteration

[RECOVERY_SCAN.md](RECOVERY_SCAN.md) separates buffered ACK maximum folding from
single-pass directory metadata enumeration. The initial isolated source `e5744609`
passed native differential/full/race/fuzz and 80 audited lifecycles. Public-API
measurements establish less word-copy/dispatch work and fewer metadata syscalls;
the complete fixed experimental integration is now enabled in checked-in source.
No extra read-ahead, cache, weaker error handling, skipped Stat or persistence change.

The recovery workflow now requires an empty candidate.patch, compares actual
checked-in binaries, repeats the same frozen benchmarks/lifecycle controls, and
checks two real failure mutants. The PR body identifies the latest verified head
and artifacts; do not replace exact-head acceptance with the prototype's result.

## Qualification, regressions and remaining work

Whole-lifecycle timing qualification failed in the isolated campaign. Four main
lifecycle comparisons remain inconclusive; two small-phase regression flags are
retained in RECOVERY_SCAN.md. Public warm-cache scan/preparation gains are not
qualified end-to-end latency, delivery throughput or universal RSS savings.

Preserve every observation, match source and compiler/workload identity, audit
raw results, and keep profiler runs separate. Do not relax A/A criteria or rerun
unfavorable trials to obtain a passing label. Prior empty-replay CPU/RSS concerns
are not asserted solved. Offered-rate/entropy/soak workloads remain distinct scope;
no global-optimality or production-capacity claim follows from these fixtures.

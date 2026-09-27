# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue this branch; no automatic
merge, deployment, force-push or replacement PR.

## Recovered and verified

The interrupted work reached `71790780de0b665e1c33b2583c424fcc770944b2`, not just
`df475e4f`. Its five workflows completed successfully. Artifact
[10944316965](https://github.com/Laisky/go-journal/actions/runs/36356638836/artifacts/10944316965)
was downloaded again: 5,439 manifest files, all 170 lifecycle audits, paired
assessments, medians and timing qualification were recomputed. The 98-file
source tree matches `999d1ebbb35ae95922b9f7a09119de3d8250520c`.

Retain the ID-only scan, overlapping Sync barriers, lazy Sync notification,
operation-local data-reader reuse, and parent-death-safe load supervision.
The 1 MiB data-reader and 4 KiB/256 B writer-buffer experiments remain rejected.
Their reports and sources are not overwritten. See [SCAN_REUSE.md](SCAN_REUSE.md),
[WRITER_RESULTS.md](WRITER_RESULTS.md), [SYNC_RESULTS.md](SYNC_RESULTS.md) and
[RESULTS.md](RESULTS.md) for their distinct baselines and limitations.

## Active experiment, not yet production acceptance

Incremental baseline: `71790780de0b665e1c33b2583c424fcc770944b2`.
The ACK-reader candidate retains one original-size plaintext buffer per ACK
traversal; it resets the per-file absolute base, unread bytes, errors and file
reference. Gzip decoder state is never reused. No global cache or synchronization
change is proposed.

Run [36358799934](https://github.com/Laisky/go-journal/actions/runs/36358799934)
uses source `21c9fcc959d34013336c13f7b14fbb88a3e81735` plus an explicit retained
`candidate.patch` in a detached worktree. `legacy.go` on the PR branch is still
unchanged at this checkpoint. The native correctness/race/crash/mutation stage
has passed; paired measurements and profiling must be read before adoption.

Next executable step: verify the complete artifact, inspect all paired resource
and latency effects including controls, then either adopt the exact tested patch
and validate the checked-in head or retain the failed hypothesis without enabling
it. The follow-up ACK mutation runner also uses existing guarded supervision;
its later commit is not part of the initial experiment's measured revision.

## Unchanged acceptance limits

Keep every failed/unfavorable observation. Use the same worker, dependency graph,
compiler settings and synchronization cadence on both sides. CPU/heap/trace runs
are diagnostic-only. Before/after A/A qualification is independent of correctness;
its prior p99 failure is not relaxed to obtain a favorable result. Allocation/GC
reductions are not universal RSS, whole-lifecycle latency or production-capacity
claims. The final PR body identifies the exact latest verified head and artifact.

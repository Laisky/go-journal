# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue this branch; no automatic
merge, deployment, force-push or replacement PR. The PR body is the exact-head
validation record; the files below preserve distinct measured iterations.

## Recovered interrupted work

The interrupted work reached `71790780de0b665e1c33b2583c424fcc770944b2`, not just
`df475e4f`. Its five workflows completed successfully. Artifact
[10944316965](https://github.com/Laisky/go-journal/actions/runs/36356638836/artifacts/10944316965)
was downloaded again: 5,439 manifest files, all 170 lifecycle audits, paired
assessments, medians and timing qualification were recomputed. The 98-file
source tree matches `999d1ebbb35ae95922b9f7a09119de3d8250520c`.

Retained: ID-only scanning, overlapping Sync barriers, lazy Sync notification,
operation-local data-reader reuse, and parent-death-safe load supervision.
The 1 MiB data-reader and 4 KiB/256 B writer-buffer experiments remain rejected.
Their reports and sources are not overwritten. See [SCAN_REUSE.md](SCAN_REUSE.md),
[WRITER_RESULTS.md](WRITER_RESULTS.md), [SYNC_RESULTS.md](SYNC_RESULTS.md) and
[RESULTS.md](RESULTS.md) for their distinct baselines and limitations.

The remote branch inventory contained the existing PR10 branch and historical
repository branches, not an additional unmerged ACK experiment branch. The
available local runtime had no earlier journal worktree or live load process.
Recovered archives remain immutable; experimental GitHub runner worktrees are
isolated from the PR branch. Unrelated historical branches were not deleted.

## Current implemented iteration

Incremental baseline: `71790780de0b665e1c33b2583c424fcc770944b2`.
ACK-reader reuse is integrated in maximum-ID scanning, replay ACK-snapshot loading
and cleanup planning. One original-size plaintext buffer belongs to each traversal;
release resets its absolute-ID base, scratch, unread bytes, errors and file
reference. Gzip state is never reused. No global cache or synchronization change.

The initial isolated campaign
[36358799934](https://github.com/Laisky/go-journal/actions/runs/36358799934) used
`21c9fcc959d34013336c13f7b14fbb88a3e81735` plus retained `candidate.patch`.
Its 5,282 manifest files and 160 lifecycle audits were verified independently,
including all assessments, medians and timing qualification. Source tree:
`4deb279e66e142f650e14b627c4c786bc2405435`. All 88,320 source deliveries reconciled.
The accepted resource scope and every unfavorable observation are recorded in
[ACK_REUSE.md](ACK_REUSE.md); whole-lifecycle timing is not qualified.

The checked-in `legacy.go` blob `78b91e4bedd91c1f871ab33b07064025abe29a43`
exactly matches that experiment's integration. The ACK mutation runner also
uses the existing parent-death-safe supervision. The adopted-mode workflow
requires an empty candidate patch, tests the actual checked-in implementation,
and repeats the frozen ACK/segment cases and before/after A/A controls.
The PR body names that exact final run, head, artifact and verification result;
an isolated prototype's success is not substituted for exact-head validation.

## Remaining limits and next decisions

Keep every failed/unfavorable observation. Preserve identical workers, dependency
graphs, compiler settings, workloads and synchronization cadence across versions.
CPU/heap/trace runs are diagnostic-only. A/A qualification is independent of
correctness; its p99 requirements are not relaxed to obtain favorable claims.
Allocation/GC reductions are not uniform RSS or whole-lifecycle latency gains.

Remaining profiling candidates include per-file metadata/open/close work and
representative offered-rate/long-duration loads. They are not implemented or
claimed as improvements. Removing file verification or durability barriers is
not an acceptable shortcut. First inspect the exact-head measurements and
qualification before selecting another isolated hypothesis. No global-optimality
or sustainable production-capacity claim is made by these synthetic trials.

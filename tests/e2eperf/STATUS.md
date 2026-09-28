# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue this branch. No automatic
merge/deployment, force-push, replacement PR or deletion of unrelated branches.

## Consolidated implementation

Retain ID-only scanning, overlapping Sync barriers with lazy notification,
operation-local data/ACK readers, parent-death-safe supervision and events-v2
observation. The new incremental baseline is accepted head
`bab91ebc1119a7e6443726fe9c400902ddd6fbbb`; its previous gains are not credited again.
Earlier isolated 1 MiB read-buffer and simple 4 KiB/256 B writer reductions remain
rejected. Their reports and unfavorable observations are preserved.

## Current production iteration

[Transactional record staging](STAGING.md) reassigns the existing 4 MiB data-output
buffer budget to an encoder-private prefix, with temporary overflow-only storage
and a 4 KiB output writer. The entire encoding must succeed before either part
reaches live storage. Partial append failures remain sticky; Sync/ACK ordering,
reader/ACK/compressor buffers, wire format and dependencies remain unchanged.

The contiguous prototype was measured first (`cb2536d7`, 130 trials), then the
segmented correction (`bd2df18f`, 150 trials). Both artifacts were verified.
The exact tested segmented serializer patch is now adopted; the native workflow
requires an empty candidate.patch and repeats the same frozen cases, the pinned
contiguous reference, correctness/mutation suites and before/after A/A controls.
The PR body is the final exact-head CI/artifact record, not a prediction based on
an isolated prototype. See STAGING.md for every baseline and quantitative result.

## Acceptance limits and remaining work

Allocation/GC gains are not uniform RSS or latency gains. Do not relax the A/A
qualification policy, hide adverse samples, replace failed trials or weaken
serialization rejection/Sync to increase throughput. Final verification must
recompute all audits, source identity, reports and qualification from exact-head
evidence. The earlier empty-replay CPU/RSS findings are not claimed solved by
this write-path change. Representative offered-rate/entropy/soak workloads remain
separate scope; no global-optimality or sustainable production-capacity claim.

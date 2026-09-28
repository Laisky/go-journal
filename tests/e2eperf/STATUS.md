# PR #10: implementation complete, regression-protected review candidate

Branch `perf/journal-e2e-20260927`, target `master`. Production implementation is
frozen at `ad3ce410a1c6005b581ead6c5fdf6a24e9c5d324`; this closeout adds CI and
acceptance documentation, not another unmeasured production optimization.
The PR body identifies the exact verified closeout commit and readiness status.

## Retained scope

ID-only recovery, operation-local readers, buffered ACK maxima/imports,
single-pass directory metadata, overlapping Sync with lazy notification,
transactional prefix/overflow staging, conditional TTL clocks and stable atomic
deadline cells remain intact. Events-v2 supervision, independent fsynced receipts,
real crash/replay tests, negative controls and historical adverse samples remain.

## Permanent protection

[Benchmark regression gate](../benchgate/README.md) runs on all PRs and master
pushes, with fixed absolute allocation budgets, pinned and rolling reference
comparisons, counterbalanced CPU controls, strict evidence validation and two
real deliberately regressed binaries that must be detected. CPU uncertainty or
an unstable CPU control is not a pass. Wall-clock durable p99 is not substituted
for process CPU or claimed qualified by the new gate.

The same workflow runs a four-case, five-pair cumulative original-baseline E2E
comparison and independently re-audits each lifecycle. Completed ACK-import,
recovery and staging optimization campaigns are manual-only at their historical
source; ongoing Go/race/crash/replay/append/observer coverage stays automatic.

## Review and follow-up boundaries

Stop adding speculative optimization here. Ready-for-review is not a universal
speed/capacity certificate: prior RSS, empty-replay and oversized-write timing
observations remain in the PR-wide evidence ledger. Deployment-specific p99,
representative entropy, open-loop load, long soaks and physical power loss remain
separate work. Do not weaken Sync/ACK ordering, raise budgets automatically,
filter samples, change GC policy or retry until favorable. No automatic merge,
deployment, force push or unrelated branch deletion.

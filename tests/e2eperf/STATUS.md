# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue this branch. No automatic
merge, deployment, force-push, replacement PR or deletion of unrelated branches.

## Consolidated production state

Retain ID-only scans, overlapping Sync barriers, lazy Sync notification,
operation-local data and ACK reader reuse, and parent-death-safe load supervision.
The accepted ACK integration is `9acb382533d23b918debe9f5a80345de63340e16`.
Its native artifact, resource improvements, timing-qualification failures and
small-phase regressions remain in the PR history and [ACK_REUSE.md](ACK_REUSE.md).
The 1 MiB data-reader and 4 KiB/256 B writer experiments remain rejected.

## Current harness iteration

The interrupted local observer patch is consolidated with remote observer work.
See [OBSERVER_RESULTS.md](OBSERVER_RESULTS.md). This changes measurement and
validation, not Go production code or synchronization behavior. Exact CHECKPOINT
and pidfd observation replace resource-tick-delayed polling; events-v2 evidence
requires successful cleanup and consistent method/backend across every process.
Paired reports reject mixed measurement methods. Real HTTP framing/startup tests
protect the independent durable-receipt peer against transcription regressions.

Local validation passed 51 Python methods, one 20-pair controller comparison and
six real public-API lifecycles using the verified prior worker. Native exact-head
validation is tracked by the PR description and Actions artifacts, not inferred
from those local runs. The new observer workflow rebuilds the worker, repeats
two controller experiments and six lifecycle cases. Existing native full/race/fuzz,
negative controls and ACK/segment comparisons are retained.

## Next acceptance step

Read and verify exact-head artifacts, recompute every audit/assessment and retain
all adverse samples. Update the PR with the exact verified head, not merely the
last locally tested state. Do not conflate controller delay, scan resource savings,
retained heap and whole-lifecycle throughput. Keep the predeclared A/A p99 policy;
failed timing qualification cannot be relaxed to manufacture a speedup.

# Performance acceptance checkpoint

Owner: [PR #10](https://github.com/Laisky/go-journal/pull/10), branch
`perf/journal-e2e-20260927`, target `master`. Continue this branch; no force-push,
replacement PR, automatic merge/deployment or unrelated branch deletion.

## Recovered and preserved production

Interrupted work had reached c013e4b8, four commits beyond the stale 8092601f
PR description. Its exact tree, native artifact and audits were recovered.
Retain stable atomic TTL deadline cells, the clock lookup change, buffered ACK
maxima, single-pass directory metadata, transaction staging, reader reuse,
overlapping Sync with lazy notification and events-v2 process observation.
[TTL_GENERATIONS.md](TTL_GENERATIONS.md) preserves accepted/rejected designs;
[ACK_IMPORT.md](ACK_IMPORT.md) records recovered exact-head results and evidence.
No unrelated branches or earlier reports were deleted.

## Current iteration: buffered ACK imports

Incremental baseline c013e4b87458da53950c7b530726d29f035bfe35. The only existing
Go production change is readOffset in serialize.go: complete buffered words
avoid ReadFull dispatch/copy, while partial reads and pending errors retain it.
Each word is consumed before invoking user code. The adopted implementation
must equal the separately measured candidate transform and have an empty patch.
The isolated public import/bitmap improvements are recorded in ACK_IMPORT.md;
real TTL replay and full-lifecycle gains were not established.

The first general race suites timed out because the new differential test
rechecked immutable residual bytes quadratically. The linear correction keeps
all fixtures and every decoded word/error/cursor/read-count assertion, and
retains complete-tail checks at errors. Existing timeouts are unchanged; the
failed run and completed isolated performance observations are not deleted.
Final exact-head native full/race/fuzz, real cursor mutant and paired evidence
must pass before the PR body marks the new publication verified.

## Evidence and next acceptance step

Read the exact-head artifacts, recheck their manifest/source identity, and
recompute all raw benchmarks, lifecycle audits, assessments and qualification.
Keep small-phase/RSS regressions and A/A failures visible. The PR body is the
latest verification record; prototype figures do not become final-head results.
Completed typed-generation and clock-only workflows are manual/pinned, while
their behavior tests remain in active full/race suites.

## Rejected alternatives and limits

ACK_WRITER_BUDGET.md, HEADROOM.md and WRITER_RESULTS.md retain rejected buffer
changes. RECOVERY_SCAN.md, STAGING.md, ACK_REUSE.md, SCAN_REUSE.md,
SYNC_RESULTS.md and OBSERVER_RESULTS.md retain distinct earlier baselines and
adverse observations. Do not tune GOGC/Sync or relax A/A p99 qualification to
manufacture gains. Allocation, read-only, cold/refresh and lifecycle results
must remain separate; inconclusive is not equivalent. Representative entropy,
open-loop offered-rate and long-soak testing remain separate scope. No global
optimality, universal speedup or sustainable production-capacity claim.

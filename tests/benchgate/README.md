# Permanent benchmark regression gate

> Policy amendment (2026-10-08): [CI testing policy](../../docs/ci-testing.md)
> moves full/race, integration and performance qualification to manual dev/staging
> or workflow dispatch. Historical automatic-gate descriptions below are superseded;
> test assertions and thresholds remain intact. Repository protection settings are unchanged.

Check name: **Benchmark regression gate / benchmark-regression**. This is a
fail-closed regression check, not another report-only experiment. It runs on every
pull request (including drafts), pushes to master, merge groups and manual runs.
No path filters, write token, secrets, automatic PR comments, baseline writes or
`continue-on-error` are used. Configure this check as required in repository branch
protection/rulesets to prohibit merging a failed check; a workflow alone cannot
prevent an administrator or an unprotected branch from bypassing CI.

## What fails

Ten fixed public-API workloads protect serialization, current TTL reads/refresh,
ACK import/maximum scans, directory preparation and multi-segment data/ACK scans.
All calls validate identities/cardinality where applicable; existing full/race,
crash, corruption and append-safety workflows remain authoritative for correctness.

The committed `policy.json` sets per-case absolute B/op and allocations/op budgets.
Any candidate sample exceeding a budget fails. Relative median limits also fail:
reference B/op × 1.05 + 1,024 bytes, and reference allocations/op × 1.05 + 4.
The absolute budgets prevent gradual drift, while comparisons with both a pinned
accepted revision and a descendant PR target/pre-push revision protect incremental
improvements. Changing policy/reference requires an explicit reviewed commit;
the runner never rewrites baselines from a current result.

Four serial CPU workloads also gate CPU cost: TTL hits, TTL refresh, ACK import
and ACK maximum scan. Nine blocks each run reference, the identical-reference
control and candidate, counterbalanced through all six execution orders. Every
sample uses 256 fixed operations. The candidate is compared to the mean of its
two reference samples in that block. A deterministic 99% bootstrap interval of
the median CPU ratio must have its upper endpoint <= 1.20. The same-binary
control interval must lie inside [0.80, 1.20]. A demonstrated >20% regression,
unstable control, or interval crossing the limit **fails** (not a silent pass).
This practical CPU budget does not promise detection of every smaller regression.

CPU cost uses process user+system CPU time; wall time is retained but not gated.
Setup, one warm-up, GC before measurement and final cleanup are excluded. Runtime
allocation counters include all allocations during the loop; they are not retained
heap. Process CPU includes library background goroutines, not just one function.
Prepared serialization writes to /dev/null. ACK/directory/multi-segment workloads
use real warm files; these are not durable-throughput or p99 SLO measurements.

Invalid/missing/extra cases, work counts, non-finite metrics, failed processes,
timeouts, mixed runtimes, different A/A binaries, duplicate JSON keys and changed
raw data fail validation. All failed evidence is retained; samples are not retried,
filtered or replaced. `gate.py --verify` recomputes results from raw JSON and hashes.

## A green detector must actually catch bad code

The workflow builds two deliberately regressed **library** copies from the pinned
reference: one copies each staged record again, and one restores unconditional
TTL clock sampling. The ordinary gate must return exit code 1 and identify the
specific allocation/CPU regression. A compilation error, timeout, malformed report
or generic noisy-control failure is not accepted as evidence that a probe worked.
These binaries never become PR source or production artifacts.

## Scope closure for PR #10

Production source is frozen at the previously verified ad3ce410 implementation.
The completed ACK/recovery/staging experiments become manual-only and are pinned
to that historical head. Their raw unfavorable results and reports are preserved;
the new ongoing gate replaces automatic repetition of historical transformations.
The regular Go, behavior/crash, selective replay, append-safety and observer
workflows continue on current source. This avoids tests that permanently require
current code to equal an old one-line experiment when future changes are made.

Each gate run also compares original PR baseline fc156a60 with the current head
using one identical E2E worker/controller: four representative plain/gzip,
large-record and rotating cases, five alternating pairs, 40 real durable lifecycles.
Every lifecycle is re-audited; reports retain all metrics, including regressions.
That cumulative comparison closes the missing same-harness baseline comparison,
but does **not** override prior failed durable-p99 timing qualification. No new
universal throughput, RSS, production capacity or physical-power-loss guarantee
is asserted. Ready for review means implementation and regression protection are
reviewable, not that these separate production SLO claims have become proven.

## Local reproduction

Build **the same current** `tests/benchgate/*.go` against the current library and
pinned/reference library with one compiler and module environment. The workflow
shows detached-worktree construction. No external Python packages are needed.

```sh
export GOTOOLCHAIN=local GOMAXPROCS=4
go build -mod=readonly -trimpath -o /tmp/bench-candidate ./tests/benchgate
python3 -m unittest discover -s tests/benchgate -p 'test_*.py' -v
python3 tests/benchgate/gate.py --reference /tmp/bench-reference \
  --candidate /tmp/bench-candidate --out /tmp/bench-gate-new
python3 tests/benchgate/gate.py --verify /tmp/bench-gate-new
```

Use new output directories. Exit 0 means these regression budgets passed; 1 means
a measured/control budget failed; 2 means invalid or incomplete evidence. Source
archives, binaries, environment, commands, raw samples, probe patches, cumulative
ledgers and SHA256SUMS are uploaded for 30 days, including after failure. The job
summary links the per-case result; no unauthenticated artifact scripts need run.

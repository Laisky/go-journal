# Event-driven worker observation

## Recovered work and current scope

The previously unpushed observer patch was recovered from the conversation
artifacts and reconciled with accepted PR10 head
`9acb382533d23b918debe9f5a80345de63340e16`. The intervening ACK-reader reuse,
its tests, reports and rejected experiments remain intact. No Go library,
worker, dependency, wire-format or durability change belongs to this update.

This iteration improves the **test controller**, not journal execution. The
existing ACK/segment performance campaign continues to use its declared library
baseline with the same current harness on both sides; its old allocation gains
must not be credited again to the observer. Latest native acceptance, immutable
source and artifact IDs are recorded in the PR body after CI completes.

## Removed measurement overhead

The old controller checked worker completion and reread its growing log every
20 ms. That observation delay was counted in the externally measured lifecycle,
but not in Go's internal phase or per-record latency measurements.

The recovered `monitor.py` uses checkpoint stdout events and Linux pidfd exit
notifications. Resource sampling remains at 20 ms. An unavailable/forbidden
pidfd is explicitly recorded as `pipe-poll`; resource exhaustion is not silently
ignored. A full exact CHECKPOINT line and an existing result file are mandatory
before sending the planned SIGKILL. Spontaneous exit or SIGKILL is not accepted
as a supervised checkpoint. No extra Rotate, Sync or Close repairs durability.

Parsing keeps a marker-sized suffix. Reads and retained logs are bounded; noisy
stdout cannot starve sampling/deadlines. EOF of stdout is not process exit, and
inherited stdout handles do not force an unbounded wait. Existing parent-death
supervision, kill/reap cleanup and separate profiling runs are preserved.

## Strict metric continuity

New options and summaries identify `events-v1` plus the actual backend. Audit
checks every process including the independent scan, validates timestamp order,
exit code, held mode and observed duration, and rejects mixed backends within a
trial. Paired reporting also rejects different methods or backends across trials.
Legacy artifacts without method metadata retain their original summary format.
Do not compare old polling lifecycle values directly with event-based values.

`worker_phase_seconds` is the sum of Go-reported phases for seed, transfer,
deliver and verify. `outside_phase_seconds` is the remainder of their external
process durations. Standalone scan time remains separate. The remainder includes
process/interpreter startup, uninstrumented work, result output, shutdown and
observation: it is **not all removable overhead** or an estimate of fsync cost.
The worker's internal p99 is unchanged. This fix does not explain every previous
p99 fluctuation, and the A/A qualification policy is not relaxed.

## Reproduction and independent acceptance

```sh
python3 -m unittest discover -s tests/e2eperf -p 'test_*.py' -v
python3 -S tests/e2eperf/monitor_bench.py --pairs 20 --out /tmp/new-observer-evidence
```

Use a fresh output directory. The synthetic benchmark compares identical
checkpoint producers in alternating poll/events order and predeclared delays.
It measures from the timestamp just before result/marker publication until
observed exit, including publication and signal handling. It is not a journal,
durability, throughput or production-capacity benchmark. Both raw sides and all
pairs are retained, with no filtering or automatic replacement of slow samples.

The dedicated `E2E observer contracts` workflow repeats the 20-pair experiment
before and after six real native lifecycles: plain/gzip x 0/50/100% ACK, 128 source
records, sixteen writers, periodic rotation and scans. Separate CPU, trace and
contention runs are audited but excluded from performance comparisons. The main
performance workflow retains full/race/fuzz, actual incorrect-program mutation
controls, matched ACK/segment loads and before/after A/A qualification.

Local reconciliation passed 47 Python test methods. Additional regression cases
cover scan-process metadata corruption, mixed backend rejection and ensuring
scan process time cannot change the delivered-lifecycle total. The first local
test command exceeded its 20-second command limit and was retained as incomplete;
a complete run with a sufficient command deadline passed. This is not a
performance-trial retry or a substitute for native CI.

All published source blobs must match the tested tree before acceptance. The
prior patch and historical measurements remain separate evidence; current CI
results are not inferred from a different previously compiled worker.

## Primary API references

- Python pidfds: https://docs.python.org/3/library/os.html#os.pidfd_open
- Selector contract: https://docs.python.org/3/library/selectors.html
- Linux process-exit readiness: https://man7.org/linux/man-pages/man2/pidfd_open.2.html

# Event-driven observation and interrupted-work consolidation

The accepted production baseline remains `9acb382533d23b918debe9f5a80345de63340e16`,
including ACK/data reader reuse and overlapping Sync barriers. This follow-up
changes the external Python measurement harness, not Go production code, file
formats, dependencies, Sync cadence or the previously rejected buffer experiments.
The final PR description records the exact verified head and native artifact.

## Recovered work and fixes

The interrupted observer patch has been reconciled with subsequent remote commits,
not applied on top of an older library version. HTTP framing and peer startup
must remain correct: a response Content-Length is the encoded byte count, and the
server thread runs `self.server.serve_forever`. Real persistent HTTP tests cover
both successful receipts and rejected requests. The Content-Length transcription
regression in `a2580f9` failed both tests before correction.

The event monitor reacts to an exact, complete CHECKPOINT line and uses Linux
pidfd exit readiness when available. The explicitly recorded pipe-poll fallback
is not silently mixed with pidfd measurements. Resource sampling remains every
20 ms. There is no final Rotate, Sync or graceful Close added before a planned
SIGKILL. Output buffering is bounded, errors propagate, and process ownership
retains the existing parent-death guard.

`events-v2` adds explicit successful finalization. The flag is published only
after stdout draining and descriptor cleanup finish. A close error after process
exit must not be reconstructed as successful evidence. Offline audit checks every
worker, including scan: exact return-code/checkpoint types, positive durations,
completion, method/backend/fallback, sampling cadence, timestamps and kill order.
Stripping the method from event-bearing process results cannot downgrade them to
legacy evidence. Mixing methods or backends is rejected by paired reporting.

## Measure the controller, not an invented library speedup

```sh
python3 tests/e2eperf/monitor_bench.py --pairs 20 --out /tmp/observer-fresh
python3 -m unittest discover -s tests/e2eperf -p 'test_*.py' -v
```

The benchmark alternates identical synthetic checkpoint producers between the
old polling control and the event monitor. Its ready-to-observed-exit interval
includes writing the marker and processing SIGKILL; it is not journal durability
latency. The JSON preserves all paired samples, environment/source identity and
exploratory bootstrap intervals. Do not compare historical poll-v1 lifecycle
numbers directly with current event results to claim a Go optimization.

Before native CI, a local 20-pair run observed median notification delay
10.710 ms (poll) versus 0.394 ms (events), median paired ratio 0.0422. These local
controller figures do not certify hosted-runner timing. Fifty-one Python test
methods passed. Six real lifecycles using the verified Go 1.27.1 `9acb3825` worker
reconciled 384 source records; that reuses a verified executable and is not a
fresh native Go build. The dedicated workflow repeats two 20-pair experiments
and six lifecycle cases with a newly built exact-head Go 1.27.1 worker.

`worker_phase_seconds` sums the Go-measured seed/transfer/deliver/verify phases.
`outside_phase_seconds` is the rest of those process lifetimes, including startup,
result serialization, shutdown and observation. Independent repeated scan work
is excluded from both lifecycle components. The outside component is not all
avoidable overhead and must not simply be subtracted to promise throughput.

## Acceptance limits

The existing full/race/fuzz, real negative controls, ACK/segment paired workloads,
before/after A/A qualification and separate pprof/trace evidence remain enabled.
The p99 qualification policy is not relaxed. Controller improvement does not
explain every internal worker latency fluctuation or establish production capacity.
All previous reports keep their original baselines; earlier resource improvements
are not credited again to this follow-up. No automatic merge or deployment.

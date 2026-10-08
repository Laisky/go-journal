# Retained-root performance investigation

> Policy amendment (2026-10-08): [CI testing policy](docs/ci-testing.md)
> moves full/race, integration and performance qualification to manual dev/staging
> or workflow dispatch. Historical automatic-gate descriptions below are superseded;
> test assertions and thresholds remain intact. Repository protection settings are unchanged.

The original rooted implementation at `939a5210b8e9ed8f8b1c1a8781493ac265dac4a4`
failed the unchanged pinned allocation gate: data scans used 201.0625 allocations
per operation (limit 146.8656), and ACK scans used 244.0586 (limit 176.2615).
The failed [exact-head benchmark run](https://github.com/Laisky/go-journal/actions/runs/37703336303)
is retained; no acceptance threshold or workload was changed.

## Diagnosis and selected correction

Identical open/stat/close microbenchmarks on dev with Go 1.27.1 measured:

| Open implementation | Allocations/op | Bytes/op |
| --- | ---: | ---: |
| `os.Open` path control | 4 | 376 |
| direct `Root.Open` | 6 | 400 |
| portable rooted adapter | 6 | 400 |
| Linux direct-child owner | 4 | 320 |

A portable correction first deferred `newestDataName` until an interrupted record
needed it. This eliminated repeated metadata probes on complete scans but alone
left data/ACK allocations at approximately 198/196 per operation, outside the gate.
The selected Linux correction uses a Root-derived directory capability and
single-component `openat(O_NOFOLLOW)`; symlinks and unusual flags use Root.
[Ownership and confinement details](ROOTED.md) describe its invariants.

Holding segment descriptors or caching scan results was excluded because frontier
calls must observe replacement, removal, chmod/read failures and independent seek
positions. Plain/gzip and concurrent regression tests protect those behaviors.
A raw `openat2` prototype was also rejected: its concurrency tests produced invalid
successful opens on this dev host. This observation does not establish a kernel
root cause. The submitted implementation has no raw syscall ABI, unsafe pointer
storage, pooled argument frame, or segment cache.

## Unchanged pinned gate on dev

Nine interleaved rounds use the identical current harness against pinned
`ad3ce410a1c6005b581ead6c5fdf6a24e9c5d324`, with identical-binary CPU controls:

| Workload | Pinned allocations/op | Optimized allocations/op | Pinned bytes/op | Optimized bytes/op |
| --- | ---: | ---: | ---: | ---: |
| Data segments | 136.0508 | 134.0508 | 78,631.7 | 77,079.7 |
| ACK segments | 164.0586 | 132.0508 | 82,648.2 | 76,871.9 |

There are no allocation failures. The single remaining local issue is
`ack-maximum: inconclusive-cpu` (99% ratio interval 0.8515 to 1.2073). The local
gate therefore did not pass; this is retained rather than rerun until green.
Exact-head CI remains the acceptance authority for the unchanged pinned/rolling
gates and cumulative correctness audit.

## Fair current-master durable workload comparison

Both versions use the same frozen current external-consumer harness and identical
go.mod/go.sum. The baseline is current master
`a7f7377c4a3dfe3e76244412c082e1e76e121b44`, which includes PR #12.
The four cumulative cases cover concurrent plain/gzip, large pending records,
and sparse rotating workloads. Five paired trials per case include real Sync,
downstream fsync receipts, ACK/replay, reopen and frontier scans. Each trial is
audited. Five identical-master pairs for plain/gzip run before and after each
comparison, with host CPU/IO/cgroup observation.

The first campaign against the unoptimized rooted head failed timing
qualification. Diagnostic lifecycle records/second were 331.6/332.2 plain,
320.2/325.0 gzip, 26.5/27.8 large-pending and 284.3/264.3 sparse-rotating
(master/rooted). These values are not evidence of equivalence or speedup.
The optimized campaign also failed before/after timing qualification; all 40
comparison trials and all 40 identical-binary control trials passed their audits.
These diagnostic medians show the observed scale of the change:

| Workload | Lifecycle records/s master / optimized | Sync p99 ms master / optimized | Repeated scan ms master / optimized | Scan bytes master / optimized |
| --- | ---: | ---: | ---: | ---: |
| Plain concurrent | 404.0 / 367.9 | 6.167 / 6.773 | 6.019 / 6.450 | 9,556,240 / 9,486,080 |
| Gzip concurrent | 300.0 / 319.4 | 6.183 / 5.396 | 14.378 / 13.172 | 14,417,648 / 14,394,656 |
| Large pending | 26.5 / 26.8 | 22.014 / 15.989 | 67.824 / 62.723 | 101,485,328 / 101,471,944 |
| Sparse rotating | 240.5 / 247.7 | 12.429 / 11.034 | 6.023 / 4.871 | 4,866,192 / 4,728,552 |

Plain concurrent timing is unfavorable in this sample, while other cases have
mixed favorable diagnostics. The failed identical-binary controls prevent using
either result as a causal speedup, regression, or equivalence claim. Allocation
counts establish the fixed-work improvement; these measurements do not establish
a meaningful durable throughput/latency improvement.

Recommendation: retain the lazy recovery decision and confined single-child
Linux open, subject to green exact-head CI and architectural review. Do not
accept extra scan allocations as inevitable, cache segment state, or relax the
confinement or performance gates. A dedicated-host, timing-qualified campaign
would be needed for a durable throughput/latency acceptance claim.

All loads are fixed-work and closed-loop. Timing qualification only applies to
the control workloads; neither benchmark nor lifecycle throughput is a production
capacity or power-loss certificate.

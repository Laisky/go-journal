# Framing slack — measured, not adopted

## Decision

Keep the existing **4 MiB segmented staging arena**. Do not add the proposed
4 KiB framing allowance to production. The isolated experiment proves fewer
write syscalls for its boundary payload, but does not establish a qualified
end-to-end speedup or lower total allocation, and increases constructor memory.
This is an evidence-limited rejection, not a claim that framing slack is always
slower. The experiment is preserved as a pinned, manual-only workflow so it does
not repeatedly consume pull-request CI resources after the decision.

Production baseline: `1db7ba2d05c77781388e2117a321951bac2dbf26`. Experimental
source: `9855736005f75662acc3af3315c48212efaee73f`, tree
`920006c165828ef8b71d5a5178a6aceb9f1a0bcb`, plus its retained candidate.patch.
The library code in that source retains the adopted segmented implementation;
only the detached candidate has additional framing capacity.

## Why this experiment was run

The first adopted staging replication retained allocation savings but flagged a
16-record, 4 MiB replay-transfer time increase versus the contiguous prototype:
139.134 to 168.945 ms, paired ratio 1.21426, exploratory interval
[1.07638, 1.36465]. The original-baseline overflow case also had an unfavorable
full-lifecycle median (564.775 to 583.319 ms, inconclusive). These observations
remain in artifact 10948674183, run 36370917100. They are not erased or called
noise merely because timing qualification failed.

One hypothesis was that a power-of-two payload plus MessagePack framing crosses
the fixed prefix boundary, requiring two live writes. The isolated change added
4 KiB of arena capacity. Tests moved with the boundary so real overflow, short
writes, failed tail writes, private encoding and borrowed-input protection were
still exercised. All public E2E payload sizes remained frozen. The benchmark
boundary moved in the helper test source; no changed-size serialization benchmark
was used to claim an improvement (only fixed constructor benchmarks were run).

## Measurements and retained tradeoffs

Native run [36372041087](https://github.com/Laisky/go-journal/actions/runs/36372041087)
used Go 1.27.1, GOMAXPROCS=4, identical external workers/controllers and five
alternating pairs per case. The two before/after A/A controls used the original
qualification policy. No timed trial was filtered, replaced or retried.

| Observation | Existing arena | Additional framing capacity |
| --- | ---: | ---: |
| 32-record plain 4 MiB lifecycle, median | 1.8580 s | 1.8151 s |
| Same workload replay-transfer, median | 336.284 ms | 318.617 ms |
| Same workload seed allocation, median | 403,275,072 B | 403,285,792 B |
| Same workload transfer allocation, median | 542,004,576 B | 541,995,440 B |
| Data+ACK encoder construction, median | 8,393,026 B | 8,401,219 B |
| Separate trace: seed data-write syscalls, 32 records | 64 | 32 |
| Separate trace: transfer data-write syscalls, 32 records | 64 | 32 |

The first four rows remain inconclusive under the declared 5% exploratory effect
threshold. The lifecycle paired ratio was 0.97872 [0.94399, 0.99710], and replay
ratio 0.93561 [0.84249, 1.07820]. Allocation differences were negligible. The
constructor difference is approximately **8 KiB** of measured allocation, despite
4 KiB of requested extra arena capacity. Construction benchmarks are not durable
completion measurements. Syscall counts were reconstructed from trace samples
containing BOTH syscall.Write and DataEncoder.Write; unrelated ledger/log writes
are excluded. Traced delays do not enter timing acceptance.

`timing_qualified=false`. The gzip boundary case had an exploratory seed-p99
ratio 0.42468 [0.39522, 0.93855], but the same-binary p99 controls failed the
unchanged qualification bounds. This isolated signal is not accepted as a
qualified latency improvement. The 256 KiB control's seed/seal CPU regressed
1.891 to 2.719 CPU-ms, paired ratio 1.34024 [1.07598, 2.09929]. It remains visible
without asserting that arena size alone caused it.

A/A itself flagged verify high-water RSS before the campaign (13.508 to
17.453 MiB), and final-frontier CPU/time after it (0.141 to 0.199 CPU-ms;
0.0585 to 0.0900 ms). Inconclusive is not equivalence, and allocation savings
elsewhere are not a universal RSS reduction. All individual samples and metrics
are retained in the artifact.

## Correctness and independent verification

The candidate passed **534 full-suite test/subtest executions**, **1,602 over
three shuffled race repetitions**, vet, module verification and the intended
bypass-staging mutation failure. Compiler failures or timeouts do not satisfy
that mutation gate. All 80 unprofiled lifecycles passed their independent oracle:
40 focused comparisons and 40 A/A trials, **12,320 source deliveries**, zero
observed duplicates. Both separate trace lifecycles were also audited, but not
counted as timing trials or exactly-once proof.

Artifact [10949756082](https://github.com/Laisky/go-journal/actions/runs/36372041087/artifacts/10949756082),
ZIP SHA256 `09d811d70fd23603c8eff91d19c8924b5b46c0b8ab327a21b20f7cd3a84b2b96`.
After download, all **2,475 manifest files**, source identity, 80 trial audits,
paired assessments and displayed medians were verified independently using the
matching trusted source. Qualification and trace write counts were recomputed.

The primary staging campaign from the same source remains separate evidence:
run [36372041041](https://github.com/Laisky/go-journal/actions/runs/36372041041),
artifact [10949961618](https://github.com/Laisky/go-journal/actions/runs/36372041041/artifacts/10949961618),
SHA256 `3075d07565f119028875754e000acf07dc867b5b0351e7a1557ff07252b2fad3`.
Its 4,795 files and 150 lifecycle audits were independently verified. It retains
allocation gains from the adopted segmented implementation, not framing slack.
See [STAGING.md](STAGING.md) and the PR body for that distinct baseline and the
latest exact-head replication. The earlier overflow timing concern is not
claimed solved by rejecting or measuring this follow-up.

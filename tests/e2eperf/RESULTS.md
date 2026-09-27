# Measured E2E results — 2026-09-27

## Decision and scope

Retain the stateless maximum-ID scan and its measured large-record correction. Reject the smaller large-file reader buffer. No new dependency, wire-format change, reduced synchronization cadence, ACK shortcut, replay-state change, automatic merge or deployment is part of this work.

The immutable pre-optimization baseline is `fc156a60fafc21dd36ad534e5b8f7a971beccec8`. PR #9 has merged; `master` at merge commit `308922dd24758f14c49ab54b907d0a03457f765e` has the same files as that baseline. All comparisons build **the same current external worker source** against both library versions, with Go 1.27.1, unchanged go.mod/go.sum, GOMAXPROCS=4 and one Linux hosted runner per campaign.

The measured candidate in the second campaign is `e951c2aadc2979c8866bcfe856fa06612cc6337f`. Later formatting/documentation edits do not change these observations into measurements of another source revision. The PR description identifies the final-head verification separately.

## Accepted iteration: validate IDs without constructing payloads

The original PR10 scanner avoids constructing a record's payload only when an entire supported canonical envelope is already buffered. Unknown, duplicate or missing fields, extensions, malformed values and cross-buffer records preserve the generated-decoder fallback and error policy. Maximum-ID scanning uses fresh record state; it does not perform ACK lookup or cleanup.

The first campaign exposed a gap: the scanner inspected only a 128 KiB prefix of an existing buffer. Fully buffered 256 KiB records therefore still took the allocating string-decoding path. The separate allocation profile attributed **80.08%** of allocation bytes to `msgp.Reader.ReadString`; the original oversized scan also had a flagged CPU regression.

Commit `1056f23c7c478c86ee0d3df11e1838baf29f5d41` removes that prefix limit **only from stateless readRecordID**. It examines bytes already buffered, adds no read-ahead and keeps all validation/fallback behavior. Replay's 128 KiB bound and independent-successor guard remain unchanged. Forty-five added large-record/buffer/truncation subtests compare against generated decoding, including missing-ID and negative-ID sequences.

### First isolated correction experiment

Run [36340761366](https://github.com/Laisky/go-journal/actions/runs/36340761366), source `4a41e4b26450fc153e2bf8a8a55970ac58df0524`, applied the exact correction only in a temporary worktree before adoption. Five alternating pairs used 128 records of 256 KiB, four workers and sixteen scans per worker.

| Metric | Before correction, median | Corrected, median | Median paired candidate/baseline ratio |
| --- | ---: | ---: | ---: |
| Scan elapsed time | 234.757 ms | 85.636 ms | 0.3483 |
| Scan allocation churn | 2,492,675,208 B | 412,172,584 B | 0.1654 |
| Whole lifecycle | 498.481 ms | 498.937 ms | 1.0009; inconclusive |

The 95% exploratory paired-bootstrap intervals were [0.3159, 0.4105] for scan time and [0.1652, 0.1654] for allocation. The plain control's empty final verification increased by 0.294 ms and was retained for follow-up. This experiment did not establish a whole-lifecycle throughput improvement.

### Second campaign: adopted implementation versus fixed baseline

Run [36341654735](https://github.com/Laisky/go-journal/actions/runs/36341654735), candidate `e951c2aadc2979c8866bcfe856fa06612cc6337f`. Each of the four frozen cases used five alternating pairs, independent re-audits and no profilers in timing trials.

| Frozen case | Scan time, baseline → candidate | Scan allocation bytes, baseline → candidate | Paired time ratio | Paired allocation ratio |
| --- | ---: | ---: | ---: | ---: |
| Plain 16 KiB, sparse ACK | 635.499 → 190.376 ms | 5,211,957,784 → 292,330,424 | 0.3035 | 0.0561 |
| Gzip 4 KiB, sparse ACK | 65.925 → 48.668 ms | 187,236,904 → 14,412,512 | 0.7390 | 0.0770 |
| Plain 256 B, all pending | 9.667 → 3.122 ms | 26,975,600 → 4,635,576 | 0.3165 | 0.1719 |
| Plain 256 KiB | 18.401 → 8.357 ms | 207,330,024 → 77,045,080 | 0.4567 | 0.3716 |

These are **whole scan-phase totals for each frozen case**, not per-message latency or resident memory. Paired ratios are medians of individual candidate/baseline pairs, not ratios of the two displayed medians. The four cases have different amounts of work; compare versions within a row, not absolute times across rows. In particular, this campaign's oversized case has 32 source records, unlike the isolated 128-record experiment.

Scan CPU and process high-water RSS also improved in all four cases. Plain 16 KiB scan CPU medians were 1.976 → 0.639 CPU-seconds and RSS was 110.18 → 67.83 MiB. CPU time can exceed wall time with concurrent workers, and VmHWM is cumulative process high-water memory.

**Whole-lifecycle duration and seed Sync p99 were inconclusive in all four cases and in all twelve stress cases. No general end-to-end throughput improvement is claimed.**

## Rejected iteration: reduce the large-file read buffer from 4 MiB to 1 MiB

After the ID-scan correction, reader-buffer allocation became the largest allocation-profile share, motivating a separate experiment. The second campaign applied a one-line `serialize.go` change only in an isolated worktree. It retained the exact patch, full tests, fuzz results, profiles and thirty matched lifecycle trials. Production still uses the original 4 MiB large-file buffer.

| Experimental case | Scan time, 4 MiB → 1 MiB buffer | Allocation bytes, 4 MiB → 1 MiB | Paired time ratio | Paired allocation ratio |
| --- | ---: | ---: | ---: | ---: |
| Plain 256 KiB records | 81.126 → 97.496 ms | 411,992,768 → 627,618,824 | 1.2165 | 1.5234 |
| Plain 16 KiB control | 33.146 → 21.338 ms | 275,747,736 → 81,350,824 | 0.6450 | 0.2950 |

The oversized time interval was [1.0862, 1.2513]; the allocation interval was [1.5224, 1.5240]. A clear large-record regression outweighs the smaller-record gain. The experiment is **rejected**, not silently omitted from the results. Gzip was a control and did not establish a material improvement.

## Guardrails and remaining bottlenecks

All raw samples and exploratory regression flags remain available. Second-campaign flags outside the intended scan improvement include:

| Case and phase | Baseline → candidate median | Absolute increase |
| --- | ---: | ---: |
| Gzip 4 KiB seed/seal elapsed | 1.252 → 1.623 ms | 0.371 ms |
| Plain 256 KiB replay-transfer elapsed | 4.222 → 4.699 ms | 0.476 ms |
| Same replay-transfer CPU | 3.855 → 4.461 CPU-ms | 0.606 CPU-ms |
| Gzip 1 KiB, 32 writers, all ACKed: delivery frontier CPU | 0.844 → 0.938 CPU-ms | 0.094 CPU-ms |

These are small absolute effects on paths not directly changed by the ID-only correction, but that alone does not prove noise or equivalence. Intervals are exploratory, not multiplicity-corrected. The identical-binary A/A campaign produced no improved/regressed labels. Final-head replication is tracked separately rather than deleting these observations.

The first separate seed execution trace attributed 234.42 ms, or **83.20% of summed traced syscall delay**, to fsync. This is not 83.20% of wall time. Sync was also the main mutex-contention source. Independent downstream ledger fsyncs and serial replay delivery limit the observable lifecycle gain from a faster ID scan; the Python peer's CPU is not included in worker CPU profiles.

The tested low-risk scan hypotheses have converged on the retained implementation, not on a claim of global optimality. Group commit, different replay concurrency or reader reuse would require new correctness proofs and matched experiments; none is claimed as implemented. There is no justification here for weakening durability to manufacture higher throughput.

## Validation and evidence

The second campaign completed **200 unprofiled lifecycle trials**: 40 frozen-case baseline/candidate trials, 120 stress trials, 30 isolated buffer experiments and 10 A/A controls. The stress matrix was 1/8/32 writers × 0/100% initial ACK × plain/gzip, with 512 records of 1 KiB and scanning disabled. Across those trials, 132,160 source deliveries were reconciled; every trial passed and identical downstream duplicates were zero. This is observed behavior, not an exactly-once guarantee.

Native Go 1.27.1 validation passed: 407 test/subtest executions in the full suite; 1,221 in three randomized race repetitions; 108,896 ID-scan fuzz executions; 407 full-suite executions and 113,198 fuzz executions in the isolated buffer experiment; twelve Python test methods; and all three executable negative controls failing their intended assertion. Vet and module verification passed. Four existing native correctness workflows also passed on the same candidate head.

After downloading the second artifact, all **6,153 manifest entries** were SHA256-verified. An independent local pass recomputed the auditor output for all 200 trials, compared each result with both its stored summary and report entry, and recomputed all paired assessments exactly. No timed trial was retried or replaced to improve the result.

| Campaign | Artifact | ZIP SHA256 |
| --- | --- | --- |
| First: 40 baseline/candidate + 30 correction trials | [10938693548](https://github.com/Laisky/go-journal/actions/runs/36340761366/artifacts/10938693548) | `d8d15827766cd02380bc52aa43e38e835eb6a4a3d565c56dd830f0d6123cf866` |
| Second: 200 trials including stress and rejected buffer experiment | [10939326309](https://github.com/Laisky/go-journal/actions/runs/36341654735/artifacts/10939326309) | `0e7aadbda74fd3863a71ecf695e176898d312beacdac13f48d899c45f3246ba2` |

The first upload contained 2,705 verified manifest files but omitted ten hidden synthetic `.journal.lock` files. The second upload fixed this with `include-hidden-files: true`; its manifest is complete. Artifacts include exact sources, executables, module graph, per-record observations, peer ledgers, commands, process results, profiles, patches and test output. They expire after fourteen days; preserve a verified local copy.

These fixed-work, closed-loop, highly compressible synthetic loads are not open-loop saturation tests, representative production traffic, long-term soak tests, physical power-loss tests or sustained-capacity certification. See [README.md](README.md) for reproducible load, profiling, interactive flame-graph and trace commands.

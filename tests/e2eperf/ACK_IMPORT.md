# Buffered ACK imports and interrupted-work recovery

Incremental production baseline: `c013e4b87458da53950c7b530726d29f035bfe35`.
This is distinct from the recovered stable-deadline-cell improvement against
8092601f. The prior implementation, experiments and adverse results remain in
[TTL_GENERATIONS.md](TTL_GENERATIONS.md) and the historical reports.

## Recovered stable deadline cells

The live branch had advanced four commits beyond the stale PR description.
Source c013e4b8 and all eight workflows were recovered. Its 165-file tree is
`a1d1c5c148917981941988097dd70b9c75225af5`. After download, artifact 10984354720
(run 36455105902) was checked: 2,958 manifest entries, eight raw public reports,
100 unprofiled lifecycle audits / 195,840 source deliveries and two diagnostics.
ZIP SHA256: `7fc6b11f55a334c039fc009524ba034067ea4a03afe1a8cbe173fcf38b1d731f`.

This retained sync.Map and the generation lock, but stores stable atomic
int64 deadline cells rather than replacing map values on every refresh. Rejected
sharded/preloaded variants remain documented. The exact-head serial refresh
batch (8,192 updates/checks) used 522,263 -> 129,035 B, and all-ACK real-file replay
used 672,576 -> 280,252 B. All-ACK replay time was 2.357 -> 2.022 ms, paired ratio
0.8527. Cold insertion and pure-read comparisons were inconclusive, not equivalent.
The rotating/small/large durable cases were all inconclusive; timing qualification
failed. A large-case seed/seal CPU regression (1.583 -> 2.542 CPU-ms) is retained.
These recovered benefits are not credited again to the ACK-import follow-up.

## New production change and behavioral contract

Only `IdsDecoder.readOffset` changes. With at least eight bytes already buffered,
Peek reads the signed word and Discard consumes it before returning. Otherwise
ReadFull is unchanged. No byte slice escapes or survives a callback. Read-ahead,
buffer sizes, signed-ID validation, partial tails, pending device/gzip errors,
Sync/ACK order, file format, TTL/clock logic and dependencies remain unchanged.
The earlier buffered maximum folding remains in place; this targets set/bitmap
imports that still read each individual word.

Both Peek and Discard are bounded by Buffered; their no-underlying-I/O contracts
are documented by https://pkg.go.dev/bufio. Differential fixtures compare with
an independent ReadFull reference, not the optimized helper: every output word,
error, read count, cursor and residual tail at errors. A callback that panics
after the second record can resume with exactly the third record. A real
omitted-Discard mutant must fail its specific cursor assertion. Existing signed
bounds, bitmap, gzip corruption/multimember and decoder-reset tests remain.

## Isolated native experiment

Source `c81dbfca2cbf4c935272e51a63c5bbc3508e9198`, run 36462537540, artifact
10988990311. Five alternating pairs, Go 1.27.1, GOMAXPROCS=4, identical harness
and fixtures on both sides. Each public batch opens and exhausts a real sealed
ACK file, including decoder construction and validation; fixture creation is
outside timing. These are warm-file import operations, not new message delivery.

| Public batch | Baseline median | Candidate median | Median paired ratio |
| --- | ---: | ---: | ---: |
| Validate 8,192 plaintext ACKs through a sink | 0.108477 ms | 0.088959 ms | 0.8335 |
| Validate 131,072 plaintext ACKs through a sink | 1.511720 ms | 1.236047 ms | 0.8198 |
| Import/validate 131,072 ACKs into a real bitmap | 3.142241 ms | 2.897309 ms | 0.9232 |
| Decode/validate 8,192 gzip ACKs | 0.310831 ms | 0.287055 ms | 0.9301 |

Allocation comparisons were inconclusive. Real TTL replay (all/sparse/gzip)
did not reach the 5% improvement rule; all six complete lifecycle comparisons
were inconclusive. No generalized recovery or delivery-chain gain is claimed.
Public identical-binary reports had no improved/regressed labels. Lifecycle
qualification failed; after-A/A flagged the same dense binary's lifecycle,
seed and summed-worker-phase durations. All three flags and raw samples remain.

The separate fixed-work CPU profile attributes 1.09 CPU-seconds of baseline
samples to ReadFull/ReadAtLeast versus 0.01 in the candidate. Work moved to
Peek/Discard; total sampled CPU was 1.58 -> 1.33 seconds. Sampling is mechanism
evidence, not a precise wall-clock measurement or performance acceptance.

All 2,907 manifest entries, source identity, four raw public reports, 100
unprofiled lifecycle audits / 195,840 deliveries and two diagnostics were checked.
The isolated full suite passed 579 test/subtest executions and three race
repetitions passed 1,737. The intended cursor mutant was detected. Artifact ZIP
SHA256: `030b7bc9c2a244c474f4ff082ece57dce409c223adc08c1099c6dd7859c05520`.

## Failed test-budget attempt retained

The same c81 source failed the general Go/public-behavior race suites' existing
180-second limit. The Go job 109064424820 stack was in bytes.Equal inside the
new ACK offset test; artifact 10987904389 retains the public-behavior timeout.
Comparing the complete remaining fixture after every eight-byte output made
race-instrumented verification quadratic. It was a test-cost bug, not a race
report and not accepted correctness evidence for the final commit.

The correction retains every fixture and per-word/error/read-count/cursor check,
but compares full residual bytes at errors rather than repeatedly scanning the
same immutable suffix. Subsequent reads remain checked. No timeout, workload,
public benchmark, error assertion or performance threshold was relaxed. The
already-running isolated performance campaign was allowed to finish and all
its results were retained before changing the test; no timed trial was canceled
or replaced for a more favorable sample.

## Reproduction and final acceptance

The ACK-import workflow builds the same tests and external worker against both
revisions. Adopted mode requires empty candidate.patch and exact equality to
the isolated serializer transformation; every other Go file must be identical.
The final PR body records exact-head replication, including regressions and
qualification, rather than substituting the prototype results above.

```sh
python3 tests/e2eperf/bench_pairs.py --baseline /tmp/bench-baseline \
  --candidate /tmp/bench-candidate --suite ack-offset --pairs 5 \
  --iterations 128 --out /tmp/ack-import-fresh
python3 tests/e2eperf/bench_pairs.py --verify /tmp/ack-import-fresh
```

Use a new output directory and the README's matching-source build procedure.
CPU, allocation and trace trials stay separate from acceptance. No uniform
RSS reduction, production capacity, open-loop saturation, power-loss guarantee,
physical-storage speedup or global-optimality claim follows from these loads.

# Isolated ACK writer budget

Incremental baseline: `7a754e0786bf1de591f3c7cdee69ea5a8db04385`.
The default reduction was rejected; production keeps its original ACK buffer.
The completed experiment is manual-only and pinned to its exact original source.

## Hypothesis and distinction from rejected work

Each IdsEncoder.Write writes one signed eight-byte word and immediately flushes
its bufio.Writer. Its constructor nevertheless allocates a 4 MiB buffer. Size
that one buffer to len(enc.word), preserving the existing bufio implementation,
error wrapping, base ID, flush cadence and compressor calls. Both plaintext and
gzip adapters change; the compressor's own 4 MiB buffer remains unchanged.

The earlier rejected writer experiment changed data AND ACK buffers. This one
keeps the accepted 4 MiB private record staging, the data writer, compressor and
all reader sizes byte-identical. Reducing live memory may still alter GC pacing;
the earlier GC/CPU regression is a reason to measure, not to ignore this risk.
No GOGC/GOMEMLIMIT adjustment masks the effect. No durability barrier is removed.

## Acceptance and reproduction

The dedicated workflow first measures an exact two-argument candidate patch in
a detached worktree, with matching current worker/test sources against both
library revisions. Candidate mutation is explicit, recorded and never silently
used as evidence for an untested checked-in implementation. Adoption requires
reading all evidence, including unfavorable CPU/GC/latency and RSS samples, and
then a fresh checked-in-head run with an empty candidate.patch.

Public constructor and 1,024-word /dev/null batches are mechanism checks, not
fsync throughput. Each suite uses five alternating pairs and an identical-binary
control. Nine real lifecycle cases cover absent/sparse/full ACKs, 1/4/8/32 writers,
256 B through 4 MiB payloads, gzip and frequent rotation. Matching before/after
A/A controls retain the existing p99 qualification policy. CPU/heap/trace and
syscall-count runs remain separate from unprofiled timing trials.

```sh
python3 tests/e2eperf/ack_writer_experiment.py --source /path/to/isolated/worktree
python3 tests/e2eperf/bench_pairs.py --baseline /tmp/bench-before \
  --candidate /tmp/bench-after --suite ack-writer --pairs 5 --iterations 64 \
  --out /tmp/ack-budget-public-new
python3 tests/e2eperf/compare.py --baseline /tmp/worker-before \
  --candidate /tmp/worker-after --cases tests/e2eperf/ack_writer_cases.json \
  --pairs 5 --out /tmp/ack-budget-e2e-new
```

Baseline and candidate both run signed-delta wire, immediate scalar visibility,
negative-input, partial/short/error stickiness and concurrent-stream tests. Gzip
visibility continues to require explicit Flush, as before. A real no-ACK-Flush
mutant must fail the intended visibility assertion, not compile failure/timeout.
The usual full/race/fuzz and earlier recovery/staging guardrails remain enabled.

No new performance result is claimed before the immutable artifact is read.
The PR body records the exact final decision, source, evidence and limits.

Primary references: Go bufio implementation https://go.dev/src/bufio/bufio.go;
Go GC tradeoffs https://go.dev/doc/gc-guide. Native CI pins Go 1.27.1 rather than
assuming the unversioned online source exactly matches the measured toolchain.

## Completed experiment: rejected as the default

Run 36418166111, source 7d9eb2c95cbdefe7f01d3c9a4a5469cf62508d5e,
artifact 10968935761. ZIP SHA256:
`357ffc63a52e49094a581f82b91054e65a87f47344cab7a0a11120e340f8bf1e`.
After download all 3,955 manifest files, the complete 147-file source tree,
130 unprofiled lifecycle audits (60,960 source deliveries), four diagnostic
audits, public benchmark raw reports, paired assessments and qualification
were independently recomputed. Full tests: 567 passes; three race repetitions:
1,701; unchanged-baseline boundaries: 23; ACK fuzz: 29,057 executions. No ordinary
test failed/skipped, and the actual missing-Flush mutant failed as intended.

Public plain-ACK construction allocation fell from 4,194,523 to 136 B/op;
data+ACK construction from 8,393,029 to 4,198,736 B/op. These do not certify
throughput. Scalar write timings were inconclusive and both traced scalar
workloads issued 33,800 write syscalls, including calibration/output overhead.
The separate rotating allocation profile removed the 136.05 MB attributed to
NewIdsEncoder's bufio construction; data staging allocation stayed unchanged.

The 64-rotation all-ACK lifecycle seed allocation fell from 546,471,048 to
278,163,192 B (paired ratio 0.50892) and seed process high-water RSS from 51.51
to 34.43 MiB. However, plain 1 MiB replay CPU rose from 51.041 to 60.250 CPU-ms,
paired ratio 1.19671, 95% exploratory interval [1.09512,1.23383]. Seed GC counts
in that case rose from 18-20 to 26-29; transfer GC from 8-9 to 12-13. Plain
256 KiB seed GC also rose from 15-16 to 24-26. Gzip scan elapsed rose 47.816 to
53.648 ms; its delivery-frontier high-water RSS rose 23.53 to 27.52 MiB.

All nine complete lifecycle comparisons and all scalar write comparisons were
inconclusive. Timing qualification failed the unchanged p99 criteria. The
report retains 19 primary exploratory regression labels (including sub-ms
frontier costs), with no A/A regression labels. Lower live heap is consistent
with changed GC pacing, not proof that it explains every timing difference.

Decision: do not trade away repeated large-record CPU/GC behavior merely to
advertise constructor savings. Do not adjust GOGC/GOMEMLIMIT, hide observations
or rerun the same timed trial to manufacture acceptance. Exact sources, failure
controls and raw positives/negatives stay in the immutable artifact; the next
TTL clock experiment has its own baseline and independent hypothesis.

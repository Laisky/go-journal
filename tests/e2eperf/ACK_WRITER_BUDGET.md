# Isolated ACK writer budget

Incremental baseline: `7a754e0786bf1de591f3c7cdee69ea5a8db04385`.
This experiment leaves all production code unchanged until acceptance.

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

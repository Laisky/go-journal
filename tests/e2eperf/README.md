# Observable public-API journal E2E performance

An ordinary consumer of exported Journal APIs, not an internal microbenchmark. It uses real private data/ACK files, an independent loopback downstream, and repeated process restart. PR #9 has merged; PR #10 targets `master`. The immutable pre-optimization measurement baseline is `fc156a60fafc21dd36ad534e5b8f7a971beccec8`.

See [measured results, accepted/rejected changes, and evidence hashes](RESULTS.md). Do not merge or deploy automatically.

## What one trial proves

The lifecycle is **seed → SIGKILL → scan → transfer → SIGKILL → deliver → verify**. Scanning can be disabled with `--scans 0`.

Concurrent writers append generated IDs/payloads and call `Sync`. For the selected ACK subset, the independent Python HTTP peer validates the complete payload and fsyncs its own ledger **before** returning a matching ID/hash/durable receipt. Only then does the Go worker call `WriteId` and `Sync`.

The controller kills seed and transfer workers only after their synchronized checkpoint. A new process recovers every pending record, copies and synchronizes it **before** requesting the next record or EOF cleanup, and is killed again. Final delivery reconciles the independently generated expected IDs and hashes with the peer ledger. The last open must retain the maximum ID and contain no pending record.

Queue admission, `Flush`, `Close`, and arbitrary HTTP success are not durable completion. Identical downstream retries are counted, not silently deduplicated or called exactly-once. `negative.py` builds three real executable mutants (omitted append, ACK, and transfer); each must fail its intended independent assertion, not merely fail compilation.

This is a synchronized-checkpoint process-crash test, **not** random-point crash or physical power-loss certification. Keep the existing crash, corruption, gzip-checksum and sequence-state tests.

## Build and run

Requirements: Linux with `/proc`, Python 3.10+, the Go version in `go.mod` (CI pins Go 1.27.1), and enough private storage. Run from the repository root. Use a new output directory for every trial; failures remain on disk.

```sh
export GOTOOLCHAIN=local GOMAXPROCS=4
E=$(mktemp -d /tmp/journal-e2e.XXXXXXXX)
go mod download
go mod verify
go build -mod=readonly -trimpath -o "$E/worker" ./tests/e2eperf

python3 tests/e2eperf/run.py --binary "$E/worker" \
  --out "$E/smoke" --count 512 --payload 1024 --writers 8 \
  --ack-percent 50 --scans 4
python3 tests/e2eperf/run.py --audit-only "$E/smoke"
python3 -m unittest discover -s tests/e2eperf -p 'test_*.py' -v
python3 tests/e2eperf/negative.py --out "$E/negative"
```

`--count` is the number of source records, not repeated scan operations. `--payload` excludes the identity/Unicode prefix. A scan trial performs `writers × scans` full maximum-ID scans; do not report these as newly delivered messages.

## Build a fair baseline

Use the **same current worker source**, compiler, dependency graph, flags and workload against both library revisions. Building different historical harnesses is not a valid comparison.

```sh
BASE=fc156a60fafc21dd36ad534e5b8f7a971beccec8
git worktree add --detach "$E/baseline-src" "$BASE"
cmp go.mod "$E/baseline-src/go.mod"
cmp go.sum "$E/baseline-src/go.sum"
mkdir -p "$E/baseline-src/tests/e2eperf"
cp tests/e2eperf/*.go "$E/baseline-src/tests/e2eperf/"
(cd "$E/baseline-src" && go build -mod=readonly -trimpath \
  -o "$E/worker-baseline" ./tests/e2eperf)
go version -m "$E/worker" > "$E/build-candidate.txt"
go version -m "$E/worker-baseline" > "$E/build-baseline.txt"
git rev-parse HEAD HEAD^{tree} > "$E/source.txt"

python3 tests/e2eperf/compare.py \
  --baseline "$E/worker-baseline" --candidate "$E/worker" \
  --cases tests/e2eperf/cases.json --pairs 5 --out "$E/paired"
python3 tests/e2eperf/report.py "$E/paired/report.json"
```

The comparison alternates AB/BA ordering, retains every observation and failure, and independently re-audits each lifecycle before aggregation. Only accepted synthetic WAL files are removed to bound storage; observations, process results and the independent ledger remain. A controller timeout terminates its entire process group, including an active worker, and records failure rather than silently retrying it.

## Load matrix

`stress.py` freezes a bounded cross-product of record size, concurrent writers, ACK ratio and codec. Its default matrix has six cases. Preview large campaigns before execution: no more than 36 cases or 10 pairs per campaign, with a 2 GiB synthetic data-work bound per trial. This is not a total artifact-size or memory guarantee.

```sh
python3 tests/e2eperf/stress.py \
  --baseline "$E/worker-baseline" --candidate "$E/worker" \
  --out "$E/matrix" --count 4096 --payloads 256,16384 \
  --writers 1,4,16 --ack-percents 0,50,100 \
  --codecs plain,gzip --scans 4 --pairs 5 --generate-only

# Execute 12 cases, including all-pending and zero-pending recovery.
python3 tests/e2eperf/stress.py \
  --baseline "$E/worker-baseline" --candidate "$E/worker" \
  --out "$E/concurrency" --count 512 --payloads 1024 \
  --writers 1,8,32 --ack-percents 0,100 \
  --codecs plain,gzip --scans 0 --pairs 5
```

These are **fixed-work, closed-loop** loads: each writer waits for synchronization and its selected delivery/ACK before admitting another record. They identify contention and resource changes, not an open-loop offered-rate SLO or sustained-capacity certificate. The payload is deterministic and highly compressible; gzip results do not represent high-entropy production traffic. Use representative storage, payload distributions and long-running offered-rate workloads before sizing production.

## Capture profiles separately from timings

`--diagnostics cpu`, `trace`, or `contention` profiles each worker stage across open, frontier discovery, append/Sync, replay and seal. CPU and execution tracing use separate runs. Contention sampling is intentionally expensive. Diagnostic runs are marked `diagnostic_only`; both comparison and reporting reject them for performance acceptance.

```sh
for kind in cpu trace contention; do
  python3 tests/e2eperf/run.py --binary "$E/worker" \
    --out "$E/diagnostic-$kind" --count 2048 --payload 16384 \
    --writers 8 --ack-percent 50 --scans 4 --diagnostics "$kind"
done
```

Each profiled worker process writes before/after allocation profiles, a post-GC live-heap profile, and configuration metadata. CPU mode adds `cpu.pprof`; trace mode adds `trace.out`; contention mode adds `block.pprof` and `mutex.pprof`. Profiles are finalized **before** publishing the checkpoint, so intentional SIGKILL does not truncate them. Existing evidence files are never overwritten.

Trace regions identify `WriteData`, `Sync/data`, `downstream/fsync-receipt`, `WriteId`, `Sync/ack`, `LoadLegacyBuf`, and replay equivalents. Phase labels distinguish top-level operations within a worker profile. Background goroutines may retain their creation-phase label; that label is not exclusive ownership of asynchronous work.

### Interactive CPU flame graphs and heap views

These commands start local-only interactive pprof views. Select Flame Graph or Graph in the browser. Keep the exact measured executable for symbolization.

```sh
go tool pprof -http=127.0.0.1:8081 -no_browser \
  "$E/worker" "$E/diagnostic-cpu/seed/cpu.pprof"

go tool pprof -http=127.0.0.1:8082 -no_browser -sample_index=alloc_space \
  -base="$E/diagnostic-cpu/scan/alloc-before.pprof" \
  "$E/worker" "$E/diagnostic-cpu/scan/alloc.pprof"

go tool pprof -http=127.0.0.1:8083 -no_browser -sample_index=inuse_space \
  "$E/worker" "$E/diagnostic-cpu/seed/heap.pprof"
```

Allocation churn and retained heap are different metrics. Subtract the before profile when analyzing allocations. The live heap is captured before closing the journal and excludes later result-file serialization. For short phases, increase work or use a separate `--profile-seconds 5` scan diagnostic; never mix its variable operation count into fixed-work comparisons.

### Scheduler, syscall and lock bottlenecks

```sh
go tool trace -http=127.0.0.1:8084 "$E/diagnostic-trace/seed/trace.out"
go tool trace -pprof=sync "$E/diagnostic-trace/seed/trace.out" > "$E/sync.pprof"
go tool trace -pprof=syscall "$E/diagnostic-trace/seed/trace.out" > "$E/syscall.pprof"
go tool pprof -top "$E/worker" "$E/sync.pprof"
go tool pprof -top "$E/worker" "$E/diagnostic-contention/seed/mutex.pprof"
```

Summed blocked-goroutine time can exceed elapsed time; it is not a percentage of wall-clock latency. The independent Python peer performs real fsyncs and can dominate delivery throughput; worker profiles do not measure the peer's CPU.

## Evidence and acceptance

`result.json` retains monotonic intervals, exact operation counts, per-record timestamps and process observations. `summary.json` reports p50/p95/p99 operation latency, CPU, allocations, RSS and lifecycle throughput. `environment.json` records affinity/quota, GOMAXPROCS, filesystem mounts, CPU information and executable SHA256. RSS is process VmHWM: a cumulative high-water mark, not independently reset per phase.

Paired reports retain individual samples and candidate/baseline ratios. Deterministic 95% paired-bootstrap intervals are **exploratory**, not multiplicity-corrected proofs. Fewer than five pairs, zero denominators, incomplete work, failures and diagnostic contamination cannot be called measured improvements. The `improved`/`regressed` labels use a 5% relative threshold; inspect absolute effects and whole-lifecycle guardrails. Inconclusive does not mean equivalent.

Accept changes only after behavior/race/fuzz/negative controls and matched measurements. Keep regressions visible; investigate small noisy effects instead of deleting them. Do not change synchronization cadence, ACK order, generated-decoder error policy, replay sequence state, dependencies or wire format to obtain a benchmark win.

CI publishes source, executables, module graph, every observation, profiles, tests and SHA256SUMS. Download artifacts before their 14-day retention expires. The workflow includes hidden synthetic evidence files so the complete manifest can be checked. A passing job means the campaign completed correctly, not that every metric improved or production capacity was certified. [RESULTS.md](RESULTS.md) records the measured scope, decisions and remaining limitations.

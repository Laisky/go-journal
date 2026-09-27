# Observable public-API journal E2E performance

This executable is an ordinary consumer of exported Journal APIs, not an internal microbenchmark. It uses real private data/ACK files, an independent loopback downstream, and repeated process restart. PR #9 has merged; PR #10 targets `master`. The immutable pre-optimization measurement baseline is `fc156a60fafc21dd36ad534e5b8f7a971beccec8`. Do not merge or deploy automatically.

## What one trial proves

The lifecycle is **seed → SIGKILL → scan → transfer → SIGKILL → deliver → verify**. Scanning can be disabled with `--scans 0`.

Concurrent writers append generated IDs/payloads and call `Sync`. For the selected ACK subset, the independent Python HTTP peer validates the complete payload and fsyncs its own ledger **before** returning a matching ID/hash/durable receipt. Only then does the Go worker call `WriteId` and `Sync`.

The controller kills seed and transfer workers only after their synchronized checkpoint. A new process recovers every pending record, copies and synchronizes it **before** requesting the next record or EOF cleanup, and is killed again. Final delivery reconciles the independently generated expected IDs and hashes with the peer ledger. The last open must retain the maximum ID and contain no pending record.

Queue admission, `Flush`, `Close`, and an arbitrary HTTP success are not durable completion. Identical downstream retries are counted, not silently deduplicated or called exactly-once. `negative.py` builds three real executable mutants (omitted append, ACK, and transfer); each must fail its intended independent assertion, not merely fail compilation.

This is a synchronized-checkpoint process-crash test. It is **not** random-point crash or physical power-loss certification. Keep the repository's existing crash, corruption, gzip-checksum and sequence-state tests.

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

`--count` is the number of source records, not the number of repeated scan operations. `--payload` excludes the identity/Unicode prefix. A scan trial performs `writers × scans` full maximum-ID scans; do not report that as newly delivered messages.

## Build a fair baseline

Use the **same current worker source**, compiler, dependency graph, flags and workload against both library revisions. Building each revision's different historical harness is not a valid comparison.

```sh
BASE=fc156a60fafc21dd36ad534e5b8f7a971beccec8
ROOT=$(pwd)
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

`stress.py` freezes a bounded cross-product of record size, concurrent writers, ACK ratio and codec. The default matrix has six cases. Preview large campaigns before executing them; no more than 36 cases or 10 pairs per campaign are allowed, and each trial has a 2 GiB synthetic data-work bound. This is not a total artifact-size guarantee.

```sh
python3 tests/e2eperf/stress.py \
  --baseline "$E/worker-baseline" --candidate "$E/worker" \
  --out "$E/matrix" --count 4096 --payloads 256,16384 \
  --writers 1,4,16 --ack-percents 0,50,100 \
  --codecs plain,gzip --scans 4 --pairs 5 --generate-only

# Execute a smaller 12-case concurrency/ACK matrix, including zero pending work.
python3 tests/e2eperf/stress.py \
  --baseline "$E/worker-baseline" --candidate "$E/worker" \
  --out "$E/concurrency" --count 512 --payloads 1024 \
  --writers 1,8,32 --ack-percents 0,100 \
  --codecs plain,gzip --scans 0 --pairs 5
```

These are **fixed-work, closed-loop** loads: each writer waits for successful synchronization and its selected delivery/ACK before admitting another record. They are useful for contention and resource comparisons, not an open-loop offered-rate SLO or sustained-capacity certificate. The payload is deliberately deterministic and highly compressible; gzip results do not represent high-entropy production traffic. Use representative storage, payload distributions and long-running offered-rate workloads before sizing production.

## Capture profiles separately from timings

`--diagnostics cpu`, `trace`, or `contention` profiles **every lifecycle stage**, including open, frontier discovery, append/Sync, replay and seal. CPU and execution tracing use separate runs. Contention sampling is intentionally expensive. All diagnostic runs are marked `diagnostic_only`; `compare.py` and `report.py` reject them for performance acceptance.

```sh
for kind in cpu trace contention; do
  python3 tests/e2eperf/run.py --binary "$E/worker" \
    --out "$E/diagnostic-$kind" --count 2048 --payload 16384 \
    --writers 8 --ack-percent 50 --scans 4 --diagnostics "$kind"
done
```

Every phase writes before/after allocation profiles, a post-GC live-heap profile, and profiler configuration metadata. CPU mode adds `cpu.pprof`; trace mode adds `trace.out`; contention mode adds `block.pprof` and `mutex.pprof`. Profiles are finalized **before** publishing the checkpoint, so the intentional SIGKILL does not truncate them. Existing evidence files are never overwritten.

Trace regions identify `WriteData`, `Sync/data`, `downstream/fsync-receipt`, `WriteId`, `Sync/ack`, `LoadLegacyBuf`, and replay equivalents. Phase labels distinguish top-level operations. Background goroutines may retain the label of their creation phase; do not interpret a phase label as exclusive ownership of asynchronous work.

### Interactive CPU flame graph and heap views

These commands start local-only interactive pprof views. Use the Flame Graph and Graph menus. Keep the exact measured executable with its profiles for symbolization.

```sh
go tool pprof -http=127.0.0.1:8081 -no_browser \
  "$E/worker" "$E/diagnostic-cpu/seed/cpu.pprof"

go tool pprof -http=127.0.0.1:8082 -no_browser -sample_index=alloc_space \
  -base="$E/diagnostic-cpu/scan/alloc-before.pprof" \
  "$E/worker" "$E/diagnostic-cpu/scan/alloc.pprof"

go tool pprof -http=127.0.0.1:8083 -no_browser -sample_index=inuse_space \
  "$E/worker" "$E/diagnostic-cpu/seed/heap.pprof"
```

Allocation churn and retained heap are different metrics. Subtract the before profile when analyzing allocations; the live heap is captured before closing the journal and excludes the later result-file serialization. For short phases, increase fixed work or use a separate `--profile-seconds 5` scan diagnostic; never mix its variable operation count into the fixed-work comparison.

### Scheduler, syscall and lock bottlenecks

```sh
go tool trace -http=127.0.0.1:8084 "$E/diagnostic-trace/seed/trace.out"
go tool trace -pprof=sync "$E/diagnostic-trace/seed/trace.out" > "$E/sync.pprof"
go tool trace -pprof=syscall "$E/diagnostic-trace/seed/trace.out" > "$E/syscall.pprof"
go tool pprof -top "$E/worker" "$E/sync.pprof"
go tool pprof -top "$E/worker" "$E/diagnostic-contention/seed/mutex.pprof"
```

Summed blocked-goroutine time can exceed elapsed time; it is not a percentage of wall-clock latency. The independent Python peer also performs real fsyncs. Its cost can dominate delivery throughput; worker CPU profiles do not measure the peer's CPU.

## Evidence and decisions

`result.json` contains monotonic phase intervals, exact operation counts, per-record timestamps and process resource observations. `summary.json` reports p50/p95/p99 operation latency, CPU, allocations, RSS and lifecycle throughput. `environment.json` records affinity/quota, GOMAXPROCS, filesystem mounts, CPU information and executable SHA256. RSS uses process VmHWM: it is cumulative high-water memory, not independently reset per phase.

The paired report retains individual baseline/candidate samples and candidate/baseline ratios. Its deterministic 95% paired-bootstrap intervals are **exploratory**, not multiplicity-corrected proofs. Fewer than five pairs, zero denominators, incomplete work, failed trials and diagnostic contamination cannot be called a measured improvement. The `improved`/`regressed` labels use a 5% relative threshold; inspect absolute effects and whole-lifecycle guardrails too. An inconclusive result is not proof of equivalence.

Accept production changes only after behavior/race/fuzz/negative controls and matched measurements. Preserve all regressions in the report; explain or investigate small noisy effects rather than deleting them. Do not change synchronization cadence, ACK order, generated-decoder error policy, replay sequence state, dependencies or wire format to obtain a benchmark win.

### First measured iteration (2026-09-27)

Run [36340761366](https://github.com/Laisky/go-journal/actions/runs/36340761366), source `4a41e4b26450fc153e2bf8a8a55970ac58df0524`, retained 40 baseline/candidate lifecycles and 30 isolated large-buffer experiment lifecycles. Full suite: 361 test/subtest passes; race: 1,083; ID-scan fuzz: 123,144 executions; experiment fuzz: 114,963. No behavior failure or skip occurred.

The original ID-only scan reduced 16 KiB scan median time from 614.4 ms to 172.8 ms and allocation churn from 5.212 GB to 0.292 GB. However, oversized-record scan CPU regressed: the 128 KiB inspection cap forced large buffered payloads through full string decoding. The separate allocation profile attributed 80.08% of allocation bytes to `ReadString`.

Removing only the stateless ID-scan cap (not replay's successor guard) reduced the isolated 256 KiB case's scan median from 234.8 ms to 85.6 ms and allocation churn from 2.493 GB to 0.412 GB. Paired ratios were 0.3483 and 0.1654 respectively. Whole-lifecycle throughput did not show a material improvement. A 0.294 ms median increase in the empty final verification phase was also retained and flagged for follow-up; no end-to-end speedup is claimed from the scan result.

The post-change allocation profile moved its largest share to reader-buffer allocation, motivating the next isolated buffer-size experiment rather than speculative production changes. The workflow retains exact experiment patches and results; experimental worktrees are never merged automatically.

Artifact 10938693548 ZIP SHA256: `d8d15827766cd02380bc52aa43e38e835eb6a4a3d565c56dd830f0d6123cf866`. Its 2,705 included manifest entries verified; upload-artifact omitted ten hidden `.journal.lock` files. This packaging gap is explicit, and subsequent uploads include hidden synthetic evidence files so the complete manifest can be verified.

CI publishes source, both executables, module graph, every observation, profiles, tests and SHA256SUMS. Download artifacts before their 14-day retention expires. A passing CI job means the campaign completed correctly, not that every performance metric improved or that production capacity has been certified.

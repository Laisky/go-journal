# Observable public-API journal E2E performance

An ordinary consumer of exported Journal APIs with real private data/ACK files,
an independent fsynced loopback downstream, and repeated process restart.
PR #10 targets `master`; do not merge or deploy automatically.

Current incremental baseline: `bab91ebc1119a7e6443726fe9c400902ddd6fbbb`.
Start with [the consolidated checkpoint](STATUS.md) and [transactional staging](STAGING.md).
The previous [ACK-reader reuse](ACK_REUSE.md) remains historical evidence.
Earlier [data-reader reuse](SCAN_REUSE.md), [rejected writer buffers](WRITER_RESULTS.md),
[Sync](SYNC_RESULTS.md), and [ID-scan results](RESULTS.md) keep their own baselines.
Do not attribute earlier improvements to a later patch. The PR body identifies
the latest verified head, qualification result and artifact.

## Lifecycle and correctness boundary

**seed → SIGKILL → scan → transfer → SIGKILL → deliver → verify**.
Scanning is optional with `--scans 0`. Concurrent writers append deterministic
IDs/payloads and call Sync. The independent Python peer checks complete messages
and fsyncs its ledger before issuing a matching ID/hash/durable receipt; only
then does the worker WriteId and Sync the ACK.

The controller kills seed and transfer only after their synchronized checkpoint.
A new process copies and synchronizes each pending record before requesting the
next record or EOF cleanup. Final delivery reconciles all expected identities
and hashes. The last open must preserve the ID frontier with no pending records.
Queue admission, Flush, Close and arbitrary HTTP success are not durability.
Identical retries are counted, not hidden or described as exactly-once.

`negative.py` detects executable omitted-append/ACK/transfer mutants;
`sync_experiment.py --negative` checks barrier ordering/error/cache mutants;
`segment_negative.py` checks a no-op Rotate against actual segment files;
`ack_negative.py` checks a stale per-file absolute ACK base. Compilation errors,
timeouts and missing tests are not accepted negative controls. Existing
immediate-crash, corruption, gzip-checksum and sequence tests remain. This does
not certify physical power loss or exhaustive random-point crashes.

## Build a fixed worker and run

Linux with `/proc`, Python 3.10+, the module's Go version (CI pins 1.27.1), and
private storage are required. Use fresh output paths. The controller bounds
source work to 2 GiB; total evidence size and peak memory are separate limits.

```sh
export GOTOOLCHAIN=local GOMAXPROCS=4
E=$(mktemp -d /tmp/journal-e2e.XXXXXXXX)
go mod download
go mod verify
go build -mod=readonly -trimpath -o "$E/worker" ./tests/e2eperf
python3 tests/e2eperf/run.py --binary "$E/worker" --out "$E/smoke" \
  --count 512 --payload 1024 --writers 8 --ack-percent 50 --scans 4
python3 tests/e2eperf/run.py --audit-only "$E/smoke"
python3 -m unittest discover -s tests/e2eperf -p 'test_*.py' -v
```

`count` means source records; payload excludes the identity/Unicode prefix.
A scan performs `writers × scans` complete maximum-ID scans, not that many new
deliveries. Report phase work and whole-lifecycle work separately.

## Fair paired comparisons

Compile the same current worker against both library revisions, with identical
compiler, dependencies, flags and case definitions. Historical workers are not
interchangeable. An old artifact must be audited with its matching trusted driver.

```sh
BASE=bab91ebc1119a7e6443726fe9c400902ddd6fbbb
git worktree add --detach "$E/baseline-src" "$BASE"
cmp go.mod "$E/baseline-src/go.mod"
cmp go.sum "$E/baseline-src/go.sum"
cp tests/e2eperf/*.go "$E/baseline-src/tests/e2eperf/"
(cd "$E/baseline-src" && go build -mod=readonly -trimpath \
  -o "$E/worker-baseline" ./tests/e2eperf)
python3 tests/e2eperf/compare.py --baseline "$E/worker-baseline" \
  --candidate "$E/worker" --cases tests/e2eperf/staging_cases.json \
  --pairs 5 --out "$E/paired"
python3 tests/e2eperf/report.py "$E/paired/report.json"
```

AB/BA ordering alternates. Every result is independently audited before
aggregation; failures are retained, never replaced. Only accepted synthetic WAL
files are removed; observations, peer ledgers and process evidence remain.

## Multi-segment and concurrency loads

```sh
python3 tests/e2eperf/run.py --binary "$E/worker" --out "$E/segments" \
  --count 512 --payload 65536 --writers 1 --ack-percent 50 \
  --rotate-every 64 --scans 16

python3 tests/e2eperf/stress.py --baseline "$E/worker-baseline" \
  --candidate "$E/worker" --out "$E/concurrency" --count 512 \
  --payloads 1024 --writers 1,8,32 --ack-percents 0,100 \
  --codecs plain,gzip --scans 0 --pairs 5
```

`rotate-every` calls public Rotate after each N completed seed operations, capped
at 256 rotations. The count is audited; physical segment/replay tests reject fake
rotation counters. Per-segment preallocation is bounded by the frozen workload
formula in [SCAN_REUSE.md](SCAN_REUSE.md), identically for both revisions.
`stress.py` caps matrices at 36 cases and ten pairs; `--generate-only` previews one.

These are fixed-work, closed-loop loads. Writers wait for their durability and
selected delivery/ACK steps. Payloads are deterministic and highly compressible;
representative entropy, open-loop arrival rates, sustained saturation and long
soaks require additional workloads. No production capacity claim follows.

## Environment observation and timing qualification

`observe.py` samples host CPU/IO pressure, disk counters, dirty pages and mounted
cgroup counters without instrumenting the Go worker. Missing counters are errors,
not zero load. Parent-death guards and TERM/KILL cascade tests prevent nested
loads surviving an interrupted owner; guards require Linux and non-setuid programs.

```sh
python3 tests/e2eperf/observe.py --out "$E/host" -- \
  python3 tests/e2eperf/compare.py --baseline "$E/worker-baseline" \
    --candidate "$E/worker" --cases tests/e2eperf/staging_cases.json \
    --pairs 5 --out "$E/observed-pairs"
```

CI runs identical-binary controls before and after the campaign. `qualify.py`
requires the entire lifecycle/p99 paired interval inside [0.9, 1.1] and max/min
sample span at most 1.25. A failed qualification is retained, not rerun until green.
Correctness/byte-count evidence remains useful, but timing claims must explicitly
state the qualification failure. Qualification covers only its control workloads;
it is not proof of exclusive resources, equivalence or universal speedup.

## Profiles: separate from acceptance timings

```sh
for kind in cpu trace contention; do
  python3 tests/e2eperf/run.py --binary "$E/worker" \
    --out "$E/diagnostic-$kind" --count 512 --payload 256 \
    --writers 1 --ack-percent 100 --rotate-every 8 --scans 64 \
    --diagnostics "$kind"
done
```

Each profiled worker includes open, frontier, append/Sync, replay and seal. It
writes before/after allocations, post-GC live heap and profiler metadata; CPU
adds `cpu.pprof`, trace adds `trace.out`, contention adds block/mutex profiles.
Profiles close before intentional SIGKILL. CPU and tracing use separate runs.
All diagnostic trials are marked and rejected from paired timing acceptance.

Trace regions include WriteData, Sync/data, downstream/fsync-receipt, WriteId,
Sync/ack, LoadLegacyBuf and Rotate/seed. Phase labels do not exclusively own
background goroutines created in a phase. Worker profiles omit Python-peer CPU;
the peer's persistence cost can dominate delivery.

### Interactive flame graph, allocation and live-heap views

```sh
go tool pprof -http=127.0.0.1:8081 -no_browser \
  "$E/worker" "$E/diagnostic-cpu/scan/cpu.pprof"
go tool pprof -http=127.0.0.1:8082 -no_browser -sample_index=alloc_space \
  -base="$E/diagnostic-cpu/scan/alloc-before.pprof" \
  "$E/worker" "$E/diagnostic-cpu/scan/alloc.pprof"
go tool pprof -http=127.0.0.1:8083 -no_browser -sample_index=inuse_space \
  "$E/worker" "$E/diagnostic-cpu/scan/heap.pprof"
go tool trace -http=127.0.0.1:8084 "$E/diagnostic-trace/scan/trace.out"
go tool trace -pprof=syscall "$E/diagnostic-trace/scan/trace.out" > "$E/syscall.pprof"
go tool pprof -top "$E/worker" "$E/syscall.pprof"
```

Use pprof's Flame Graph/Graph menus and the exact executable. Allocation churn
is not retained heap; subtract the before profile. Live heap is captured before
journal Close and result serialization. Summed goroutine wait time is not wall
latency. A duration-based `--profile-seconds` diagnostic has variable work and
must not enter fixed-work comparisons.

## Retained evidence

`result.json` contains monotonic intervals, exact work, timestamps and counters.
`summary.json` adds latency percentiles, CPU, allocations, GC cycles, mallocs and
process-high-water RSS. VmHWM is cumulative per process, not reset per phase.
Reports keep all samples, paired ratios and exploratory 95% bootstrap intervals.
The 5% improvement/regression labels are not multiplicity-corrected; inspect
absolute effects, controls and lifecycle guardrails. Inconclusive is not equivalent.

`verify.py` recomputes manifests, binary identity, options, audits, assessments and
displayed medians. Store its output outside the immutable evidence directory.
CI includes hidden synthetic files so SHA256SUMS can be checked completely.
Artifacts retain sources, executables, module graph, profiles and observations
for fourteen days. Preserve a verified copy. Passing CI means the campaign and
correctness checks completed, not that every performance metric improved.


## Worker observation and metric continuity

The controller now reacts to checkpoint stdout and Linux process-exit notifications
without waiting for the 20 ms resource-sampling tick. This is a **harness change**,
not a journal optimization. New trial options/summaries identify `events-v2` and
the actual `pidfd` or `pipe-poll` backend; mixed methods/backends are rejected by
paired reporting. Use one frozen current harness against both library versions.
Do not compare old poll-based lifecycle numbers directly with new event-based ones.

`worker_phase_seconds` sums the Go-reported phases of seed, transfer, deliver and
verify, exactly the stages included by `lifecycle_seconds`; the standalone scan
stage remains separate. `outside_phase_seconds` is their difference, including
interpreter/process startup, uninstrumented initialization, resource snapshots,
result serialization and shutdown/observation. It is **not all removable overhead**,
nor an estimate of fsync cost. Worker-internal p99 and profiling code are unchanged.

See [observer implementation, measurements and validation limits](OBSERVER_RESULTS.md).

## Current write-path experiment

The current data encoder reassigns its original 4 MiB output-buffer budget to
complete private record staging, retaining only excess bytes in temporary spill
storage. Data output buffering is 4 KiB; ACK and compressor buffers are unchanged.
Both prefix and overflow are validated before live append. A failed live append
still poisons the encoder. This is not atomic filesystem writing or a weaker
Sync/ACK policy. [STAGING.md](STAGING.md) separates the two measured candidates,
accepted implementation, constructor/overflow controls and timing limitations.

```sh
python3 tests/e2eperf/run.py --binary "$E/worker" --out "$E/staging-cpu" \
  --count 256 --payload 262144 --writers 4 --ack-percent 0 \
  --scans 0 --diagnostics cpu
go tool pprof -top -sample_index=alloc_space \
  -base="$E/staging-cpu/seed/alloc-before.pprof" \
  "$E/worker" "$E/staging-cpu/seed/alloc.pprof"
python3 tests/e2eperf/staging_negative.py --out "$E/staging-negative"
```

The bypass-staging mutation must fail a real rejected-payload/live-file assertion;
compiler errors or timeouts are not accepted. The overflow campaign reconstructs
the pinned first prototype only as a reference, never by weakening production.

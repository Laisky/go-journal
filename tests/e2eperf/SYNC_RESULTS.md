# Observable Sync optimization — follow-up to PR10 ID scans

The incremental baseline is the previously accepted PR10 head
`a295f519c37dce75ddedac3803f26a46c9ad7279`, **not** the older `fc156a60` scan baseline.
The [earlier scan results](RESULTS.md) remain historical evidence. This iteration
moves to the durability/lock bottleneck identified by execution tracing.
The PR description links the final exact-head workflow and downloaded evidence;
the first isolated experiment below is retained rather than replaced by later runs.

## Implementation and correctness boundary

`Journal.Sync` now shares an **overlapping** barrier. It does not cache success,
add a batching timer, reduce application Sync calls, acknowledge early, or omit
any file/directory synchronization within a barrier. `syncLocked` is unchanged.

An owner registers a running generation, releases the coordinator mutex, and
acquires the journal's existing exclusive lock. It checks lifecycle state and
flushes/synchronizes both files and the directory. Followers join the running
generation and wait for its result. The owner publishes that result and retires
the generation **before releasing the journal lock**.

That ordering is essential: a follower's completed write must either precede
exclusive ownership and be covered by the barrier, or be blocked until ownership
ends. By that time the old generation is no longer joinable. Publishing after
unlock would introduce a window in which a later completed write could join an
already-finished barrier. The negative-control suite deliberately makes that
mistake and requires the intended assertion to fail.

Channel close publishes the immutable result to followers, following the
[Go memory model](https://go.dev/ref/mem#chan). A failed barrier releases all
followers with its error; later calls retry a fresh barrier. Panic/Goexit also
releases followers with an error without swallowing the owner's panic. Rotation,
cleanup and Close keep their existing locking and direct barrier paths. Closed
and not-started journal behavior remains checked. No dependency or data-format
migration is required.

The first coordinator eagerly allocated a channel/result for every owner. The
retained implementation instead creates that object **only when a follower
actually arrives**. Sequential coordination has a zero-allocation regression test.
There is still one result per contended generation, so concurrent callers never
read a mutable result reused by a later generation.

## First isolated, native experiment

Source `f5b0609477fb5ce80096c2cb59fa15b6faaf116f` plus the retained one-file
`candidate.patch`, run [36346678532](https://github.com/Laisky/go-journal/actions/runs/36346678532).
The patch integrated the eager coordinator only in a detached worktree before
production adoption. Baseline and candidate used the same external worker source,
Go 1.27.1, unchanged module graph, GOMAXPROCS=4 and five alternating pairs.

| Workload | Lifecycle median, baseline → candidate | Median paired lifecycle reduction | Median paired append-to-Sync p99 reduction |
| --- | ---: | ---: | ---: |
| Plain 1 KiB, 16 writers, all pending | 3.054449 → 2.610676 s | 14.2% | 61.0% |
| Plain 1 KiB, 32 writers, all initially ACKed | 1.158109 → 0.992085 s | 14.3% | 83.9% |
| Gzip 1 KiB, 16 writers, 50% initial ACK | 2.694934 → 1.915026 s | 28.9% | 75.1% |

These are lifecycle measurements, not just a scan or coordinator microbenchmark.
Single-writer lifecycle and p99 controls were inconclusive; no single-writer
speedup or equivalence claim follows. Percentages use median paired ratios,
not ratios of the displayed medians. Intervals are exploratory bootstrap estimates
without multiplicity correction.

Separate seed traces recorded **4,650 → 1,422 fsync calls** under `syncLocked`
for the same 1,024-record, 16-writer, 50%-ACK diagnostic workload. This explains
how the unchanged barrier implementation performs less redundant work. Tracing
perturbs scheduling and group membership; these counts are diagnostic evidence,
not a guaranteed production reduction or profiled timing acceptance.

The first run also retained a real small-record replay allocation regression:
**1,143,576 → 1,213,448 bytes**, a paired increase of **6.1%**. The increase is
consistent with 512 sequential calls allocating approximately 136 bytes each.
The lazy-notification variant addresses this cost instead of hiding the regression.
`lazy_cases.json` directly compares the initial eager implementation with the
retained implementation, including the affected replay and single-writer cases.
The workflow also records native, isolated allocation benchmarks for both variants.

Other initial exploratory flags were small absolute effects: plain four-writer
seed/seal CPU increased 1.321 ms, and the plain 16 KiB final frontier phase increased
about 0.144 ms. All samples remain in the first artifact. They are not deleted or
called equivalent simply because the changed code does not directly own the phase.
The initial A/A control produced only inconclusive results.

## Tests that protect the durability boundary

The coordinator suite checks shared success/error, retirement before unlock,
sequential calls not reusing cached results, aborted owners releasing followers,
and 4,096 concurrent completed-write/returned-barrier relationships. Three real
mutants must fail their intended assertion: late publication, hidden errors and
cached completed barriers. A compile failure or timeout does not satisfy a control.

Six new public-API subprocess cases cover plain/gzip × 0/50/100% ACK. Sixteen
writers call individual WriteData/Sync and WriteId/Sync operations. The parent
kills the process immediately after their completion, **without a later Rotate,
final Sync or Close that could repair a false durability claim**. A new process
checks every pending payload and the maximum ID. An additional directory-failure
case verifies concurrent error delivery and a successful later retry.

These complement the existing randomized race, lifecycle, corruption, append
failure, selective replay and fuzz suites. They do not claim physical power-loss
certification or exhaustive random-instruction crash coverage.

## Reproduce incremental comparisons and verify artifacts

Build the same current worker against `a295f519` and the current library, following
[the common build instructions](README.md#build-a-fair-baseline), but substitute
`BASE=a295f519c37dce75ddedac3803f26a46c9ad7279` for this iteration.

```sh
python3 tests/e2eperf/compare.py \
  --baseline "$E/worker-baseline" --candidate "$E/worker" \
  --cases tests/e2eperf/sync_cases.json --pairs 5 --out "$E/sync-paired"
```

The native workflow builds its candidate from the exact checked-in head with no
implicit integration patch. For the separate eager/lazy comparison it reconstructs
the pinned historical eager source and records that source and its explicit patch.
CPU, heap, mutex and execution-trace captures remain separate from timing trials.

`verify.py` checks the complete SHA256 manifest, frozen workload parameters,
executable identity, CPU/platform settings, every independent lifecycle audit,
reported summaries, displayed medians and paired assessments. Run it from trusted
repository source. Save its output **outside** the immutable evidence directory:

```sh
python3 tests/e2eperf/verify.py --root "$E" \
  --campaign sync-paired=worker-baseline,worker-candidate \
  --campaign scan-guardrails=worker-baseline,worker-candidate \
  --campaign lazy-paired=worker-eager,worker-candidate \
  --campaign aa=worker-candidate,worker-candidate > verified-sync.json
```

For the first experiment, omit the `lazy-paired` argument. Its downloaded artifact
[10941431353](https://github.com/Laisky/go-journal/actions/runs/36346678532/artifacts/10941431353)
has ZIP SHA256 `6c608fa001b5631bac6c8a206c6b12eb060c56d1005fbc2c2faf54d9818e2e9c`.
All **3,840 manifest files** and **120 paired lifecycle trials** were independently
rechecked after download: 70 Sync workloads, 40 scan/recovery guardrails and 10 A/A
controls, reconciling **180,800 source deliveries** with zero observed duplicates.
These counts describe the first experiment, not the final-head campaign.

Loads remain fixed-work, closed-loop and deliberately compressible. Independent
peer fsync, serial replay delivery and storage still limit throughput. No open-loop
sustainable rate, production sizing, long-duration soak, exactly-once guarantee or
global optimality is inferred. There is no automatic merge or deployment.

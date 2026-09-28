# Recovery scans: buffered ACK folding and single-pass metadata

Incremental baseline: `b4792253d0260b0cae0c5731c5dad4d755646c56`. This already
includes transactional staging, ID-only scanning, Sync coalescing and data/ACK
reader reuse. Their earlier gains are not credited to this increment. The exact
combined integration measured below is now enabled. The PR body records the
latest exact-head native validation; an isolated prototype is not substituted
for checked-in-source acceptance.

## Two independent changes, without a new cache or weaker persistence

**Buffered ACK maximum.** Read and validate the absolute base through the existing
scalar path, then fold complete 8-byte signed deltas already in the read buffer.
No extra read-ahead, global cache or retained borrowed slice. An invalid word
returns the same error and leaves exactly the same unread suffix. A partial tail
and pending I/O/gzip-checksum errors remain for the ordinary scalar ReadFull path.
Bitmap and ACK-set loading are unchanged. The data format, base-ID interpretation,
Sync/ACK order and cleanup frontier rules are unchanged.

The initial CPU profile put 2.28 sampled CPU-seconds under `io.ReadFull` in the
labeled public scan, versus 0.05 after buffered folding. Labeled Journal.LoadMaxId
samples fell from 3.01 to 0.46 CPU-seconds for the same fixed-work profile command.
These are sampled mechanism evidence, not wall-clock or production SLO estimates.

**Directory preparation.** Enumerate sorted names with `os.ReadDir` rather than
requesting eager FileInfo/Lstat for every entry, then calling Stat again. Keep the
existing explicit `os.Stat` on EVERY entry, including unknown and dangling symlinks.
Ordering, symlink resolution, collision avoidance, error-before-create behavior
and new-file preparation remain intact. No cross-call metadata cache is added.
Concurrent external directory mutation can change observation timing; the journal
still owns its directory and no new snapshot-atomicity guarantee is introduced.

## Initial isolated experiment, before adoption

Source `e57446091406c5d2e22e88c911a26c97b50b503e`, tree
`ec775fdc40a08549de9320f4deadf43aaebc1422`, run
[36374321445](https://github.com/Laisky/go-journal/actions/runs/36374321445).
The workflow retained separate baseline, ACK-only, directory-only and combined
patches/binaries. Both changes were measured independently before enabling their
combined production integration. Go 1.27.1, unchanged modules and GOMAXPROCS=4;
five alternating pairs, without profilers in acceptance timings.

| Public API operation | Baseline median | Isolated candidate median | Median paired time ratio |
| --- | ---: | ---: | ---: |
| LoadMaxId: 8,192 plaintext ACKs | 144.985 us | 47.421 us | 0.32863 |
| LoadMaxId: 131,072 plaintext ACKs, one segment | 1.488668 ms | 0.233303 ms | 0.15752 |
| LoadMaxId: 131,072 plaintext ACKs, eight segments | 1.617644 ms | 0.291328 ms | 0.17990 |
| PrepareNewBufFile: 256 directory entries | 0.766465 ms | 0.545887 ms | 0.71661 |
| PrepareNewBufFile: 4,096 directory entries | 13.103947 ms | 9.635974 ms | 0.73850 |

ACK measurements construct real histories using public WriteId/Sync/Rotate,
close and reopen, time repeated public LoadMaxId, then verify cleanup/reopen
preserves an old maximum. Timed scans are warm-cache; fixture generation and final
cleanup are excluded. ACK-only history represents completed deliveries, not new
message throughput. Every returned maximum is checked. The gzip timing control
is inconclusive, as are ACK allocation comparisons; no gzip speedup or new ACK
buffer-memory saving is claimed.

The directory benchmark times exported PrepareNewBufFile plus actual new-file
close/removal, over a fixed real directory. It does not merely parse names.
At 4,096 entries, allocation fell from 3,599,776 to 2,910,370 B/op (paired ratio
0.80862), allocations from 32,865 to 28,765/op. The 16-entry timing control is
inconclusive, though allocation decreases. Separate strace diagnostics counted
270,337 -> 135,169 metadata syscalls. Counts include setup/cleanup and both Go
benchmark calibration and 32 fixed iterations; the 135,168-call reduction matches
4,096 entries times 33 scans. Instrumented times are not acceptance timings.

Identical-binary controls accompany both public benchmark suites and produced no
improved/regressed labels. The 131k one/eight-segment time intervals were
[0.96405, 1.03671] and [0.96847, 1.03162]. Small-operation controls are noisier;
these descriptive intervals do not certify all hardware or whole-lifecycle timing.

## Full lifecycle observations and retained regressions

The combined candidate also completed 80 independently audited unprofiled
lifecycles: 40 main comparisons and 40 before/after A/A controls. All 110,080
source deliveries reconciled, with zero observed duplicates. Four cases include
256 rotations, dense ACKs, gzip and single-segment controls. The dense-ACK full
scan phase used 256 complete scans: CPU median 206.927 -> 174.036 ms (paired ratio
0.84251), elapsed median 55.969 -> 49.598 ms (paired ratio 0.83228).

All four main whole-lifecycle comparisons were inconclusive. The original
before/after lifecycle/p99 qualification is still **false**; it was not relaxed.
Do not generalize the public scan/preparation results into sustained delivery
throughput, qualified end-to-end p99 or production capacity.

Both initial main regression flags remain visible: many-segment transfer/frontier
allocation increased 439,296 -> 476,384 B (paired ratio 1.08432); small-control
seed/open elapsed increased 1.275 -> 1.425 ms (paired ratio 1.11373). Their precise
causes are not established, and they are not erased because other metrics improve.
No A/A full-lifecycle campaign produced regression labels. Bootstrap labels are
exploratory, without multiplicity correction; inconclusive does not mean equivalent.

## Correctness and evidence

Full native suite: 547 test/subtest passes; three shuffled race repetitions:
1,641 passes; 41,688 ACK maximum differential fuzz executions and 106,186 ID-scan
fuzz executions. Ordinary tests had no failures or skips. The unchanged baseline
also passed the new public directory/ACK maximum boundary tests. Supplemental
standard-library-only local tests used Go 1.23.2, not the repository's native version.

Tests compare full maxima, error text/classes, base state and exact remaining
bytes against independent word-at-a-time references, across truncated/invalid
records, integer bounds, varying buffer sizes, terminal device errors and gzip
checksum/member cases. Directory tests preserve explicit validation, sorted
ordering and sparse public replay after rotation/reopen. The adoption workflow
also runs real invalid-delta and skipped-Stat mutants, with positive controls:
compilation failure, timeout or missing tests cannot satisfy those assertions.

Downloaded artifact [10950890688](https://github.com/Laisky/go-journal/actions/runs/36374321445/artifacts/10950890688),
SHA256 `a72c22b3daeeef63345fa3dcf09c0c5f9d883dbda46e84dc1d9f83c16830f9d1`.
All 2,714 manifest entries and the complete 140-file source tree were independently
verified. All 80 lifecycle audits, four public benchmark reports, paired assessments
and timing qualification were recomputed. Two diagnostic trace lifecycles are
separate. Verification outputs remain outside the immutable artifact directory.

## Reproduction and exact-head validation

`recovery_experiment.py` retains the isolated transforms. The checked-in workflow
now uses `CANDIDATE_MODE: adopted`, requires an empty candidate.patch, and measures
the actual candidate test binary for both public benchmark suites. Historical
isolated binaries are not substituted for the current implementation. The same
frozen workload counts, before/after controls and baseline are retained.

Build the same public benchmark source against each library revision:

```sh
BASE=b4792253d0260b0cae0c5731c5dad4d755646c56
E=$(mktemp -d /tmp/journal-recovery.XXXXXXXX)
git worktree add --detach "$E/base" "$BASE"
cp ack_*.go directory_*test.go "$E/base/"
(cd "$E/base" && go test -mod=readonly -trimpath -c -o "$E/bench-baseline" .)
go test -mod=readonly -trimpath -c -o "$E/bench-candidate" .
python3 tests/e2eperf/bench_pairs.py --baseline "$E/bench-baseline" \
  --candidate "$E/bench-candidate" --suite ack --pairs 5 --iterations 64 \
  --out "$E/ack"
python3 tests/e2eperf/bench_pairs.py --verify "$E/ack"
```

Use one compiler/module graph/GOMAXPROCS setting. `recovery_cases.json` runs the
existing independent durable peer and crash/recovery oracle. CPU profiles label
public ACK scans to separate them from fixture generation. Metadata syscall
tracing and all profiling remain diagnostic-only. No durability barrier, record
validation, cleanup or replay-state guard is removed to obtain these results.

Primary contracts: https://pkg.go.dev/bufio#Reader.Peek,
https://pkg.go.dev/bufio#Reader.Discard, https://pkg.go.dev/io#ReadFull,
https://pkg.go.dev/os#ReadDir.

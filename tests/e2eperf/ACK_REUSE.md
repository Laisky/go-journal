# Operation-local ACK reader reuse

This iteration starts from recovered PR10 head
`71790780de0b665e1c33b2583c424fcc770944b2`, already including data-reader reuse,
ID-only scanning and overlapping Sync. Earlier improvements are not credited
again. [STATUS.md](STATUS.md) identifies the consolidated work and
[PR #10](https://github.com/Laisky/go-journal/pull/10) records the latest exact-head
replication, CI and downloaded artifact. No automatic merge or deployment.

## Change and invariant

After data-reader reuse, every nonempty ACK file still constructed a fresh
64 KiB bufio reader. `scanIDBuffers` now belongs to one synchronous traversal:
maximum-ID scanning, loading the replay ACK snapshot, or planning cleanup.
Concurrent traversals have separate buffers; neither Journal nor LegacyLoader
retains one after the operation. No global pool, altered read size, skipped file
or weaker synchronization is introduced.

Each ACK file starts with its own absolute ID. Before releasing a plaintext
reader, reset `baseID` to -1, clear the eight-byte scratch word, and Reset the
reader to nil to discard unread bytes, errors and the file reference. The next
file resets the reader to its own descriptor. Gzip construction remains fresh
for every file, including header/member/checksum validation. Empty-file, open,
stat, decode, callback and close errors keep the original per-file policy.

`serialize.go`, `journal.go`, data scanning, exact-ID acknowledgement checks,
cleanup planning and all file/directory barriers are unchanged from this
iteration's baseline. Only ACK-reader allocation is shared across files.

## First isolated, native campaign

Run [36358799934](https://github.com/Laisky/go-journal/actions/runs/36358799934)
used source `21c9fcc959d34013336c13f7b14fbb88a3e81735` plus the explicit retained
`candidate.patch` in a detached worktree. Both binaries used identical current
worker source, Go 1.27.1, unchanged go.mod/go.sum, GOMAXPROCS=4 and one runner.
All five native workflows passed. The integration was adopted only after this
campaign and its downloaded evidence were read and independently verified.
The adopted `legacy.go` blob is `78b91e4bedd91c1f871ab33b07064025abe29a43`, matching
the measured patch exactly; a caught intermediate transcription error was
corrected before exact-head testing, not treated as a passing implementation.

Each row has five alternating AB/BA pairs. Allocations and GC are complete
scan-phase observations, not per-message latency or retained heap. Percentages
use medians of paired ratios rather than ratios of the displayed medians.

| Plain ACK workload | Scan allocated bytes, baseline → candidate median | Paired reduction | GC cycles, baseline → candidate |
| --- | ---: | ---: | --- |
| 128 segments; 256 B payloads; all ACKed | 137,773,048 → 3,899,672 | 97.17% | 16–17 → 0 |
| 32 segments; 256 B payloads; sparse ACKs | 28,988,176 → 2,596,984 | 91.04% | 3 → 0 |
| Concurrent scans; 1 KiB payloads; sparse ACKs | 51,519,128 → 5,259,560 | 89.86% | 4–6 → 0–1 |

The first two cases perform sixteen full scans; the concurrent case performs
thirty-two. The prior 32-small-segment guardrail also reduced scan allocation
by 90.97%. Its 64 KiB-record guardrail reduced allocation by 8.67%; the 256 KiB
case's 4.81% observation did not meet the predeclared 5% threshold. Empty-ACK,
gzip and single-segment allocation controls remained inconclusive.

ACK-heavy replay transfer (including traversal/cleanup with no pending records)
allocated 25,774,248 → 9,010,504 bytes, a paired 65.06% reduction. Sparse-ACK
replay transfer allocated 6,707,920 → 3,423,392 bytes, a 48.97% reduction. This
also covers the ACK-snapshot and cleanup use of the same scoped reuse helper.
It does not mean fewer source messages were checked or delivered.

Separate fixed-work allocation profiles attributed 96.60% of baseline sampled
allocation bytes to `bufio.NewReaderSize`: 2,023.56 MB before versus 25.53 MB
in the candidate profile. The diagnostic uses 512 repeated scans plus the
initial frontier over 64 requested rotation intervals. CPU, allocation delta,
post-GC heap and execution-trace/syscall data are retained separately from the
unprofiled trial timings. Profile totals are sampled estimates, not exact
runtime counters or resident memory.

## Timing and unfavorable results

**Timing qualification is false.** The unchanged before/after A/A requirements
include both an entire paired interval inside [0.9, 1.1] and max/min sample span
at most 1.25 for lifecycle and seed p99. Both controls failed p99 requirements;
no policy is relaxed after observing results. All six main-case lifecycle and
seed-p99 assessments remained inconclusive. No qualified whole-lifecycle speedup,
universal equivalence, or production latency SLO is claimed.

All eight main/guardrail exploratory regression flags are retained below. Units
are milliseconds; CPU time is not wall time. These effects are not proved to be
noise or ignored simply because their absolute values are small.

| Case / metric | Baseline → candidate median |
| --- | ---: |
| Sparse 32-segment final verification CPU | 1.123 → 1.603 |
| Concurrent ACK final verification CPU | 1.039 → 1.430 |
| Prior 32-small-segment final verification CPU | 1.086 → 1.691 |
| Same final verification elapsed | 0.898 → 1.208 |
| Prior oversized transfer frontier CPU | 4.555 → 4.964 |
| Same transfer seal CPU | 0.644 → 1.931 |
| Same transfer seal elapsed | 0.745 → 1.182 |
| Same final verification CPU | 0.941 → 1.476 |

Before-A/A also flagged delivery-frontier CPU, 0.173 → 0.350 ms, on the same
binary; after-A/A had no regression label. These do not override failed timing
qualification. Intervals are exploratory, not multiplicity-corrected. Exact-head
replication is reported separately rather than overwriting the initial evidence.
The accepted scope is allocation/GC improvement, not an unqualified speedup.

## Correctness and verification

Independent wire fixtures exercise changing absolute bases, decreasing IDs,
maximum integer values, negative/overflow records, every 1–7 byte truncation,
read-buffer boundaries and a following clean file. Early consumer failure
leaves unread offsets deliberately; the next file must still decode correctly.
Fresh and reused readers must produce identical values and error strings.
Gzip checks include empty/header/truncated/checksum failures and multiple members.

Public tests build ACK-only journals with the largest ID in an old segment,
perform concurrent scans, clean up, close and reopen, and require the same ID
frontier. The stale-base executable mutant must fail its intended assertion;
compilation errors and timeouts cannot satisfy a negative control. Its runner
now uses the existing parent-death-safe supervision so interruption cannot
leave a nested test process running. Existing crash, replay, corruption and
Sync tests remain in force.

The isolated campaign passed 510 full-suite test/subtest executions, 1,530 in
three shuffled race repetitions, 116,638 ID-scan fuzz executions, 24 Python test
methods, vet/module verification and eight intended executable mutation failures.
There were no ordinary correctness failures or skips.

[Artifact 10945565149](https://github.com/Laisky/go-journal/actions/runs/36358799934/artifacts/10945565149)
ZIP SHA256: `1b5622cb00243e964eec23b4ba0a61f012e3be7e0aa873a9c770d91d02eb5a48`.
All 5,282 manifest files and 160 trial audits were independently recomputed:
60 ACK comparisons, 60 prior segment guardrails and 40 before/after A/A controls.
All 88,320 source deliveries reconciled, with zero observed duplicates (not an
exactly-once guarantee). Paired assessments, displayed medians and qualification
also reproduced. The archived source tree matches
`4deb279e66e142f650e14b627c4c786bc2405435`; the candidate integration patch is explicit.

## Reproduce

Follow [README.md](README.md) using baseline `71790780...`, then run:

```sh
python3 tests/e2eperf/compare.py --baseline "$E/worker-baseline" \
  --candidate "$E/worker" --cases tests/e2eperf/ack_cases.json \
  --pairs 5 --out "$E/ack-paired"
python3 tests/e2eperf/ack_negative.py --out "$E/ack-negative"
```

The workflow's adopted mode builds the checked-in head and requires an empty
candidate patch. Verify downloaded manifests and every trial using trusted,
matching verifier source; write verification output outside the immutable
artifact. The exact-head PR report includes the final run/artifact and all
remaining regression flags. Artifacts expire after fourteen days.

Fixed-work, closed-loop, highly compressible synthetic loads do not establish
sustained open-loop capacity, representative production traffic, long-duration
soak reliability, physical power-loss behavior or global optimality.

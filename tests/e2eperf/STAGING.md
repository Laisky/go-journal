# Transactional staging: reassign, do not enlarge, the buffer budget

Incremental baseline: `bab91ebc1119a7e6443726fe9c400902ddd6fbbb`. Earlier
scan, Sync and observer changes are retained and not credited to this iteration.

## Hypothesis and preservation boundary

The baseline data encoder copies a complete record to private `bytes.Buffer`
scratch, then into a 4 MiB live MessagePack writer. Scratch over 128 KiB is dropped
after every append, so repeated large records allocate again despite an existing
4 MiB output buffer. Reassign that 4 MiB to private record staging and use a
4 KiB output writer. ACK/compressor/read buffers and every Flush/Sync/ACK operation
are unchanged. This is not the rejected experiment that simply shrank buffers.

The staging arena belongs to the encoder mutex, not a global pool. EncodeMsg is
still called exactly once via msgp.Encode; Encodable-only values and failures
preserve their semantics. No live append occurs before the complete encoding
succeeds. Valid records are immediately appended/flushed as before, and a live
append failure still poisons the encoder. Overflow beyond the 4 MiB arena is
accepted, but its storage is dropped after success, rejection or panic. First
overflow is request-sized plus 4 KiB headroom rather than doubling the arena;
further fragmented overflow grows amortized. No unsafe or borrowed caller data.

Steady data buffering is approximately 4 MiB + 4 KiB, replacing 4 MiB output plus
up to 128 KiB scratch. This is a memory-budget reassignment, not a proof of lower
RSS under all loads. Fresh construction, overflow and single-writer controls
must remain visible alongside steady-state allocation savings.

## Experiment status

The initial CI applies `staging_experiment.py` only in a detached candidate
worktree and retains candidate.patch. The main branch's serializer is unchanged
until matched results are read and accepted. All cases have five alternating
pairs; production adoption requires a subsequent exact-head run with empty patch.

Nine fixed workloads cover 256 B through 4 MiB+envelope, single/concurrent writers,
plain/gzip, rotation and ACK-only cleanup. Existing behavior/race/fuzz and all
eight mutation controls run with the candidate. High-entropy boundary/rejection
tests compare generated encodings. The independent durable peer and restart oracle
are unchanged. Two before/after A/A controls retain the original qualification.

Prepared-payload /dev/null benchmarks isolate serialization allocations and are
not durability benchmarks. Separate fixed-work CPU/allocation/live-heap/trace
runs cover both versions. Retain unfavorable samples; do not use profiler timings
or unqualified intervals to claim production latency/capacity. The PR body names
the exact accepted commit and archived evidence when validation completes.

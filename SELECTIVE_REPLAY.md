# Selective acknowledgement replay

`LegacyLoader.Load` can avoid materializing the payload of an already acknowledged
record. It validates complete envelopes in the ordinary reader's **already
buffered bytes** before checking exact acknowledgement membership. No on-disk
format, exported API, durability barrier, recovery-tail policy or cleanup order
changes. A maximum acknowledged ID is never a substitute for exact membership.

The fast path recognizes one `Data` map and one integer `ID` in either order,
including existing Data-before-ID records. It validates primitive values, arrays
and string-keyed maps. Pending payloads use the original generated decoder and
retain independent, owned values. Existing ACK membership semantics are preserved;
tests cover both retained membership and consume-once callbacks.

Unknown/duplicate/missing envelope fields, extensions, deeper structures, records
larger than 128 KiB and records crossing the current buffer boundary use the
original decoder. This is an optimization bound, not an acceptance limit.
Extension validators and malformed non-string map keys cannot be bypassed by a
generic MessagePack Skip. Peek is never used to discard a stored I/O/checksum error.

## Sequence compatibility correction after PR8

PR8 was merged as `8758f84c23d9ae29bf800f9e84d2ec64125008aa` without the
previously local-only sequence correction. A fallback for an unfamiliar record is
not sufficient when the preceding acknowledged record was skipped: the generated
legacy decoder preserves fields absent from the next record in its destination.

For example, an acknowledged ID9/payload A followed by an envelope containing only
ID10 must retain payload A. An envelope lacking ID must preserve the preceding ID
and its acknowledgement behavior, not inherit the caller's stale sentinel.
These layouts are accepted by the old decoder; normal `WriteData` emits both fields.

Fast skipping now also requires an already-buffered complete canonical successor,
so both fields will be overwritten. Before unfamiliar/cross-buffer records and at
segment ends, materialize the preceding record normally. Decide this **before ACK
lookup** to preserve consume-once callbacks without an optimistic second check.
No borrowed payload storage or new retained-state buffer is introduced.

The correction and regression tests were recovered unchanged from the prior
validated implementation (production/test tree `f742df7c`). Eight public cases
cover both missing fields, plain/gzip storage, and same/cross-segment sequences.
Stateful differential tests additionally compare retained/consume-once ACKs,
fragmented readers, truncated tails and 64 envelope layouts against full decoding.
Existing sparse-ACK, corruption, interrupted-append, crash, cleanup, ownership and
synchronization tests remain.

Reproduce with the repository's native dependencies and Go 1.27.1:

```sh
go test -mod=readonly -count=1 ./...
go test -mod=readonly -race -shuffle=on -count=3 ./...
go test -mod=readonly -run '^$' -fuzz '^FuzzSelectiveAcceptedFrames$' -fuzztime=8s
go test -mod=readonly -run '^$' -fuzz '^FuzzSelectiveSequenceState$' -fuzztime=8s
python3 .scripts/verify_selective_sequence.py /tmp/sequence-controls
go test -mod=readonly -run '^$' -bench '^BenchmarkSelectiveAcknowledgedReplay$' -benchmem -count=3
```

The permanent CI also removes just the successor guard in a disposable copy and
requires all eight named public cases to fail their behavioral assertions. A
compiler error, timeout, missing test or skipped case does not count as detection.
The unmodified public cases and stateful differential controls must pass first.
CI retains raw logs and Go-generated module version/checksum/zip metadata for
immutable downstream adoption in go-fluentd PR18.

## Allocation tradeoff and measurement provenance

Previously recorded local Go1.27.1 tests of this exact correction reported343
passes,1,029 shuffled race passes and165,339 sequence-fuzz executions. Those are
inherited results, not new publication-time runs. The follow-up CI is the source
of fresh acceptance and must be checked for its exact head.

For the same64-record16KiB in-memory block, the corrected selector retained about
16,436 bytes at100% ACK,525,954 at50%, and1,051,907 at0%; full decoding used about
1,051,907 bytes at each density. The boundary materialization is intentional. The
initial selector's zero-byte100%-ACK result does not apply to the corrected path.
Fully pending input still pays validation/lookahead CPU. These are decoder
measurements, not application or storage throughput guarantees.

Prior application performance evidence used an explicit experimental local
replacement of this correction. It is not evidence of a published-module run.
PR18 must pin the newly published corrected revision, without local replacements,
and execute fresh consumer/recovery checks before adoption. No exactly-once,
physical-power-loss, uniform speedup or sustainable-capacity guarantee is added.

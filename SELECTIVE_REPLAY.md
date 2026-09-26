# Selective acknowledgement replay

`LegacyLoader.Load` can avoid materializing the payload of an already acknowledged
record. It validates a complete envelope in the ordinary reader's **already
buffered bytes** before consuming its exact acknowledgement. No on-disk format,
exported API, durability barrier, recovery-tail policy or cleanup order changes.

The fast path recognizes one `Data` map and one integer `ID` in either order,
including existing Data-before-ID records. It validates primitive values, arrays
and string-keyed maps. It never infers completion from the largest ACK. Duplicate
record IDs keep the old consume-once ACK behavior. Pending payloads use the
original generated decoder and therefore retain independent, owned values.

Unknown/duplicate/missing envelope fields, extensions, deeper structures, records
larger than 128 KiB and records crossing the current buffer boundary use the
original decoder. This is an optimization bound, not an acceptance limit. In
particular extension validators and malformed non-string map keys cannot be
bypassed by a generic MessagePack Skip. Peek is never used to read ahead past the
buffered record or to discard a stored I/O/checksum error.

Tests cover sparse/reordered ACKs, duplicate records, both field orders, every
prefix of a representative frame, unsupported/malformed shapes, reader chunks,
large records, gzip and public journal reopen/rewrite. The fuzz target checks that
every frame accepted by the fast validator is also accepted with the same ID and
boundary by the generated decoder. Existing corruption, interrupted append,
process crash, cleanup and synchronization tests remain unchanged.

Reproduce (Go 1.27.1):

```sh
go test -count=1 ./...
go test -race -shuffle=on -count=3 ./...
go test -run '^$' -fuzz '^FuzzSelectiveAcceptedFrames$' -fuzztime=8s
go test -run '^$' -bench '^BenchmarkSelectiveAcknowledgedReplay$' -benchmem -count=3
```

The benchmark compares the same 64-record, 16 KiB payload block at 0%, 50% and
100% acknowledged. Local native-module trials observed approximately 1,051,908
bytes per block before selection; selective decoding used the same allocation at
0%, 525,954 bytes at 50%, and zero at 100%. The fully pending case can cost extra
validation CPU. These in-memory decoder measurements are not end-to-end or disk
throughput claims. Application adoption and workload-specific measurements belong
to Laisky/go-fluentd PR18. No global-optimality or physical-power-loss guarantee.

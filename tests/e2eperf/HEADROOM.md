# Framing-slack hypothesis after exact-head staging replication

Incremental baseline: `1db7ba2d05c77781388e2117a321951bac2dbf26`.
This is a separate follow-up, not a replacement of unfavorable observations.
No previously timed trial is retried or removed.

The adopted staging replication passed correctness and retained its allocation
reductions. However, the direct 16-record 4 MiB comparison against the first
contiguous prototype flagged replay-transfer time 139.134 to 168.945 ms, paired
ratio 1.21426, exploratory 95% interval [1.07638, 1.36465]. Its allocation savings
were still 25.01% for seed and 19.75% for transfer. The original-baseline eight-
record overflow case also had an unfavorable full-lifecycle median 564.775 to
583.319 ms (inconclusive). These data remain in artifact 10948674183, run
36370917100; they are not dismissed as noise because timing qualification failed.

The exact cause is not established. One testable mechanism is that a 4 MiB
payload plus MessagePack framing crosses the fixed prefix capacity. It then
needs two live writes rather than one. The new isolated candidate adds a bounded
4 KiB framing allowance, rather than assuming payload size equals encoded size.
This costs a little more constructor/retained memory; it is not a free reduction
or a guarantee that arbitrary metadata fits. Larger records still overflow.

The helper and its boundary tests are changed only in a detached candidate at
first. The tests move WITH the new capacity, so they still cover true overflow,
short/failed writes, rejection before live append, caller memory ownership and
sticky errors. A new assertion proves the framed 4 MiB record takes one append.
All E2E payloads remain frozen and identical across versions. The existing
E2E workload cap remains 4 MiB of payload; larger true-overflow behavior has
boundary/failure tests, not sustained throughput coverage.

Four focused cases include small/concurrent controls and plain/gzip 4 MiB
payloads. Five alternating pairs are bracketed by identical-binary controls.
The existing timing qualification policy is unchanged. Separate fixed-work
execution traces provide syscall evidence; they cannot supply acceptance timing.
No Sync, ACK, compression, wire format or decoder change is proposed.

The final PR body records the decision and exact-head replication. This change
must not be adopted just because it removes a second write: matched resource
and time observations, including unfavorable ones, decide acceptance.

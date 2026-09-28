# Defer unnecessary TTL clock reads

Incremental production baseline: `7a754e0786bf1de591f3c7cdee69ea5a8db04385`.
The rejected ACK-only buffer experiment is separate; no memory-layout, allocation
budget, Sync or GC-environment change is part of this hypothesis.

Current-generation ACK membership is intentionally non-consumptive and does
not compare deadlines until a generation rotates. Yet CheckAndRemove acquires
a timestamp before it knows whether that comparison is needed. The candidate
moves the clock read inside the old-generation branch, after a current miss.
It preserves the generation read lock and reads time before old-map lookup.
Current hits and misses with no old generation avoid the clock entirely.
Old-generation expiration, refresh, cardinality, rotation and shutdown remain.

Differential contracts retain current membership even with a historical deadline,
old-generation expiration at the exact virtual-time boundary, shadowing, signed
ID extremes and concurrent expiration. A real expiry-bypass mutant must fail
its intended assertion. Full/race/TTL tests run in native Go 1.27.1.

Two public benchmark suites isolate current/missing/parallel membership and
actual recovery-loader scans over fsynced data/ACK files with exact pending
payload checks. Fixture creation and final cleanup are excluded. Each replay
operation resets and exhausts a snapshot; this is not new-delivery throughput.
Five alternating pairs and identical-binary controls accompany each suite.
Separate CPU profiles attribute clock and lookup work; profiled runs never
enter timing acceptance. Four complete durable lifecycle workloads provide
dense/sparse/gzip/pending guardrails, bracketed by unchanged A/A qualification.

The first candidate remains isolated until the complete artifact is read.
An accepted default requires publishing the tested diff and a fresh exact-head
replication, with an empty candidate.patch. No latency result is claimed here.

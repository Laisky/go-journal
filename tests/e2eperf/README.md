# Public-API journal performance acceptance

This work is based on corrected PR9 (`fc156a60fafc21dd36ad534e5b8f7a971beccec8`), not the uncorrected PR8 merge. The performance PR is stacked on PR9 so its implementation is not duplicated. Do not merge a default branch automatically.

## Measurement contract

Run an ordinary Go executable using only exported Journal APIs against fresh private storage and an independent local downstream. Measure concurrent append and successful Sync, downstream ledger persistence before WriteId/Sync, sparse-ACK recovery, maximum-ID scanning, safe replay transfer and cleanup separately. Kill the worker after synchronized checkpoints and verify recovery in new processes. A third open must preserve the ID frontier and have no pending deliveries.

Never treat queue admission, Flush, Close, or a plain HTTP success as equivalent to durable completion. Every accepted ID and payload must be reconciled with independently generated expected values. Identical retries are counted, not silently removed or called exactly-once.

## Iteration rules

Freeze the same workload, driver and dependency graph for baseline/candidate. Alternate AB/BA pairs. Keep all samples, failed runs and resource regressions. Separate CPU/heap profiles from unprofiled timings; report whole-process CPU, allocation churn, RSS and phase latency independently. Do not change synchronization cadence between versions or count a configuration change as a code optimization.

Storage and CPU qualification are part of each result. A passing correctness run is not sustained-capacity certification. File/directory synchronization remains mandatory even on a filesystem that does not provide production power-loss guarantees. Existing crash, corruption and sequence-state regression tests remain in force.

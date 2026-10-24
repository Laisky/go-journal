# 20261024 — Journal v2 storage-guard and retained-recovery qualification

Review target: [go-journal PR #16](https://github.com/Laisky/go-journal/pull/16).
This is source qualification, **not** permission to merge into v1 master, tag
v2, or deploy. All published v1 tags and stored journal formats are preserved.

## Problems — behavior-first baseline

The regression suite `TestStorageGuardRegressionContracts` was added before
production changes. On the preceding source implementation it reproduced:

1. A `WriteData`/ `Rotate` deadlock with a valid exclusive storage guard
   (journal read lock -> guard versus guard -> journal write lock).
2. A panic when a guard succeeded with a nil release callback.
3. A stale pending-data flag after an explicitly acknowledged, successful cleanup.
4. Dirty ACK snapshot refresh synchronizing without requesting a storage grant.
5. Legacy EOF auto-clean deleting records despite denied cleanup authorization.
6. Flush-worker shutdown bypassing the storage guard.
7. `Close` completing while a running recovery reader still held a source.

The first baseline was intentionally red:
[40048021511](https://github.com/Laisky/go-journal/actions/runs/40048021511).
After correcting a startup-rotation signal in the deadlock test, the independent
deadlock reproduction was red:
[40048104567](https://github.com/Laisky/go-journal/actions/runs/40048104567).

## Changes and acceptance

All guarded journal operations now acquire the journal lock **before** requesting
an external storage grant. Guard callbacks must not reenter journal methods or
invert that lock order. Invalid successful grants fail with an error, not a panic.
`Sync` only charges the owner of a shared barrier. ACK refresh, legacy EOF
cleanup, periodic/deferred flush and close now honor the same funding boundary.

Successful explicitly authorized cleanup clears the retired snapshot obligation;
newer writes remain visible through independent active-data/sequence tracking.
The running recovery reader has a separate lifetime lease so `Close` cannot
invalidate rooted descriptors until callbacks finish. The callback must not
call back into journal I/O or `Close`.

`TestStorageGuardRegressionContracts` remains in the existing fast Go test
allowlist (one additional top-level test; no new permanent CI job).
`TestGuardedJournalDurableReplayAndArchiveEndToEnd` covers both plain and gzip
on-disk persistence: denied admission, accepted records, Sync, retained replay,
partial downstream ACK, stop/restart, raw archive read-only preservation,
recovered unresolved IDs, explicit retirement, and reconciliation.

## Executed checks

- Focused patched fast CI:
  [40048218303](https://github.com/Laisky/go-journal/actions/runs/40048218303).
- Full native package tests, randomized race runs, ten repeated recovery/guard
  tests under `-race`, `go vet`, module verification and all independent
  security-consumer tests:
  [40048651089](https://github.com/Laisky/go-journal/actions/runs/40048651089).
- Source integration against
  [go-fluentd PR #49 at 831775c](https://github.com/Laisky/go-fluentd/pull/49):
  controller, receiver, sender and OTLP journal test packages pass with the
  Go race detector, using a **temporary CI-only local module replace** of the
  unchanged collector checkout. The collector's dependency pin was not modified.
- Normal fast CI at source revision `9eee607` passed:
  [40048658119](https://github.com/Laisky/go-journal/actions/runs/40048658119).

The temporary branch-scoped full-qualification workflow was removed after its
successful run; only the original fast CI gate and regression tests remain.

## Compatibility, risks and release gates

No v1 source branch, published tag, on-disk wire format, historical benchmark
record, or collector PR is rewritten. The intentional `/v2` import/logger API
breaks remain documented in `docs/v2-migration.md`. Unguarded journal callers
retain their original operations; guarded callers receive stronger persistence
authorization. The original `LoadLegacyBuf` auto-retirement behavior remains,
but honors a configured guard; callers requiring source retention must use
`LoadLegacyBufRetained` and explicitly authorize `CleanLegacyBuf`.

HTTP ACK cannot certify sink durability. Process-crash acceptance assumes an
intact persistent filesystem; physical power loss, real-service deployment,
capacity/exhaustion, operational rollback and published image acceptance remain
separate. Do not merge this v2 module onto the v1 master line. On 2026-10-24 the
separate `v2` branch exists at the original v1 baseline `cba67f2` and PR #16
is retargeted there. The post-retarget normal Go check
[40048828965](https://github.com/Laisky/go-journal/actions/runs/40048828965)
and Snyk status both pass on source+documentation head `1272292`.
Independent merge/release approval, an immutable v2 tag, coordinated consumer
pinning, real-service acceptance and production rollout are still required.

## Exact-head security check discrepancy

The original post-retarget documentation head `1272292` had a successful Snyk
status. The next head `61fcb24` changed **only this documentation file**
(7 additions, 3 deletions): its Go workflow still passed, but the separate
Snyk commit status reported `error: 1 test has failed`. Its detailed report is
private and was not available to this review. Neither result establishes a
change in journal runtime, go.mod, nor the dependency-security fixtures.
This discrepancy is a reason to recheck the security gate, **not** to waive it
or claim that all advisories are false positives. No source merge or release is
authorized while required checks remain failed or unexplained.

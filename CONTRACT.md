# User-visible journal acceptance contracts

The test oracle is the caller's submitted records and acknowledged IDs, not the
implementation's cache, private file lists, counters, or coverage percentage.

## Guarantees under test

1. A record followed by successful `Sync()` remains recoverable after abrupt
   process termination and restart using the same directory. A successful
   synchronized acknowledgement suppresses that record during recovery.
2. Records and IDs retain their exact values across segment rotation, restarts,
   and changes between plain and gzip writing. Readers discover historical
   compression from each file, not the current writer setting.
3. Recovery does not overwrite existing segments or treat unrelated files as
   journal data. Clock rollback must not cause an existing segment to be reused.
4. Missing, malformed, or truncated acknowledgement data is an error, not a
   successful high-water scan or permission to clean the recovery snapshot.
   Failure must remain visible on retry until the underlying problem is repaired.
5. A failed rotation must not turn an otherwise usable writer into a panic or
   silently discard already accepted records. Concurrent public operations must
   not race with flush, rotation, or close.
6. Invalid lifecycle/input/configuration calls return an error rather than
   panicking. Close is idempotent. Operations after close cannot reopen files or
   claim a successful durability barrier.
7. Replay callers retain ownership of each returned pending record: before
   advancing to EOF they must write a replacement with `WriteData` (or durably
   acknowledge it). EOF cleanup synchronizes replacement writes first. Read-only
   replay is not a supported destructive-consumption pattern.

## Test discipline

External-package behavior tests use exported APIs and real temporary files.
Process tests use a separately executed caller with an independent input/output
manifest, explicit successful Sync receipts, actual SIGKILL and fresh reopen.
No graceful-close hook, private replay entry point, synthetic old-file list, or
extra rotation is used to make crash recovery pass. Fault tests preserve the
original evidence and distinguish application defects from invalid fixtures.

Tests assert values and sets, not just counts. Ambiguous failed/unanswered writes
may appear on recovery; they are not assumed rolled back. At-least-once delivery
is not exactly-once. Tests may not turn off errors, skip failed cases, or weaken
Sync to obtain a pass. Baseline failures and positive controls will be retained.

## Boundaries

SIGKILL does not discard the operating system's page cache. These tests do not
qualify physical power loss, untrusted filesystem mutation, unsupported remote
filesystems, unlimited backlog, or all possible interleavings. Durability depends
on storage honoring synchronization. The caller must not assign an ID already
owned by another record and must coordinate directory ownership between writers.

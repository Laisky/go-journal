# Recovery boundaries

> Policy amendment (2026-10-08): [CI testing policy](docs/ci-testing.md)
> moves full/race, integration and performance qualification to manual dev/staging
> or workflow dispatch. Historical automatic-gate descriptions below are superseded;
> test assertions and thresholds remain intact. Repository protection settings are unchanged.

`LoadMaxId()` includes sealed data records and acknowledgement records. Pending
records must reserve their IDs across a restart; ACK-only recovery can reuse an
ID that still names another retained record.

`PrepareNewBufFile` supplies only sealed predecessor files. All of those data
files belong to the next replay pass; the new active writer is not in the list.
The loader must not subtract an additional file. Before requesting the next
record/EOF, a replay caller must complete any replacement writes. The journal's
successful file/directory sync barrier precedes old-file cleanup.

An empty unused segment is valid, including an empty gzip file created before a
crash. A partially written final record in the newest nonempty segment is handled
conservatively: complete preceding records can recover, while the entire original
file is retained under a `.incomplete` hard link. The evidence name is not a
replayable WAL filename, is not overwritten, and is not automatically removed.
Failure to retain/sync evidence fails recovery. Operators must inspect and archive
that evidence and investigate the write failure.

This exception is restricted to decoder EOF/incomplete-record errors at the
newest segment's tail. Invalid MessagePack types, gzip checksum errors, and
corruption in older nonempty segments still fail closed. It is not arbitrary
corruption repair, a checksum for the uncompressed format, or a physical-power-loss
qualification. Successful Sync relies on the filesystem/storage honoring it.

`recovery_delivery_test.go` exercises the public APIs: reopen immediately without
an extra rotation; reserve unacknowledged IDs; suppress already acknowledged
records; exclude the active writer; recover complete prefixes in plain/gzip
formats; retain byte-identical interrupted-append evidence; reject arbitrary
corruption. The normal CI repeats these tests with the race detector.

A refreshed replay snapshot closes and invalidates a partially consumed cursor.
After yielding the replay lease across a rotation, identical records may repeat;
unread newly sealed records must never be reclaimed. A failed cleanup returns an
error, retains retry state, and is not successful EOF. ACK decode errors also
fail replay/recovery rather than being treated as an empty acknowledgement set.

Cleanup retains the newest ACK segment and the segment containing the highest
acknowledged ID. Completion can be out of order; keeping only the newest file
could otherwise roll the recovered identity frontier backwards. At most two old
ACK segments are retained by this rule. See BEHAVIOR.md for tests and validation
limits, including the native-module-graph check still required before merge.


## Retained originals and closed archive reading

LoadLegacyBufRetained reaches a frozen snapshot EOF without deleting its data
files. The caller owns replay progress and may use PrepareRetainedReplay after
EOF; empty retries reuse the writer. CleanLegacyBuf is an explicit synchronized
retirement operation, requiring legacy ownership and a caller-proven complete
resolution. Transport ACK is not proof of downstream stable storage.

WithStorageGuard funds persistent mutations before writing and holds the
caller's quiescence barrier until release. It receives sizes and mutation kinds,
never content. Serialization callbacks run exactly once before the grant. A
guarded journal disables speculative file preallocation; the owner must fund
startup, ACK/flush/sync/rotation and retain future progress headroom. Journal
locks always precede storage grants, including shutdown and legacy EOF cleanup;
the guard must not call back into Journal or invert this order. Nil successful
releases are errors. Successful explicit snapshot cleanup clears the old
obligation, while later writes remain pending independently. Running recovery
readers are pinned until their callbacks finish before Close releases root FDs.

ReadRecoveryRecords returns originals including acknowledged records from a
running owner's synchronized prefixes. For operator recovery of a stopped
journal, OpenClosedRecoveryArchive duplicates an os.Root and acquires the
existing ownership lock without creating files. WalkRaw validates and returns
each original MessagePack object and decoded Data identity, with a finite per
record size limit. Repeated walks use the same captured prefixes; source file
identity/size changes fail. All reads stay rooted after directory rename.
Close releases ownership and never removes records or ACKs. A caller can fully
preflight a snapshot before transport; it must preserve the source on any
transport outcome and account for ambiguous-ACK duplicates.

Only a consistent, exclusively owned journal directory is a valid recovery
source. Local process-crash tests assume persistent storage remains intact;
these APIs do not establish power-loss or host/disk-destruction protection.

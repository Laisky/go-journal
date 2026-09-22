# Recovery boundaries

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

# Retained directory ownership

`Journal.Start` opens the configured directory once and keeps it until `Close`.
Rotation, directory barriers, acknowledgement scans, replay, incomplete-append
hard links and cleanup all use that retained directory capability. Renaming or replacing the original
path cannot redirect a running owner. The on-disk filenames, encodings, replay
ordering, acknowledgement frontier and synchronization barriers are unchanged.

An embedding service may call `WithRoot(root)` instead of `WithBufDirPath(path)`.
The constructor duplicates this borrowed root after validating every option.
The caller can then close its own handle; it must still call `Journal.Close`,
even after a failed `Start`. Root-relative operations do not use `/proc/self/fd`
or turn `Root.Name()` back into a path-based filesystem capability. A path-based
configuration is anchored at `Start`; configuration-time directory creation and
writability checking retain their historical behavior. Standalone exported file
helpers and `NewLegacyLoader` keep their existing path-based contracts and are
not an untrusted-path sandbox.

Linux ownership acquires both flock and OFD locks, because historical etcd
versions can select either independent lock namespace. Darwin and supported BSD
systems use flock as before. Conflicts and unsupported locking fail closed;
Linux kernels without OFD locks retain flock. Network/mounted filesystems need
qualification for their lock and fsync semantics; local Linux/Darwin tests are
not a certification of NFS or physical power-loss behavior. The permanent lock
inode is not unlinked. New data/acknowledgement files request 0600 and directories
created by `PrepareDir` request 0700, without changing the process umask. This
does not chmod existing directories, files or ACLs, and adds no encryption.

The root must remain controlled by trusted operators. `os.Root` constrains
symlinks but does not isolate hard links, bind mounts or authorized modifications
to retained journal contents. Embedders must validate routing identities and
check the mode/ownership/ACL policy of reused roots before admitting records.
Existing shared trees require an explicit offline migration or sharing policy;
this library does not rename, delete or silently tighten historical data.

## Regression evidence

The rotation regression still fails on a7f7377c4a3dfe3e76244412c082e1e76e121b44,
after the private-creation fix in PR #12: ordinary and gzip rotation create files
in a replacement symlink target. Retained roots keep those files in the original
directory. PR #12 supplies the private-creation production changes and umask
regressions; this refactor reuses them.

Additional tests cover borrowed-root lifetime and replacement before Start,
old/new lock exclusion in both directions (flock and OFD on Linux), plain/gzip
replay and ACKs across rename/reopen, byte-identical incomplete evidence and
corruption refusal after replacement, closed-root cleanup, concurrent barrier
error propagation and retry. Interrupted-tail replay is checked with clean path
labels, labels ending in /., and borrowed roots. Replay compares names from the
scan snapshot because a duplicated Root preserves /. in opened File.Name labels.

Directory rename no longer simulates I/O failure. The former rename-failure
fixtures now check successful retained-directory barriers, a real unreadable
scan entry, and an injected typed directory-open failure. No crash, loss,
identity, corruption or failed-barrier assertion is removed. Test failure logs
must be retained; failure is not retried away as acceptance.


## Confined Linux opens and performance

On Linux, the owner also retains one directory descriptor obtained through
`Root.Open(".")`. A journal filename must validate as exactly one direct child
(or "." for directory barriers). Such opens use `openat` with `O_NOFOLLOW` and
`O_CLOEXEC`. There are no earlier path components to follow; the final symlink
cannot redirect this open. An `ELOOP` result falls back to `os.Root`, preserving
allowed relative symlinks and rejecting escaping or absolute symlinks. This
relies on the documented [O_NOFOLLOW semantics](https://man7.org/linux/man-pages/man2/open.2.html).
Unusual open flags, unsupported modes, and other operating systems use `os.Root`
directly. Directory enumeration keeps `Root.Open` so returned entries retain its
confined metadata semantics. Stat, hard links and removals also remain Root operations.

A read lock pins the extra descriptor throughout each open; `Close` takes the
write lock and closes it before the owned Root. There is no segment descriptor,
metadata, frontier, result or seek-position cache. Every scan reopens, stats,
decodes and closes every selected segment. The newest-nonempty lookup is delayed
until an interrupted record actually needs that recovery decision.

The frozen pinned and rolling public-API gates and their thresholds are unchanged.
`BenchmarkJournalFilesystemOpen` compares the same open/stat/close operation
through paths, direct Root, the portable adapter and the owner implementation.
On Linux the owner removes the additional per-open allocations. Native tests
exercise both the optimized and pure-Root paths; a Linux negative control must
fail confinement assertions when `O_NOFOLLOW` is removed.

See [measurement results and limitations](ROOTED_PERF.md). Passing benchmark and
correctness checks is required before readiness; this remains an architectural
refactor requiring review before merge.

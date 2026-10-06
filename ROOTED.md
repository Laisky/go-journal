# Retained directory ownership

`Journal.Start` opens the configured directory once and keeps it until `Close`.
Rotation, directory barriers, acknowledgement scans, replay, incomplete-append
hard links and cleanup all use that `os.Root`. Renaming or replacing the original
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

Two bounded public regressions fail on ad049272dbea: ordinary and gzip rotation
create two files in the replacement symlink target, and new segments have 0644
under umask 0022. The same tests pass with retained roots/private creation.
Additional tests cover borrowed-root lifetime and replacement before Start,
old/new lock exclusion in both directions (flock and OFD on Linux), plain/gzip
replay and ACKs across rename/reopen, byte-identical incomplete evidence and
corruption refusal after replacement, neutral/restrictive umask subprocesses,
closed-root cleanup, concurrent barrier error propagation and retry.

Directory rename no longer simulates I/O failure. The former rename-failure
fixtures now check successful retained-directory barriers, a real unreadable
scan entry, and an injected typed directory-open failure. No crash, loss,
identity, corruption or failed-barrier assertion is removed. Test failure logs
must be retained; failure is not retried away as acceptance.

//go:build linux

package journal_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestJournalRootLockExcludesBothHistoricalLinuxLockKinds(t *testing.T) {
	for _, kind := range []string{"flock", "ofd"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, ".journal.lock")
			lock := func(fp *os.File) error {
				if kind == "flock" {
					return syscall.Flock(int(fp.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
				}
				lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: 0, Len: 0}
				return syscall.FcntlFlock(fp.Fd(), 37, &lk)
			}
			old, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
			behaviorCheck(t, err)
			defer old.Close()
			// A real independent old-style file description, not a mocked error.
			behaviorCheck(t, lock(old))
			j := behaviorNew(t, dir, false)
			if err := j.Start(t.Context()); err == nil {
				t.Fatal("historical owner bypassed")
			}
			behaviorCheck(t, old.Close())
			behaviorCheck(t, j.Start(t.Context()))
			other, err := os.OpenFile(path, os.O_RDWR, 0)
			behaviorCheck(t, err)
			defer other.Close()
			if err := lock(other); err != syscall.EWOULDBLOCK {
				t.Fatalf("new owner bypassed or wrong failure: %v", err)
			}
			j.Close()
			behaviorCheck(t, lock(other))
		})
	}
}

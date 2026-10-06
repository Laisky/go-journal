package journal_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/coreos/etcd/pkg/fileutil"
)

// A bounded, baseline-compatible public-API reproduction. The original owner
// remains open while its pathname is replaced; no privileged mount is needed.
func TestRegressionJournalRotationStaysWithOpenedDirectory(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			base := t.TempDir()
			active := filepath.Join(base, "active")
			retained := filepath.Join(base, "retained")
			outside := filepath.Join(base, "outside")
			behaviorCheck(t, os.Mkdir(outside, 0700))
			j := behaviorStart(t, active, gz)
			behaviorCheck(t, j.WriteData(behaviorData(41)))
			behaviorCheck(t, j.WriteData(behaviorData(42)))
			behaviorCheck(t, j.WriteId(42))
			behaviorCheck(t, j.Sync())
			behaviorCheck(t, os.Rename(active, retained))
			behaviorCheck(t, os.Symlink(outside, active))
			behaviorCheck(t, j.Rotate(context.Background()))
			entries, err := os.ReadDir(outside)
			behaviorCheck(t, err)
			if len(entries) != 0 {
				t.Fatalf("JOURNAL_ROOT_REGRESSION: rotation redirected %d entries outside owned directory", len(entries))
			}
			high, err := j.LoadMaxId()
			behaviorCheck(t, err)
			if high != 42 {
				t.Fatalf("identity frontier %d", high)
			}
			got := behaviorReplay(t, j)
			if len(got) != 1 || got[41] == nil {
				t.Fatalf("ACK/replay changed after rename: %v", got)
			}
			behaviorCheck(t, j.Sync())
			j.Close()
			again := behaviorStart(t, retained, gz)
			got = behaviorReplay(t, again)
			if len(got) != 1 || got[41] == nil {
				t.Fatalf("restart lost admitted data: %v", got)
			}
		})
	}
}

func TestRegressionJournalPrivateSegmentModes(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := behaviorStart(t, dir, gz)
			behaviorCheck(t, j.WriteData(behaviorData(1)))
			behaviorCheck(t, j.WriteId(1))
			behaviorCheck(t, j.Sync())
			behaviorCheck(t, j.Rotate(context.Background()))
			j.Close()
			again := behaviorStart(t, dir, gz)
			again.Close()
			entries, err := os.ReadDir(dir)
			behaviorCheck(t, err)
			if len(entries) < 3 {
				t.Fatal("vacuous segment fixture")
			}
			for _, e := range entries {
				i, err := e.Info()
				behaviorCheck(t, err)
				if i.Mode().Perm()&0077 != 0 {
					t.Errorf("JOURNAL_MODE_REGRESSION: %s mode=%#o", e.Name(), i.Mode().Perm())
				}
			}
		})
	}
}

func TestJournalRootLockInteroperatesWithExistingBackend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".journal.lock")
	old, err := fileutil.TryLockFile(path, os.O_CREATE|os.O_RDWR, 0600)
	behaviorCheck(t, err)
	j := behaviorNew(t, dir, false)
	if err := j.Start(context.Background()); err == nil {
		j.Close()
		old.Close()
		t.Fatal("old lock did not exclude new owner")
	}
	behaviorCheck(t, old.Close())
	behaviorCheck(t, j.Start(context.Background()))
	old, err = fileutil.TryLockFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err == nil {
		old.Close()
		t.Fatal("new owner did not exclude old backend")
	}
	j.Close()
	old, err = fileutil.TryLockFile(path, os.O_CREATE|os.O_RDWR, 0600)
	behaviorCheck(t, err)
	behaviorCheck(t, old.Close())
}

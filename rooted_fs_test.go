package journal

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// A removed pathname is not a missing directory capability. Inject the actual
// directory-open failure here; retain the concurrent error and retry contract.
type failingDirectoryFS struct {
	journalFS
	fail  atomic.Bool
	cause error
	name  string
}

func (d *failingDirectoryFS) Open(name string) (*os.File, error) {
	if d.fail.Load() && filepath.Clean(name) == filepath.Clean(d.name) {
		return nil, d.cause
	}
	return d.journalFS.Open(name)
}
func TestSyncGroupDirectoryErrorIsNotCachedSuccess(t *testing.T) {
	j, err := NewJournal(WithBufDirPath(t.TempDir()), WithIsAggresiveGC(false), WithRotateCheckInterval(time.Hour), WithFlushInterval(time.Hour), WithBufSizeByte(4096))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := j.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"x": "one"}}); err != nil {
		t.Fatal(err)
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	disk := &failingDirectoryFS{journalFS: j.disk, cause: fs.ErrPermission, name: j.bufDirPath}
	j.Lock()
	j.disk = disk
	j.Unlock()
	disk.fail.Store(true)
	start, result := make(chan struct{}), make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func() { <-start; result <- j.Sync() }()
	}
	close(start)
	for i := 0; i < 32; i++ {
		select {
		case err := <-result:
			if !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("lost directory failure: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failed barrier did not release callers")
		}
	}
	disk.fail.Store(false)
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := j.Rotate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteData(&Data{ID: 2, Data: map[string]interface{}{"x": "two"}}); err != nil {
		t.Fatal(err)
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
}

func TestRootedFSRejectsOutsidePathsAndReleasesRoot(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "owned")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	disk := rootedFS{root: root}
	for _, name := range []string{parent, filepath.Join(parent, "foreign"), filepath.Join(dir, "child", "nested")} {
		if _, err := disk.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600); err == nil {
			t.Errorf("accepted path %q", name)
		}
	}
	if err := disk.Remove(dir); err == nil {
		t.Fatal("allowed directory deletion")
	}
	if err := disk.Link(filepath.Join(dir, "file"), filepath.Join(parent, "outside")); err == nil {
		t.Fatal("allowed outside link")
	}
	if err := os.Symlink(filepath.Join(parent, "foreign"), filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.OpenFile(filepath.Join(dir, "escape"), os.O_CREATE|os.O_RDWR, 0600); err == nil {
		t.Fatal("created through outside symlink")
	}
	if _, err := os.Stat(filepath.Join(parent, "foreign")); !os.IsNotExist(err) {
		t.Fatalf("outside file created: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	j, err := NewJournal(WithRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	owned := j.ownedRoot
	j.Close()
	if _, err := owned.Stat("."); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("root leaked after close: %v", err)
	}
	if _, err := root.Stat("."); err != nil {
		t.Fatalf("closed caller-owned root: %v", err)
	}
}

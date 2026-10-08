//go:build linux

package journal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRootedLinuxOpenLifetimeAndFlagFallback(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "record")
	if err := os.WriteFile(name, []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	disk, directory := newRootedFS(root)
	if directory == nil {
		t.Fatal("Linux direct-child backend was not selected")
	}
	defer directory.Close()
	// os.NewFile does not retain appendMode, so append requests must use Root.
	fp, err := disk.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fp.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := fp.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := fp.WriteAt([]byte("bad"), 0); err == nil {
		t.Fatal("append WriteAt unexpectedly accepted")
	}
	if err := fp.Close(); err != nil {
		t.Fatal(err)
	}
	if wire, err := os.ReadFile(name); err != nil || string(wire) != "ab" {
		t.Fatalf("append = %q, %v", wire, err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink("record", link); err != nil {
		t.Fatal(err)
	}
	if fp, err := disk.OpenFile(link, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600); !errors.Is(err, os.ErrExist) {
		if fp != nil {
			fp.Close()
		}
		t.Fatalf("exclusive create through symlink: %v", err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("owned directory descriptor remains open: %v", err)
	}
	if fp, err := disk.Open(name); !errors.Is(err, os.ErrClosed) {
		if fp != nil {
			fp.Close()
		}
		t.Fatalf("closed backend still opens: %v", err)
	}
	// Closing the extra capability does not close the caller's root.
	fp, err = root.Open("record")
	if err != nil {
		t.Fatal(err)
	}
	fp.Close()
}

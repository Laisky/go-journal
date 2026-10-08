//go:build linux || darwin

package journal

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Frontier calls must observe fresh file identities, contents and read
// permissions. Holding a sealed descriptor across calls is not sufficient.
func TestRootFrontierObservesReplacedRemovedAndUnreadableSegments(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			dir := t.TempDir()
			j, err := NewJournal(WithBufDirPath(dir), WithIsCompress(compressed), WithIsAggresiveGC(false), WithBufSizeByte(4096), WithRotateCheckInterval(time.Hour), WithFlushInterval(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if err := j.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := j.WriteData(&Data{ID: 41, Data: map[string]interface{}{"x": "original"}}); err != nil {
				t.Fatal(err)
			}
			if err := j.WriteId(52); err != nil {
				t.Fatal(err)
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := j.Rotate(t.Context()); err != nil {
				t.Fatal(err)
			}
			check := func(want int64, wantError bool) {
				t.Helper()
				got, err := j.LoadMaxId()
				if wantError {
					if err == nil {
						t.Fatalf("missing fresh filesystem failure; frontier=%d", got)
					}
				} else if err != nil || got != want {
					t.Fatalf("frontier=%d error=%v want=%d", got, err, want)
				}
			}
			check(52, false)
			encode := func(wire []byte) []byte {
				if !compressed {
					return wire
				}
				var b bytes.Buffer
				w := gzip.NewWriter(&b)
				if _, err := w.Write(wire); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				return b.Bytes()
			}
			replace := func(name string, wire []byte) {
				t.Helper()
				tmp := filepath.Join(dir, "replacement")
				if err := os.WriteFile(tmp, encode(wire), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(tmp, name); err != nil {
					t.Fatal(err)
				}
			}
			data, ack := j.fsStat.OldDataFnames[0], j.fsStat.OldIDsDataFnames[0]
			replace(data, maxIDWire(73, "replacement"))
			check(73, false)
			word := make([]byte, 8)
			binary.BigEndian.PutUint64(word, 97)
			replace(ack, word)
			check(97, false)
			if os.Geteuid() != 0 {
				if err := os.Chmod(data, 0000); err != nil {
					t.Fatal(err)
				}
				check(0, true)
				if err := os.Chmod(data, 0600); err != nil {
					t.Fatal(err)
				}
				check(97, false)
			}
			if err := os.Remove(data); err != nil {
				t.Fatal(err)
			}
			check(0, true)
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, encode(maxIDWire(999, "outside")), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, data); err != nil {
				t.Fatal(err)
			}
			check(0, true)
			if err := os.Remove(data); err != nil {
				t.Fatal(err)
			}
			replace(data, maxIDWire(73, "restored"))
			check(97, false)
			// Independent callers must not share a descriptor's seek position.
			var wg sync.WaitGroup
			errs := make(chan error, 32)
			for range 32 {
				wg.Go(func() {
					for range 8 {
						got, err := j.LoadMaxId()
						if err != nil || got != 97 {
							errs <- fmt.Errorf("concurrent frontier=%d: %w", got, err)
							return
						}
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
		})
	}
}

func TestRootedOpenConfinesSymlinkSwapsAndConcurrentClose(t *testing.T) {
	for _, standard := range []bool{false, true} {
		t.Run(fmt.Sprintf("standard=%v", standard), func(t *testing.T) {
			testRootedOpenConfinesSymlinkSwapsAndConcurrentClose(t, standard)
		})
	}
}

func testRootedOpenConfinesSymlinkSwapsAndConcurrentClose(t *testing.T, standard bool) {
	base := t.TempDir()
	dir := filepath.Join(base, "owned")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "inside"), []byte("inside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "outside"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	j, err := NewJournal(WithRoot(root))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if standard {
		if j.ownedDirectory != nil {
			j.ownedDirectory.Close()
			j.ownedDirectory = nil
		}
		j.disk = rootedFS{root: j.ownedRoot}
	}
	name := filepath.Join(dir, "link")
	for _, target := range []string{"inside", "../outside", filepath.Join(dir, "inside")} {
		if err := os.Symlink(target, name); err != nil {
			t.Fatal(err)
		}
		fp, err := j.disk.Open(name)
		if target == "inside" {
			if err != nil {
				t.Fatal(err)
			}
			wire, err := io.ReadAll(fp)
			fp.Close()
			if err != nil || string(wire) != "inside" {
				t.Fatalf("internal symlink: %q %v", wire, err)
			}
		} else if err == nil {
			fp.Close()
			t.Fatal("accepted escaping or absolute symlink")
		}
		if err := os.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("inside", name); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 17)
	wg.Go(func() {
		for i := 0; i < 512; i++ {
			target := "inside"
			if i%2 != 0 {
				target = "../outside"
			}
			next := filepath.Join(dir, "next")
			if err := os.Symlink(target, next); err != nil {
				failures <- err
				return
			}
			if err := os.Rename(next, name); err != nil {
				failures <- err
				return
			}
		}
	})
	for range 16 {
		wg.Go(func() {
			for range 256 {
				fp, err := j.disk.Open(name)
				if err != nil {
					continue
				}
				wire, err := io.ReadAll(fp)
				closeErr := fp.Close()
				if err != nil || closeErr != nil || string(wire) != "inside" {
					failures <- fmt.Errorf("escaped or invalid open: %q read=%v close=%v", wire, err, closeErr)
					return
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	// Open and Close racing must never resolve a recycled directory descriptor.
	errorsCh := make(chan error, 16)
	var closers sync.WaitGroup
	for range 16 {
		closers.Go(func() {
			for range 256 {
				fp, err := j.disk.Open(filepath.Join(dir, "inside"))
				if err != nil {
					continue
				}
				wire, err := io.ReadAll(fp)
				fp.Close()
				if err != nil || string(wire) != "inside" {
					errorsCh <- fmt.Errorf("close race: %q %v", wire, err)
					return
				}
			}
		})
	}
	j.Close()
	closers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if fp, err := j.disk.Open(filepath.Join(dir, "inside")); !errors.Is(err, os.ErrClosed) {
		if fp != nil {
			fp.Close()
		}
		t.Fatalf("closed owner still opens files: %v", err)
	}
}

func TestRootedDirectoryEntriesKeepMetadataAfterReplacement(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "active")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "record"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	disk, directory := newRootedFS(root)
	if directory != nil {
		defer directory.Close()
	}
	entries, err := disk.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	if err := os.Rename(dir, filepath.Join(parent, "retained")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "record"), []byte("foreign contents of a different length"), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := entries[0].Info()
	if err != nil || info.Size() != 8 {
		t.Fatalf("directory metadata redirected: %v %v", info, err)
	}
}

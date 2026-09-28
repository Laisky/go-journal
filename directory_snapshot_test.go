package journal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestDirectorySnapshotOrderAndEntryPolicy(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"20990101_00000009.ids", "20990101_00000002.buf", "20990101_00000001.ids", "unknown"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("20990101_00000002.buf", filepath.Join(dir, "20990101_00000001.buf")); err != nil {
		t.Fatal(err)
	}
	// Preserve existing classification, rather than silently adding a regular-
	// file-only policy as part of a metadata optimization.
	if err := os.Mkdir(filepath.Join(dir, "20990101_00000003.buf"), 0700); err != nil {
		t.Fatal(err)
	}
	s, err := PrepareNewBufFile(dir, nil, true, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.NewDataFp.Close()
	defer s.NewIDsFp.Close()
	wantData := []string{filepath.Join(dir, "20990101_00000001.buf"), filepath.Join(dir, "20990101_00000002.buf"), filepath.Join(dir, "20990101_00000003.buf")}
	wantIDs := []string{filepath.Join(dir, "20990101_00000001.ids"), filepath.Join(dir, "20990101_00000009.ids")}
	if !reflect.DeepEqual(s.OldDataFnames, wantData) || !reflect.DeepEqual(s.OldIDsDataFnames, wantIDs) {
		t.Fatalf("snapshot order: %+v", s)
	}
	if filepath.Base(s.NewDataFp.Name()) != "20990101_00000004.buf" || filepath.Base(s.NewIDsFp.Name()) != "20990101_00000010.ids" {
		t.Fatalf("frontier: %+v", s)
	}
}

func TestDirectorySnapshotStillRejectsDanglingEntriesBeforeCreatingFiles(t *testing.T) {
	for _, name := range []string{"unrecognized-link", "20990101_00000001.ids"} {
		dir := t.TempDir()
		if err := os.Symlink("missing", filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareNewBufFile(dir, nil, true, false, 0); err == nil {
			t.Fatal("dangling directory entry accepted")
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 1 || entries[0].Name() != name {
			t.Fatal("failed snapshot changed directory", err)
		}
	}
}

// Exercise creation, explicit durability, rotation, close/reopen, exact sparse
// pending replay, and an old maximum. It uses real public API calls and checks
// output independently of how directory entries are enumerated.
func TestDirectorySnapshotPublicRoundTrip(t *testing.T) {
	for _, gzip := range []bool{false, true} {
		dir := t.TempDir()
		open := func() *Journal {
			j, err := NewJournal(WithBufDirPath(dir), WithIsCompress(gzip), WithIsAggresiveGC(false), WithBufSizeByte(65536), WithFlushInterval(time.Hour), WithRotateCheckInterval(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if err = j.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			return j
		}
		j := open()
		for id := int64(64); id >= 1; id-- {
			if err := j.WriteData(&Data{ID: id, Data: map[string]interface{}{"body": "durable"}}); err != nil {
				t.Fatal(err)
			}
			if id%2 == 0 {
				if err := j.WriteId(id); err != nil {
					t.Fatal(err)
				}
			}
			if id%8 == 0 {
				if err := j.Rotate(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := j.Sync(); err != nil {
			t.Fatal(err)
		}
		j.Close()
		j = open()
		defer j.Close()
		high, err := j.LoadMaxId()
		if err != nil || high != 64 {
			t.Fatal("lost frontier", high, err)
		}
		if !j.LockLegacy() {
			t.Fatal("missing lease")
		}
		seen := map[int64]bool{}
		for {
			var d Data
			err := j.LoadLegacyBuf(&d)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if d.ID%2 != 1 || seen[d.ID] || d.Data["body"] != "durable" {
				t.Fatal("changed replay", d)
			}
			seen[d.ID] = true
			if err := j.WriteData(&d); err != nil {
				t.Fatal(err)
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		j.UnLockLegacy()
		if len(seen) != 32 {
			t.Fatal("missing pending", len(seen))
		}
	}
}

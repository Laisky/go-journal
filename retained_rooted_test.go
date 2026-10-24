package journal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRetainedRecoveryUsesOwnedRootAfterRename(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parent := t.TempDir()
	path := filepath.Join(parent, "owned")
	if e := os.Mkdir(path, 0700); e != nil {
		t.Fatal(e)
	}
	j, e := NewJournal(WithBufDirPath(path), WithBufSizeByte(1<<20), WithFlushInterval(time.Hour), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if e = j.Start(ctx); e != nil {
		t.Fatal(e)
	}
	defer j.Close()
	if e = j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"body": "original"}}); e != nil {
		t.Fatal(e)
	}
	if e = j.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(path, filepath.Join(parent, "retained")); e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(path, 0700); e != nil {
		t.Fatal(e)
	}
	decoy, e := NewJournal(WithBufDirPath(path), WithBufSizeByte(1<<20), WithFlushInterval(time.Hour), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if e = decoy.Start(ctx); e != nil {
		t.Fatal(e)
	}
	defer decoy.Close()
	if e = decoy.WriteData(&Data{ID: 99, Data: map[string]interface{}{"body": "decoy"}}); e != nil {
		t.Fatal(e)
	}
	if e = decoy.Sync(); e != nil {
		t.Fatal(e)
	}
	// A path-based scanner would observe this deliberately valid decoy root.
	paths, e := os.ReadDir(path)
	if e != nil || len(paths) == 0 {
		t.Fatal("decoy control missing")
	}
	n, e := j.ReadRecoveryRecords(ctx, func(d *Data) error {
		if d.ID != 1 || d.Data["body"] != "original" {
			t.Fatal("recovery escaped owned root")
		}
		return nil
	})
	if e != nil || n != 1 {
		t.Fatal("retained root recovery", n, e)
	}
	if _, e = j.PrepareRetainedReplay(ctx); e != nil {
		t.Fatal("rooted snapshot rewind", e)
	}
}

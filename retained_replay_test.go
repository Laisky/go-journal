package journal

import (
	"context"
	"errors"
	"github.com/tinylib/msgp/msgp"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func retainedJournal(t *testing.T) (*Journal, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	j, e := NewJournal(WithBufDirPath(t.TempDir()), WithBufSizeByte(1<<20), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour), WithFlushInterval(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if e = j.Start(ctx); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { cancel(); j.Close() })
	return j, ctx
}
func TestRetainedReplayDoesNotCleanBeforeExplicitAcknowledgedRetirement(t *testing.T) {
	j, ctx := retainedJournal(t)
	if e := j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"synthetic": "retained"}}); e != nil {
		t.Fatal(e)
	}
	if e := j.Sync(); e != nil {
		t.Fatal(e)
	}
	rotated, e := j.PrepareRetainedReplay(ctx)
	if e != nil || !rotated {
		t.Fatal(rotated, e)
	}
	old := append([]string(nil), j.fsStat.OldDataFnames...)
	if !j.LockLegacy() {
		t.Fatal("lock")
	}
	d := new(Data)
	if e = j.LoadLegacyBufRetained(d); e != nil || d.ID != 1 {
		t.Fatal(d, e)
	}
	if e = j.LoadLegacyBufRetained(new(Data)); e != io.EOF {
		t.Fatal(e)
	}
	for _, p := range old {
		if _, e = os.Stat(p); e != nil {
			t.Fatal("unacknowledged source retired", e)
		}
	}
	j.UnLockLegacy()
	for i := 0; i < 10; i++ {
		rotated, e = j.PrepareRetainedReplay(ctx)
		if e != nil || rotated {
			t.Fatal("empty outage generated segment", rotated, e)
		}
		if !j.LockLegacy() {
			t.Fatal("lock")
		}
		if e = j.LoadLegacyBufRetained(new(Data)); e != nil {
			t.Fatal(e)
		}
		if e = j.LoadLegacyBufRetained(new(Data)); e != io.EOF {
			t.Fatal(e)
		}
		j.UnLockLegacy()
	}
	if !j.LockLegacy() {
		t.Fatal("lock")
	}
	if e = j.WriteId(1); e != nil {
		t.Fatal(e)
	}
	if e = j.CleanLegacyBuf(); e != nil {
		t.Fatal(e)
	}
	j.UnLockLegacy()
	for _, p := range old {
		if _, e = os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("ACK-proven segment not reclaimed", e)
		}
	}
}
func TestRetainedCleanupRequiresOwnerAndSuccessfulSync(t *testing.T) {
	j, ctx := retainedJournal(t)
	j.WriteData(&Data{ID: 9, Data: map[string]interface{}{"x": "synthetic"}})
	j.Sync()
	j.PrepareRetainedReplay(ctx)
	if e := j.CleanLegacyBuf(); e == nil {
		t.Fatal("cleanup without owner")
	}
	if !j.LockLegacy() {
		t.Fatal("lock")
	}
	defer j.UnLockLegacy()
	old := append([]string(nil), j.fsStat.OldDataFnames...)
	j.idsFp.Close()
	if e := j.CleanLegacyBuf(); e == nil {
		t.Fatal("failed sync allowed cleanup")
	}
	for _, p := range old {
		if _, e := os.Stat(p); e != nil {
			t.Fatal("source lost after failed sync", e)
		}
	}
}
func TestRetainedPrepareIncludesCurrentACKWithoutNewEmptyData(t *testing.T) {
	j, ctx := retainedJournal(t)
	j.WriteData(&Data{ID: 3, Data: map[string]interface{}{"x": "synthetic"}})
	j.Sync()
	j.PrepareRetainedReplay(ctx)
	if !j.LockLegacy() {
		t.Fatal("lock")
	}
	j.LoadLegacyBufRetained(new(Data))
	j.LoadLegacyBufRetained(new(Data))
	j.WriteId(3)
	j.Sync()
	j.UnLockLegacy()
	before, _ := filepath.Glob(filepath.Join(j.bufDirPath, "*.buf*"))
	for i := 0; i < 5; i++ {
		rotated, e := j.PrepareRetainedReplay(ctx)
		if e != nil || rotated {
			t.Fatal(rotated, e)
		}
		if !j.LockLegacy() {
			t.Fatal("lock")
		}
		if e = j.LoadLegacyBufRetained(new(Data)); e != io.EOF {
			t.Fatal("current ACK not replayed", e)
		}
		j.UnLockLegacy()
	}
	after, _ := filepath.Glob(filepath.Join(j.bufDirPath, "*.buf*"))
	if len(before) != len(after) {
		t.Fatal("empty retry grew data segment count")
	}
}

type guardedEncodeOnly struct{ calls *int }

func (v guardedEncodeOnly) EncodeMsg(w *msgp.Writer) error { *v.calls++; return w.WriteBool(true) }
func TestGuardedJournalEncodableOnlyRunsOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls, grants := 0, 0
	guard := func(kind string, n int64) (func(), error) {
		if kind == "data" {
			grants++
			if n < 1 {
				t.Fatal("no encoded size")
			}
		}
		return func() {}, nil
	}
	j, e := NewJournal(WithBufDirPath(t.TempDir()), WithBufSizeByte(64<<10), WithStorageGuard(guard), WithFlushInterval(time.Hour), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if e = j.Start(ctx); e != nil {
		t.Fatal(e)
	}
	defer j.Close()
	if e = j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"custom": guardedEncodeOnly{&calls}}}); e != nil {
		t.Fatal(e)
	}
	if e = j.Sync(); e != nil {
		t.Fatal(e)
	}
	if calls != 1 || grants != 1 {
		t.Fatal("quota changed custom encoder semantics", calls, grants)
	}
	n, e := j.ReadRecoveryRecords(ctx, func(d *Data) error {
		if d.Data["custom"] != true {
			t.Fatal("custom value changed")
		}
		return nil
	})
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
}

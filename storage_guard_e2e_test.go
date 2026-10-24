package journal

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGuardedJournalDurableReplayAndArchiveEndToEnd(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		name := "plain"
		if compressed {
			name = "gzip"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var gate sync.Mutex
			var denyData atomic.Bool
			guard := func(kind string, n int64) (func(), error) {
				gate.Lock()
				if kind == "data" && denyData.Load() {
					gate.Unlock()
					return nil, errors.New("synthetic admission refusal")
				}
				return gate.Unlock, nil
			}
			open := func() (*Journal, context.CancelFunc) {
				t.Helper()
				ctx, cancel := context.WithCancel(context.Background())
				j, err := NewJournal(
					WithBufDirPath(dir), WithIsCompress(compressed),
					WithStorageGuard(guard), WithBufSizeByte(1<<20),
					WithFlushInterval(time.Hour), WithRotateCheckInterval(time.Hour),
					WithRotateDuration(time.Hour),
				)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				if err := j.Start(ctx); err != nil {
					cancel()
					t.Fatal(err)
				}
				t.Cleanup(func() { cancel(); j.Close() })
				return j, cancel
			}
			record := func(id int64, body string) *Data {
				return &Data{ID: id, Data: map[string]interface{}{
					"tag": "e2e.recovery",
					"message": map[string]interface{}{"body": body},
				}}
			}
			j, stop := open()
			denyData.Store(true)
			if err := j.WriteData(record(100, "not admitted")); err == nil {
				t.Fatal("denied admission was acknowledged")
			}
			denyData.Store(false)
			for _, item := range []struct {
				id   int64
				body string
			}{{101, "delivered"}, {102, "pending"}} {
				if err := j.WriteData(record(item.id, item.body)); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
			if rotated, err := j.PrepareRetainedReplay(context.Background()); err != nil || !rotated {
				t.Fatalf("freeze before delivery: rotated=%t, err=%v", rotated, err)
			}
			if !j.LockLegacy() {
				t.Fatal("retained reader ownership refused")
			}
			for _, id := range []int64{101, 102} {
				got := new(Data)
				if err := j.LoadLegacyBufRetained(got); err != nil || got.ID != id {
					t.Fatalf("initial retained snapshot id=%d got=%v, err=%v", id, got.ID, err)
				}
			}
			if err := j.LoadLegacyBufRetained(new(Data)); err != io.EOF {
				t.Fatalf("snapshot EOF: %v", err)
			}
			j.UnLockLegacy()
			// HTTP/transport acknowledgement is not retirement permission.
			if err := j.WriteId(101); err != nil {
				t.Fatal(err)
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
			stop()
			j.Close()

			checksums := func() map[string][32]byte {
				t.Helper()
				out := make(map[string][32]byte)
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				for _, e := range entries {
					if e.IsDir() {
						continue
					}
					raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
					if err != nil {
						t.Fatal(err)
					}
					out[e.Name()] = sha256.Sum256(raw)
				}
				return out
			}
			before := checksums()
			root, err := os.OpenRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			archive, err := OpenClosedRecoveryArchive(root, 1<<20)
			if err != nil {
				root.Close()
				t.Fatal(err)
			}
			var originalIDs []int64
			n, err := archive.WalkRaw(context.Background(), func(d *Data, raw []byte) error {
				if len(raw) == 0 || d.Data["tag"] != "e2e.recovery" {
					return errors.New("raw archive identity changed")
				}
				originalIDs = append(originalIDs, d.ID)
				return nil
			})
			if err != nil || n != 2 || !reflect.DeepEqual(originalIDs, []int64{101, 102}) {
				t.Fatalf("closed archive lost originals: n=%d ids=%v err=%v", n, originalIDs, err)
			}
			if err := archive.Close(); err != nil {
				t.Fatal(err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, checksums()) {
				t.Fatal("closed archive mutated persistent files")
			}

			j2, stop2 := open()
			if _, err := j2.PrepareRetainedReplay(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !j2.LockLegacy() {
				t.Fatal("restarted journal rejected replay ownership")
			}
			got := new(Data)
			if err := j2.LoadLegacyBufRetained(got); err != nil || got.ID != 102 {
				t.Fatalf("replay must skip persisted ACK and return pending record: got=%d err=%v", got.ID, err)
			}
			if err := j2.LoadLegacyBufRetained(new(Data)); err != io.EOF {
				t.Fatalf("restarted snapshot did not reach EOF: %v", err)
			}
			if err := j2.WriteId(102); err != nil {
				t.Fatal(err)
			}
			if err := j2.CleanLegacyBuf(); err != nil {
				t.Fatal(err)
			}
			j2.UnLockLegacy()
			if has, err := j2.HasRetainedData(); err != nil || has {
				t.Fatalf("explicitly retired original still present: %t %v", has, err)
			}
			if j2.HasUnacknowledgedData() {
				t.Fatal("fully retired recovered snapshot still blocks storage reconciliation")
			}
			n, err = j2.ReadRecoveryRecords(context.Background(), func(*Data) error {
				return errors.New("removed source was still replayable")
			})
			if err != nil || n != 0 {
				t.Fatalf("retired records appeared in recovery: n=%d err=%v", n, err)
			}
			stop2()
			j2.Close()
		})
	}
}

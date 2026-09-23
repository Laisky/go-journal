package journal_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

// All fixtures use public APIs and ordinary filesystem operations. The oracle
// is the caller's original data, never the journal's internal cache or counters.
func behaviorNew(t *testing.T, dir string, compressed bool) *journal.Journal {
	t.Helper()
	j, err := journal.NewJournal(journal.WithBufDirPath(dir), journal.WithIsCompress(compressed),
		journal.WithIsAggresiveGC(false), journal.WithBufSizeByte(1<<20),
		journal.WithFlushInterval(time.Hour), journal.WithRotateDuration(time.Hour),
		journal.WithRotateCheckInterval(time.Hour), journal.WithCommitIDTTL(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.Close)
	return j
}
func behaviorStart(t *testing.T, dir string, compressed bool) *journal.Journal {
	t.Helper()
	j := behaviorNew(t, dir, compressed)
	if err := j.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return j
}
func behaviorCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func behaviorData(id int64) *journal.Data {
	return &journal.Data{ID: id, Data: map[string]interface{}{"token": fmt.Sprintf("caller-%d", id), "text": "世界 / café / payload", "nested": map[string]interface{}{"enabled": true, "number": int64(73)}}}
}
func behaviorReplay(t *testing.T, j *journal.Journal) map[int64]*journal.Data {
	t.Helper()
	if !j.LockLegacy() {
		t.Fatal("cannot acquire public replay lease")
	}
	got := map[int64]*journal.Data{}
	for {
		d := new(journal.Data)
		err := j.LoadLegacyBuf(d)
		if err == io.EOF {
			break
		}
		behaviorCheck(t, err)
		if _, ok := got[d.ID]; ok {
			t.Fatalf("unexpected duplicate in a single sealed snapshot: %d", d.ID)
		}
		got[d.ID] = d
		// Transfer ownership to the active journal before allowing old-file cleanup.
		behaviorCheck(t, j.WriteData(d))
	}
	return got
}
func behaviorNoPanic(t *testing.T, fn func() error) (err error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("public call panicked: %v", p)
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return fn()
}

func TestBehaviorRestartContract(t *testing.T) {
	for _, gz := range []bool{false, true} {
		for _, ack := range []string{"none", "alternate", "all"} {
			t.Run(fmt.Sprintf("gzip=%v/ack=%s", gz, ack), func(t *testing.T) {
				dir := t.TempDir()
				j := behaviorStart(t, dir, gz)
				want := map[int64]*journal.Data{}
				ids := []int64{0, 91, 5, 72, 1, 1000, 8}
				for n, id := range ids {
					d := behaviorData(id)
					behaviorCheck(t, j.WriteData(d))
					if ack == "all" || (ack == "alternate" && n%2 == 0) {
						behaviorCheck(t, j.WriteId(id))
						behaviorCheck(t, j.WriteId(id))
					} else {
						want[id] = d
					}
					if n == 3 {
						behaviorCheck(t, j.Rotate(context.Background()))
					}
				}
				behaviorCheck(t, j.Sync())
				j.Close()
				for cycle := 0; cycle < 3; cycle++ {
					j = behaviorStart(t, dir, gz)
					high, err := j.LoadMaxId()
					behaviorCheck(t, err)
					if cycle == 0 && high != 1000 {
						t.Fatalf("high-water=%d want1000", high)
					}
					got := behaviorReplay(t, j)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("cycle %d: recovered=%v want=%v", cycle, got, want)
					}
					behaviorCheck(t, j.Sync())
					j.Close()
				}
			})
		}
	}
}

func TestBehaviorCanceledStartCanRetry(t *testing.T) {
	j := behaviorNew(t, t.TempDir(), false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := j.Start(ctx); err == nil {
		t.Error("canceled Start returned success")
	}
	behaviorCheck(t, j.Start(context.Background()))
	behaviorCheck(t, behaviorNoPanic(t, func() error { return j.WriteData(behaviorData(1)) }))
	behaviorCheck(t, j.Sync())
}
func TestBehaviorInvalidOptionsRejected(t *testing.T) {
	options := map[string]journal.OptionFunc{
		"flush": journal.WithFlushInterval(-time.Second), "rotate": journal.WithRotateDuration(-time.Second),
		"check": journal.WithRotateCheckInterval(-time.Second), "ttl": journal.WithCommitIDTTL(-time.Second),
		"size": journal.WithBufSizeByte(-1),
	}
	for name, opt := range options {
		t.Run(name, func(t *testing.T) {
			j, err := journal.NewJournal(opt)
			if j != nil {
				j.Close()
			}
			if err == nil {
				t.Fatal("invalid option accepted")
			}
		})
	}
}
func TestBehaviorLifecycleMisuseReturnsError(t *testing.T) {
	j := behaviorNew(t, t.TempDir(), false)
	for name, fn := range map[string]func() error{
		"data": func() error { return j.WriteData(behaviorData(1)) }, "id": func() error { return j.WriteId(1) },
		"sync": j.Sync, "max": func() error { _, err := j.LoadMaxId(); return err },
	} {
		t.Run("beforeStart/"+name, func(t *testing.T) {
			if behaviorNoPanic(t, fn) == nil {
				t.Error("unstarted journal reported success")
			}
		})
	}
	behaviorCheck(t, j.Start(context.Background()))
	for name, fn := range map[string]func() error{"nilData": func() error { return j.WriteData(nil) }, "negativeID": func() error { return j.WriteData(behaviorData(-1)) }} {
		t.Run(name, func(t *testing.T) {
			if behaviorNoPanic(t, fn) == nil {
				t.Error("invalid data accepted")
			}
		})
	}
	j.Close()
	for name, fn := range map[string]func() error{"data": func() error { return j.WriteData(behaviorData(2)) }, "id": func() error { return j.WriteId(2) }, "sync": j.Sync, "rotate": func() error { return j.Rotate(context.Background()) }} {
		t.Run("afterClose/"+name, func(t *testing.T) {
			if behaviorNoPanic(t, fn) == nil {
				t.Error("closed journal reported success")
			}
		})
	}
}

func TestBehaviorAcknowledgementCorruptionFailsClosedAndRetries(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := behaviorStart(t, dir, gz)
			behaviorCheck(t, j.WriteData(behaviorData(1)))
			behaviorCheck(t, j.WriteData(behaviorData(2)))
			behaviorCheck(t, j.WriteId(1))
			behaviorCheck(t, j.Sync())
			j.Close()
			suffix := ".ids"
			if gz {
				suffix += ".gz"
			}
			files, err := filepath.Glob(filepath.Join(dir, "*"+suffix))
			behaviorCheck(t, err)
			if len(files) != 1 {
				t.Fatalf("fixture: %v", files)
			}
			original, err := os.ReadFile(files[0])
			behaviorCheck(t, err)
			behaviorCheck(t, os.WriteFile(files[0], []byte{1, 2, 3}, 0600))
			j = behaviorStart(t, dir, gz)
			if _, err = j.LoadMaxId(); err == nil {
				t.Error("corrupt ACK stream silently accepted for ID recovery")
			}
			if !j.LockLegacy() {
				t.Fatal("lease")
			}
			err = j.LoadLegacyBuf(new(journal.Data))
			if err == nil || err == io.EOF {
				t.Error("corrupt ACK stream silently accepted for replay")
			}
			if j.IsLegacyRunning() {
				j.UnLockLegacy()
			}
			got, err := os.ReadFile(files[0])
			behaviorCheck(t, err)
			if !reflect.DeepEqual(got, []byte{1, 2, 3}) {
				t.Fatal("corrupt evidence changed")
			}
			behaviorCheck(t, os.WriteFile(files[0], original, 0600))
			gotRecords := behaviorReplay(t, j)
			want := map[int64]*journal.Data{2: behaviorData(2)}
			if !reflect.DeepEqual(gotRecords, want) {
				t.Fatalf("retry after repair: got=%v want=%v", gotRecords, want)
			}
		})
	}
}

func TestBehaviorCleanupFailureIsNotEOF(t *testing.T) {
	dir := t.TempDir()
	j := behaviorStart(t, dir, false)
	behaviorCheck(t, j.WriteData(behaviorData(7)))
	behaviorCheck(t, j.Sync())
	j.Close()
	files, err := filepath.Glob(filepath.Join(dir, "*.buf"))
	behaviorCheck(t, err)
	if len(files) != 1 {
		t.Fatalf("fixture: %v", files)
	}
	old := files[0]
	saved := old + ".saved"
	original, err := os.ReadFile(old)
	behaviorCheck(t, err)
	j = behaviorStart(t, dir, false)
	if !j.LockLegacy() {
		t.Fatal("lease")
	}
	d := new(journal.Data)
	behaviorCheck(t, j.LoadLegacyBuf(d))
	behaviorCheck(t, j.WriteData(d))
	behaviorCheck(t, os.Rename(old, saved))
	behaviorCheck(t, os.Mkdir(old, 0700))
	behaviorCheck(t, os.WriteFile(filepath.Join(old, "blocker"), []byte("preserve"), 0600))
	err = j.LoadLegacyBuf(new(journal.Data))
	if err == nil || err == io.EOF {
		t.Errorf("failed cleanup reported as completed EOF: %v", err)
	}
	got, err := os.ReadFile(saved)
	behaviorCheck(t, err)
	if !reflect.DeepEqual(got, original) {
		t.Fatal("preserved source changed")
	}
	behaviorCheck(t, os.RemoveAll(old))
	behaviorCheck(t, os.Rename(saved, old))
	if j.IsLegacyRunning() {
		j.UnLockLegacy()
	}
	if !j.LockLegacy() {
		t.Fatal("retry lease")
	}
	if err = j.LoadLegacyBuf(new(journal.Data)); err != io.EOF {
		t.Fatalf("cleanup retry: %v", err)
	}
	if _, err = os.Stat(old); !os.IsNotExist(err) {
		t.Errorf("failed cleanup was forgotten: %v", err)
	}
	j.Close()
	j = behaviorStart(t, dir, false)
	gotRecords := behaviorReplay(t, j)
	if !reflect.DeepEqual(gotRecords, map[int64]*journal.Data{7: behaviorData(7)}) {
		t.Fatal("replacement lost after retry")
	}
}

func TestBehaviorFailedRotationKeepsWriterUsable(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := behaviorStart(t, dir, gz)
			behaviorCheck(t, j.WriteData(behaviorData(8)))
			behaviorCheck(t, j.Sync())
			suffix := ".ids"
			if gz {
				suffix += ".gz"
			}
			files, err := filepath.Glob(filepath.Join(dir, "*"+suffix))
			behaviorCheck(t, err)
			next, err := journal.GenerateNewBufFName(time.Now().UTC(), filepath.Base(files[0]))
			behaviorCheck(t, err)
			blocked := filepath.Join(dir, next)
			behaviorCheck(t, os.Mkdir(blocked, 0700))
			// A documented replay lease prevents rescanning the active writer snapshot.
			if !j.LockLegacy() {
				t.Fatal("lease")
			}
			err = j.Rotate(context.Background())
			if err == nil {
				t.Fatal("rotation into a directory unexpectedly succeeded")
			}
			j.UnLockLegacy()
			behaviorCheck(t, behaviorNoPanic(t, func() error { return j.WriteData(behaviorData(9)) }))
			behaviorCheck(t, os.Remove(blocked))
			behaviorCheck(t, behaviorNoPanic(t, j.Sync))
			behaviorCheck(t, behaviorNoPanic(t, func() error { return j.Rotate(context.Background()) }))
			j.Close()
			j = behaviorStart(t, dir, gz)
			if got := behaviorReplay(t, j); !reflect.DeepEqual(got, map[int64]*journal.Data{8: behaviorData(8), 9: behaviorData(9)}) {
				t.Fatalf("rotation failure lost accepted data: %v", got)
			}
		})
	}
}

func TestBehaviorDirectoryHasSingleOwner(t *testing.T) {
	dir := t.TempDir()
	first := behaviorStart(t, dir, false)
	second := behaviorNew(t, dir, false)
	if err := second.Start(context.Background()); err == nil {
		t.Fatal("second writer acquired an already-owned journal directory")
	}
	behaviorCheck(t, first.WriteData(behaviorData(42)))
	behaviorCheck(t, first.Sync())
	first.Close()
	behaviorCheck(t, second.Start(context.Background()))
	if got := behaviorReplay(t, second); !reflect.DeepEqual(got, map[int64]*journal.Data{42: behaviorData(42)}) {
		t.Fatalf("ownership transfer: %v", got)
	}
}

func TestBehaviorFileNamesStayDiscoverable(t *testing.T) {
	now := time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)
	for _, old := range []string{"a.buf", "20260923.buf", "20260923_00000001.bufXgz", "20260923_99999999.ids", "20261399_00000001.buf"} {
		t.Run(old, func(t *testing.T) {
			err := behaviorNoPanic(t, func() error { _, err := journal.GenerateNewBufFName(now, old); return err })
			if err == nil {
				t.Fatal("invalid/exhausted name accepted")
			}
		})
	}
	t.Run("clockRollback", func(t *testing.T) {
		old := "20260924_00000001.buf"
		got, err := journal.GenerateNewBufFName(now, old)
		behaviorCheck(t, err)
		if got <= old {
			t.Fatalf("clock rollback reused older namespace: %s", got)
		}
	})
}
func TestBehaviorUnrelatedFilesAreNotRecoveryInput(t *testing.T) {
	dir := t.TempDir()
	j := behaviorStart(t, dir, false)
	behaviorCheck(t, j.WriteData(behaviorData(23)))
	behaviorCheck(t, j.Sync())
	j.Close()
	stray := filepath.Join(dir, "99990101_00000001.bufXgz")
	behaviorCheck(t, os.WriteFile(stray, []byte("not a WAL"), 0600))
	j = behaviorStart(t, dir, false)
	id, err := j.LoadMaxId()
	behaviorCheck(t, err)
	if id != 23 {
		t.Fatalf("id=%d", id)
	}
	if got := behaviorReplay(t, j); !reflect.DeepEqual(got, map[int64]*journal.Data{23: behaviorData(23)}) {
		t.Fatalf("replay=%v", got)
	}
	got, err := os.ReadFile(stray)
	behaviorCheck(t, err)
	if string(got) != "not a WAL" {
		t.Fatal("unrelated file altered")
	}
}

func TestBehaviorConcurrentPublicOperations(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := behaviorStart(t, dir, gz)
			var wg sync.WaitGroup
			for worker := 0; worker < 4; worker++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 1; i <= 40; i++ {
						id := int64(w*40 + i)
						if err := j.WriteData(behaviorData(id)); err != nil {
							t.Error(err)
							return
						}
						if id%3 == 0 {
							if err := j.WriteId(id); err != nil {
								t.Error(err)
							}
						}
					}
				}(worker)
			}
			for mode := 0; mode < 3; mode++ {
				wg.Add(1)
				go func(m int) {
					defer wg.Done()
					for i := 0; i < 12; i++ {
						var err error
						switch m {
						case 0:
							err = j.Flush()
						case 1:
							err = j.Sync()
						case 2:
							err = j.Rotate(context.Background())
						}
						if err != nil {
							t.Error(err)
							return
						}
					}
				}(mode)
			}
			wg.Wait()
			behaviorCheck(t, j.Sync())
			j.Close()
			j = behaviorStart(t, dir, gz)
			got := behaviorReplay(t, j)
			want := map[int64]*journal.Data{}
			for id := int64(1); id <= 160; id++ {
				if id%3 != 0 {
					want[id] = behaviorData(id)
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("concurrent operations: got %d want %d", len(got), len(want))
			}
		})
	}
}

func TestBehaviorEncoderClosedOperations(t *testing.T) {
	for _, gz := range []bool{false, true} {
		for _, kind := range []string{"data", "ids"} {
			t.Run(fmt.Sprintf("%s/%v", kind, gz), func(t *testing.T) {
				fp, err := os.Create(filepath.Join(t.TempDir(), "stream"))
				behaviorCheck(t, err)
				defer fp.Close()
				var closeFn, flushFn, writeFn func() error
				if kind == "data" {
					enc, err := journal.NewDataEncoder(fp, gz)
					behaviorCheck(t, err)
					closeFn = enc.Close
					flushFn = enc.Flush
					writeFn = func() error { return enc.Write(behaviorData(1)) }
				} else {
					enc, err := journal.NewIdsEncoder(fp, gz)
					behaviorCheck(t, err)
					closeFn = enc.Close
					flushFn = enc.Flush
					writeFn = func() error { return enc.Write(1) }
				}
				behaviorCheck(t, writeFn())
				behaviorCheck(t, closeFn())
				behaviorCheck(t, behaviorNoPanic(t, closeFn))
				if behaviorNoPanic(t, writeFn) == nil {
					t.Error("closed encoder accepted write")
				}
				if behaviorNoPanic(t, flushFn) == nil {
					t.Error("closed encoder accepted flush")
				}
			})
		}
	}
}

func FuzzBehaviorFileNames(f *testing.F) {
	for _, s := range []string{"a.buf", "20260923_00000001.ids", "20260101_00000000.buf.gz", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, err := journal.GenerateNewBufFName(time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC), s)
		if err == nil && (strings.ContainsAny(got, "/\\") || len(got) < 21) {
			t.Fatalf("invalid generated path %q", got)
		}
	})
}

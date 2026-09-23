package journal_test

import (
	"bytes"
	"context"
	"errors"
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

func userJournal(t *testing.T, dir string, compressed bool) *journal.Journal {
	t.Helper()
	j, err := journal.NewJournal(journal.WithBufDirPath(dir), journal.WithIsCompress(compressed),
		journal.WithIsAggresiveGC(false), journal.WithBufSizeByte(1<<20), journal.WithFlushInterval(time.Hour),
		journal.WithRotateDuration(time.Hour), journal.WithRotateCheckInterval(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.Close)
	return j
}

func userPut(t *testing.T, j *journal.Journal, id int64) {
	t.Helper()
	if err := j.WriteData(&journal.Data{ID: id, Data: map[string]interface{}{"event": fmt.Sprint(id), "payload": strings.Repeat("hello\x00世界", 17)}}); err != nil {
		t.Fatal(err)
	}
}
func userSync(t *testing.T, j *journal.Journal) {
	t.Helper()
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
}
func userReplay(t *testing.T, j *journal.Journal) map[int64]string {
	t.Helper()
	if !j.LockLegacy() {
		t.Fatal("cannot acquire recovery ownership")
	}
	defer j.UnLockLegacy()
	got := map[int64]string{}
	for {
		d := new(journal.Data)
		err := j.LoadLegacyBuf(d)
		if err == io.EOF {
			return got
		}
		if err != nil {
			t.Fatal(err)
		}
		want := strings.Repeat("hello\x00世界", 17)
		if d.Data["event"] != fmt.Sprint(d.ID) || d.Data["payload"] != want {
			t.Fatalf("record content changed: %+v", d)
		}
		if _, ok := got[d.ID]; ok {
			t.Fatalf("unexpected duplicate in single-pass fixture: %d", d.ID)
		}
		got[d.ID] = d.Data["event"].(string)
		if err = j.WriteData(d); err != nil {
			t.Fatal(err)
		} // transfer ownership before EOF cleanup
	}
}
func userNames(t *testing.T, dir, kind string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasSuffix(n, kind) || strings.HasSuffix(n, kind+".gz") {
			names = append(names, filepath.Join(dir, n))
		}
	}
	return names
}

func TestUserCompressionMigration(t *testing.T) {
	for _, first := range []bool{false, true} {
		t.Run(fmt.Sprint(first), func(t *testing.T) {
			dir := t.TempDir()
			want := map[int64]string{}
			for round, compressed := range []bool{first, !first, first} {
				j := userJournal(t, dir, compressed)
				// Read the predecessor with its historical format, not this writer's setting.
				if got := userReplay(t, j); !reflect.DeepEqual(got, want) {
					t.Fatalf("round %d replay=%v want=%v", round, got, want)
				}
				id := int64(round + 1)
				userPut(t, j, id)
				want[id] = fmt.Sprint(id)
				userSync(t, j)
				j.Close()
			}
			j := userJournal(t, dir, !first)
			if got := userReplay(t, j); !reflect.DeepEqual(got, want) {
				t.Fatalf("migration lost records: got=%v want=%v", got, want)
			}
		})
	}
}

func TestUserAcknowledgementDamageFailsClosedAndCanRetry(t *testing.T) {
	for _, gz := range []bool{false, true} {
		for _, damage := range []string{"missing", "truncated", "garbage"} {
			t.Run(fmt.Sprintf("gzip=%v/%s", gz, damage), func(t *testing.T) {
				dir := t.TempDir()
				j := userJournal(t, dir, gz)
				userPut(t, j, 7)
				userPut(t, j, 91)
				if err := j.WriteId(7); err != nil {
					t.Fatal(err)
				}
				userSync(t, j)
				j.Close()
				ids := userNames(t, dir, ".ids")
				if len(ids) != 1 {
					t.Fatal(ids)
				}
				original, err := os.ReadFile(ids[0])
				if err != nil {
					t.Fatal(err)
				}
				// Start snapshots the real directory. Fault then affects an actual retained file.
				j = userJournal(t, dir, gz)
				switch damage {
				case "missing":
					err = os.Remove(ids[0])
				case "truncated":
					err = os.WriteFile(ids[0], original[:len(original)-1], 0600)
				case "garbage":
					err = os.WriteFile(ids[0], []byte{255, 255, 255, 255, 255, 255, 255, 255}, 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					if _, err = j.LoadMaxId(); err == nil {
						t.Errorf("damaged ACK was accepted by high-water scan (attempt %d)", attempt)
					}
					if !j.LockLegacy() {
						t.Fatal("lock")
					}
					err = j.LoadLegacyBuf(new(journal.Data))
					j.UnLockLegacy()
					if err == nil || err == io.EOF {
						t.Errorf("damaged ACK allowed replay/cleanup (attempt %d): %v", attempt, err)
					}
				}
				if len(userNames(t, dir, ".buf")) < 2 {
					t.Fatal("failed recovery removed source data")
				}
				if err = os.WriteFile(ids[0], original, 0600); err != nil {
					t.Fatal(err)
				}
				if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{91: "91"}) {
					t.Fatalf("repair did not restore exact pending set: %v", got)
				}
			})
		}
	}
}

func TestUserEmptyCompressedAcknowledgementsAreValid(t *testing.T) {
	dir := t.TempDir()
	j := userJournal(t, dir, true)
	userPut(t, j, 91)
	userSync(t, j)
	j.Close()
	ids := userNames(t, dir, ".ids")
	if len(ids) != 1 {
		t.Fatal(ids)
	}
	// Equivalent to a crash before the first ACK is appended.
	if err := os.Truncate(ids[0], 0); err != nil {
		t.Fatal(err)
	}
	j = userJournal(t, dir, true)
	if high, err := j.LoadMaxId(); err != nil || high != 91 {
		t.Fatalf("empty ACK: high=%d err=%v", high, err)
	}
	if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{91: "91"}) {
		t.Fatal(got)
	}
}

func TestUserUnrelatedFilesAreNotRecoveryInput(t *testing.T) {
	dir := t.TempDir()
	j := userJournal(t, dir, false)
	userPut(t, j, 1)
	userSync(t, j)
	j.Close()
	unrelated := filepath.Join(dir, "99990101_00000001.bufXgz")
	sentinel := []byte("user-owned unrelated file; never parse or delete")
	if err := os.WriteFile(unrelated, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	j = userJournal(t, dir, false)
	if high, err := j.LoadMaxId(); err != nil || high != 1 {
		t.Fatalf("unrelated file affected recovery: %d %v", high, err)
	}
	if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{1: "1"}) {
		t.Fatal(got)
	}
	b, err := os.ReadFile(unrelated)
	if err != nil || !bytes.Equal(b, sentinel) {
		t.Fatal("unrelated file modified")
	}
}

func TestUserClockRollbackDoesNotOverwriteSegments(t *testing.T) {
	dir := t.TempDir()
	j := userJournal(t, dir, false)
	userPut(t, j, 1)
	userSync(t, j)
	j.Close()
	// Historical timestamps may exceed today's clock after a clock correction.
	for _, kind := range []string{".buf", ".ids"} {
		names := userNames(t, dir, kind)
		if err := os.Rename(names[0], filepath.Join(dir, "99990101_00000001"+kind)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{2, 3} {
		j = userJournal(t, dir, false)
		userPut(t, j, id)
		userSync(t, j)
		j.Close()
	}
	j = userJournal(t, dir, false)
	if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{1: "1", 2: "2", 3: "3"}) {
		t.Fatalf("clock rollback overwrote retained records: %v", got)
	}
}

func TestUserRotationFailureKeepsWriterUsable(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := userJournal(t, dir, gz)
			userPut(t, j, 1)
			userSync(t, j)
			// A temporarily inaccessible directory is a recoverable rotation failure.
			moved := dir + "-moved"
			if err := os.Rename(dir, moved); err != nil {
				t.Fatal(err)
			}
			err := j.Rotate(context.Background())
			if e := os.Rename(moved, dir); e != nil {
				t.Fatal(e)
			}
			if err == nil {
				t.Fatal("rotation through missing directory reported success")
			}
			userPut(t, j, 2)
			userSync(t, j)
			if err = j.Rotate(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{1: "1", 2: "2"}) {
				t.Fatal(got)
			}
		})
	}
}

func TestUserInvalidCallsReturnErrors(t *testing.T) {
	for _, stage := range []string{"before-start", "started", "closed"} {
		for _, op := range []string{"write", "ack", "sync", "flush", "max", "nil-data", "negative-id", "rotate-canceled"} {
			t.Run(stage+"/"+op, func(t *testing.T) {
				defer func() {
					if p := recover(); p != nil {
						t.Errorf("public call panicked: %v", p)
					}
				}()
				j, err := journal.NewJournal(journal.WithBufDirPath(t.TempDir()), journal.WithIsAggresiveGC(false))
				if err != nil {
					t.Fatal(err)
				}
				defer j.Close()
				if stage != "before-start" {
					if err = j.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if stage == "closed" {
					j.Close()
				}
				switch op {
				case "write":
					err = j.WriteData(&journal.Data{ID: 1, Data: map[string]interface{}{"event": "1"}})
				case "ack":
					err = j.WriteId(1)
				case "sync":
					err = j.Sync()
				case "flush":
					err = j.Flush()
				case "max":
					_, err = j.LoadMaxId()
				case "nil-data":
					err = j.WriteData(nil)
				case "negative-id":
					err = j.WriteData(&journal.Data{ID: -1})
				case "rotate-canceled":
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					err = j.Rotate(ctx)
				}
				shouldFail := stage != "started" || op == "nil-data" || op == "negative-id" || op == "rotate-canceled"
				if shouldFail && err == nil {
					t.Error("invalid public call reported success")
				}
				if !shouldFail && err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestUserCanceledStartCanBeRetried(t *testing.T) {
	j, err := journal.NewJournal(journal.WithBufDirPath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = j.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Start=%v", err)
	}
	if err = j.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	userPut(t, j, 1)
	userSync(t, j)
}

func TestUserRejectsNegativeOptions(t *testing.T) {
	opts := map[string]journal.OptionFunc{"flush": journal.WithFlushInterval(-time.Second), "rotation": journal.WithRotateDuration(-time.Second), "rotation-check": journal.WithRotateCheckInterval(-time.Second), "ttl": journal.WithCommitIDTTL(-time.Second), "size": journal.WithBufSizeByte(-1)}
	for name, opt := range opts {
		t.Run(name, func(t *testing.T) {
			j, err := journal.NewJournal(opt)
			if j != nil {
				j.Close()
			}
			if err == nil {
				t.Error("negative option accepted")
			}
		})
	}
}

// Close actually overlaps writers and maintenance here. Only a successful Sync
// is a durable receipt; unanswered/failed operations may still be recovered.
func TestUserFlushConcurrentWithRotationAndClose(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := userJournal(t, dir, gz)
			userPut(t, j, 0)
			userSync(t, j)
			start := make(chan struct{})
			receipts := make(chan int64, 128)
			ready := make(chan struct{}, 1)
			problems := make(chan error, 128)
			var wg sync.WaitGroup
			check := func(err error) bool {
				if err != nil && !errors.Is(err, os.ErrClosed) {
					problems <- err
				}
				return err == nil
			}
			for worker := 0; worker < 4; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for n := 1; n <= 24; n++ {
						id := int64(worker*24 + n)
						d := &journal.Data{ID: id, Data: map[string]interface{}{"event": fmt.Sprint(id), "payload": strings.Repeat("hello\x00世界", 17)}}
						if !check(j.WriteData(d)) {
							return
						}
						if check(j.Sync()) {
							receipts <- id
							select {
							case ready <- struct{}{}:
							default:
							}
						} else {
							return
						}
					}
				}(worker)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for n := 0; n < 24; n++ {
					if !check(j.Flush()) {
						return
					}
					if !check(j.Rotate(context.Background())) {
						return
					}
					if !check(j.Sync()) {
						return
					}
				}
			}()
			close(start)
			select {
			case <-ready:
			case <-time.After(10 * time.Second):
				t.Fatal("no durable writer progress")
			}
			j.Close()
			wg.Wait()
			close(problems)
			close(receipts)
			for err := range problems {
				t.Error(err)
			}
			j = userJournal(t, dir, gz)
			got := userReplay(t, j)
			if got[0] != "0" {
				t.Fatal("synchronized prefix lost")
			}
			for id := range receipts {
				if got[id] != fmt.Sprint(id) {
					t.Errorf("durable receipt %d missing", id)
				}
			}
			for id := range got {
				if id < 0 || id > 96 {
					t.Fatalf("invented record %d", id)
				}
			}
		})
	}
}

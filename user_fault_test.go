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
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
	"github.com/tinylib/msgp/msgp"
)

func TestUserOpenBufFilePreservesExistingData(t *testing.T) {
	name := filepath.Join(t.TempDir(), "owned.buf")
	original := []byte("already owned by another operation")
	if err := os.WriteFile(name, original, 0600); err != nil {
		t.Fatal(err)
	}
	fp, err := journal.OpenBufFile(name, 0)
	if fp != nil {
		fp.Close()
	}
	if err != nil {
		t.Fatalf("public open-or-create compatibility: %v", err)
	}
	got, e := os.ReadFile(name)
	if e != nil || !bytes.Equal(got, original) {
		t.Fatal("existing bytes changed")
	}
}

func TestUserFilenameErrorsDoNotPanic(t *testing.T) {
	for _, name := range []string{"", ".buf", "x.ids", "12345678.buf", "20260101_99999999.buf", "../20260101_00000001.buf", "20260101_00000001.bufXgz"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("filename input panicked: %v", p)
				}
			}()
			if _, err := journal.GenerateNewBufFName(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), name); err == nil {
				t.Error("invalid/exhausted filename accepted")
			}
		})
	}
}

var userEncodingFailure = errors.New("intentional custom encoding rejection")

// Implements Encodable only, not Marshaler. The fix must preserve the existing
// streaming interface, invoke it once, and not append its prefix on rejection.
type userEncodedValue struct {
	Text  string
	Fail  bool
	Calls *int
}

func (v userEncodedValue) EncodeMsg(w *msgp.Writer) error {
	if v.Calls != nil {
		(*v.Calls)++
	}
	if err := w.WriteString(v.Text); err != nil {
		return err
	}
	if v.Fail {
		return userEncodingFailure
	}
	return nil
}

func TestUserInvalidPayloadDoesNotPoisonLaterAcceptedData(t *testing.T) {
	for _, gz := range []bool{false, true} {
		for _, kind := range []string{"channel", "nested", "custom", "large-custom"} {
			t.Run(fmt.Sprintf("gzip=%v/%s", gz, kind), func(t *testing.T) {
				dir := t.TempDir()
				j := userJournal(t, dir, gz)
				userPut(t, j, 1)
				userSync(t, j)
				calls := 0
				var value interface{} = make(chan int)
				switch kind {
				case "nested":
					value = []interface{}{"valid prefix", map[string]interface{}{"bad": make(chan int)}}
				case "custom":
					value = userEncodedValue{"partial value", true, &calls}
				case "large-custom":
					value = userEncodedValue{string(bytes.Repeat([]byte("prefix"), (5<<20)/6)), true, &calls}
				}
				dataFile := userNames(t, dir, ".buf")[0]
				before, err := os.ReadFile(dataFile)
				if err != nil {
					t.Fatal(err)
				}
				err = j.WriteData(&journal.Data{ID: 2, Data: map[string]interface{}{"value": value}})
				if err == nil {
					t.Fatal("unsupported payload accepted")
				}
				if kind == "custom" || kind == "large-custom" {
					if !strings.Contains(err.Error(), userEncodingFailure.Error()) || calls != 1 {
						t.Fatalf("custom encoding contract: calls=%d error=%v", calls, err)
					}
				}
				after, e := os.ReadFile(dataFile)
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(before, after) {
					t.Error("rejected serialization appended bytes to the live stream")
				}
				userPut(t, j, 3)
				userSync(t, j)
				// Rejection did not reserve ID 2 or create an acknowledgement for it.
				userPut(t, j, 2)
				userSync(t, j)
				j.Close()
				j = userJournal(t, dir, gz)
				if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{1: "1", 2: "2", 3: "3"}) {
					t.Fatalf("invalid payload poisoned successful records: %v", got)
				}
			})
		}
	}
}

func TestUserCustomEncoderRemainsSupported(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			j := userJournal(t, dir, gz)
			calls := 0
			want := "custom encoded 世界"
			if err := j.WriteData(&journal.Data{ID: 4, Data: map[string]interface{}{"value": userEncodedValue{want, false, &calls}}}); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("custom encoder called %d times", calls)
			}
			userSync(t, j)
			j.Close()
			j = userJournal(t, dir, gz)
			if !j.LockLegacy() {
				t.Fatal("lease")
			}
			defer j.UnLockLegacy()
			d := new(journal.Data)
			if err := j.LoadLegacyBuf(d); err != nil {
				t.Fatal(err)
			}
			if d.ID != 4 || !reflect.DeepEqual(d.Data, map[string]interface{}{"value": want}) {
				t.Fatalf("changed custom payload: %+v", d)
			}
			if err := j.WriteData(d); err != nil {
				t.Fatal(err)
			}
			if err := j.LoadLegacyBuf(new(journal.Data)); err != io.EOF {
				t.Fatalf("extra record: %v", err)
			}
		})
	}
}

func TestUserCleanupFailureRemainsVisible(t *testing.T) {
	dir := t.TempDir()
	j := userJournal(t, dir, false)
	userPut(t, j, 1)
	userSync(t, j)
	old := userNames(t, dir, ".buf")[0]
	if err := j.Rotate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !j.LockLegacy() {
		t.Fatal("lock")
	}
	d := new(journal.Data)
	if err := j.LoadLegacyBuf(d); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteData(d); err != nil {
		t.Fatal(err)
	}
	// The open source descriptor remains readable, but unlink fails on a nonempty directory.
	if err := os.Rename(old, old+".held"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(old, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(old, "busy"), []byte("busy"), 0600); err != nil {
		t.Fatal(err)
	}
	err := j.LoadLegacyBuf(new(journal.Data))
	j.UnLockLegacy()
	if err == nil || err == io.EOF {
		t.Errorf("failed cleanup reported a successful EOF: %v", err)
	}
	if err = os.RemoveAll(old); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(old+".held", old); err != nil {
		t.Fatal(err)
	}
	if !j.LockLegacy() {
		t.Fatal("retry lease")
	}
	if err = j.LoadLegacyBuf(new(journal.Data)); err != io.EOF {
		t.Fatalf("cleanup retry: %v", err)
	}
	j.UnLockLegacy()
	if _, err = os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("source not reclaimed: %v", err)
	}
	j.Close()
	j = userJournal(t, dir, false)
	if got := userReplay(t, j); !reflect.DeepEqual(got, map[int64]string{1: "1"}) {
		t.Fatalf("cleanup retry lost replacement: %v", got)
	}
}

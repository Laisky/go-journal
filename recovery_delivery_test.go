package journal

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// These tests use the public journal contract. The caller never manufactures a
// loader file list and never rotates an extra time to make recovery work.
func TestRecoveryRetainedIDsAndNewestSegment(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			dir := t.TempDir()
			open := func() *Journal {
				t.Helper()
				j, err := NewJournal(WithBufDirPath(dir), WithBufSizeByte(1024*1024),
					WithIsCompress(compressed), WithRotateDuration(time.Hour), WithCommitIDTTL(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if err = j.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				return j
			}
			j := open()
			for _, id := range []int64{91, 7, 53} {
				if err := j.WriteData(&Data{ID: id, Data: map[string]interface{}{"payload": fmt.Sprint(id)}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.WriteId(7); err != nil {
				t.Fatal(err)
			}
			if err := j.Sync(); err != nil {
				t.Fatal(err)
			}
			j.Close()
			j = open()
			defer j.Close()
			high, err := j.LoadMaxId()
			if err != nil {
				t.Fatal(err)
			}
			if high != 91 {
				t.Errorf("recovered high-water=%d; must include unacknowledged records (want 91)", high)
			}
			if !j.LockLegacy() {
				t.Fatal("cannot start recovery")
			}
			got := map[int64]string{}
			for {
				d := &Data{}
				err := j.LoadLegacyBuf(d)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				got[d.ID] = d.Data["payload"].(string)
				// Recovery transfers every still-required record before EOF cleanup.
				if err := j.WriteData(d); err != nil {
					t.Fatal(err)
				}
			}
			if len(got) != 2 || got[91] != "91" || got[53] != "53" {
				t.Errorf("first recovery omitted newest sealed segment or replayed ACKed records: %v", got)
			}
		})
	}
}

func TestRecoveryDoesNotReadActiveSegment(t *testing.T) {
	j, err := NewJournal(WithBufDirPath(t.TempDir()), WithBufSizeByte(1024*1024), WithRotateDuration(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"value": "live"}}); err != nil {
		t.Fatal(err)
	}
	if !j.LockLegacy() {
		t.Fatal("cannot acquire lock")
	}
	if err = j.LoadLegacyBuf(&Data{}); err != io.EOF {
		t.Fatalf("active writer must not be replayed: %v", err)
	}
}

// Appends a genuine incomplete record after a previously synchronized record.
// Prefix recovery must not erase the original evidence or accept arbitrary
// corruption as EOF. No private loader fields are constructed by this test.
func TestRecoveryInterruptedTailAndCorruptionControl(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, corrupt := range []bool{false, true} {
			t.Run(fmt.Sprintf("gzip=%v/corrupt=%v", compressed, corrupt), func(t *testing.T) {
				dir := t.TempDir()
				open := func() *Journal {
					t.Helper()
					j, err := NewJournal(WithBufDirPath(dir), WithBufSizeByte(1024*1024), WithIsCompress(compressed), WithRotateDuration(time.Hour))
					if err != nil {
						t.Fatal(err)
					}
					if err = j.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					return j
				}
				j := open()
				if err := j.WriteData(&Data{ID: 41, Data: map[string]interface{}{"value": "accepted"}}); err != nil {
					t.Fatal(err)
				}
				if err := j.Sync(); err != nil {
					t.Fatal(err)
				}
				j.Close()
				suffix := ".buf"
				if compressed {
					suffix += ".gz"
				}
				files, err := filepath.Glob(filepath.Join(dir, "*"+suffix))
				if err != nil || len(files) != 1 {
					t.Fatalf("data files=%v error=%v", files, err)
				}
				payload := make([]byte, 8192)
				rand.New(rand.NewSource(127)).Read(payload)
				tail, err := (&Data{ID: 99, Data: map[string]interface{}{"value": payload}}).MarshalMsg(nil)
				if err != nil {
					t.Fatal(err)
				}
				if compressed {
					var buf bytes.Buffer
					writer := gzip.NewWriter(&buf)
					if _, err = writer.Write(tail); err != nil {
						t.Fatal(err)
					}
					if err = writer.Close(); err != nil {
						t.Fatal(err)
					}
					tail = buf.Bytes()
				}
				if corrupt {
					tail = []byte("this is not a journal record")
				} else {
					tail = tail[:len(tail)/2]
				}
				fp, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = fp.Write(tail); err != nil {
					t.Fatal(err)
				}
				if err = fp.Sync(); err != nil {
					t.Fatal(err)
				}
				fp.Close()
				original, err := os.ReadFile(files[0])
				if err != nil {
					t.Fatal(err)
				}
				j = open()
				defer j.Close()
				high, err := j.LoadMaxId()
				if corrupt {
					if err == nil {
						t.Fatal("arbitrary corruption must fail closed")
					}
					retained, e := os.ReadFile(files[0])
					if e != nil || !bytes.Equal(retained, original) {
						t.Fatal("corrupt source was changed")
					}
					return
				}
				if err != nil || high != 41 {
					t.Fatalf("recover prefix high=%d err=%v", high, err)
				}
				if !j.LockLegacy() {
					t.Fatal("lock recovery")
				}
				d := &Data{}
				if err = j.LoadLegacyBuf(d); err != nil || d.ID != 41 || d.Data["value"] != "accepted" {
					t.Fatalf("prefix=%+v err=%v", d, err)
				}
				if err = j.WriteData(d); err != nil {
					t.Fatal(err)
				}
				if err = j.LoadLegacyBuf(&Data{}); err != io.EOF {
					t.Fatalf("tail leaked: %v", err)
				}
				evidence, e := os.ReadFile(files[0] + ".incomplete")
				if e != nil || !bytes.Equal(evidence, original) {
					t.Fatal("interrupted append evidence was not preserved byte-for-byte")
				}
			})
		}
	}
}

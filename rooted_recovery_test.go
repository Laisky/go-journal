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

func TestJournalRootInterruptedEvidenceAfterDirectoryReplacement(t *testing.T) {
	for _, config := range []string{"path", "path-dot", "borrowed"} {
		t.Run("config="+config, func(t *testing.T) {
			testJournalRootInterruptedEvidenceAfterDirectoryReplacement(t, config)
		})
	}
}

func testJournalRootInterruptedEvidenceAfterDirectoryReplacement(t *testing.T, config string) {
	for _, compressed := range []bool{false, true} {
		for _, corrupt := range []bool{false, true} {
			t.Run(fmt.Sprintf("gzip=%v/corrupt=%v", compressed, corrupt), func(t *testing.T) {
				dir := t.TempDir()
				open := func() *Journal {
					t.Helper()
					var selected OptionFunc
					switch config {
					case "borrowed":
						if err := os.MkdirAll(dir, 0700); err != nil {
							t.Fatal(err)
						}
						root, err := os.OpenRoot(dir)
						if err != nil {
							t.Fatal(err)
						}
						defer root.Close()
						selected = WithRoot(root)
					case "path-dot":
						selected = WithBufDirPath(dir + string(os.PathSeparator) + ".")
					default:
						selected = WithBufDirPath(dir)
					}
					j, err := NewJournal(selected, WithBufSizeByte(1024*1024), WithIsCompress(compressed), WithRotateDuration(time.Hour))
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
				moved := filepath.Join(t.TempDir(), "retained")
				if err := os.Rename(dir, moved); err != nil {
					t.Fatal(err)
				}
				outside := t.TempDir()
				if err := os.Symlink(outside, dir); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					entries, err := os.ReadDir(outside)
					if err != nil || len(entries) != 0 {
						t.Errorf("recovery touched replacement target: %v %v", entries, err)
					}
				})
				for i := range files {
					files[i] = filepath.Join(moved, filepath.Base(files[i]))
				}
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

package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func recoverySource(t *testing.T, compressed bool, payload string) (string, *Journal) {
	t.Helper()
	dir := t.TempDir()
	os.Chmod(dir, 0700)
	j, e := NewJournal(WithBufDirPath(dir), WithIsCompress(compressed), WithBufSizeByte(1<<20), WithFlushInterval(time.Hour), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	if e = j.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = j.WriteData(&Data{ID: 7, Data: map[string]interface{}{"tag": "original.tag", "message": map[string]interface{}{"body": payload, "bytes": []byte{0, 255}, "large": int64(9007199254740993), "empty": map[string]interface{}{}}}}); e != nil {
		t.Fatal(e)
	}
	if e = j.Sync(); e != nil {
		t.Fatal(e)
	}
	return dir, j
}
func recoveryFileHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	entries, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		raw, e := os.ReadFile(filepath.Join(dir, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		out[entry.Name()] = sha256.Sum256(raw)
	}
	return out
}
func TestClosedRecoveryArchiveExactRawOwnershipAndPreservation(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "gzip"}[compressed], func(t *testing.T) {
			dir, j := recoverySource(t, compressed, "original body")
			root, e := os.OpenRoot(dir)
			if e != nil {
				t.Fatal(e)
			}
			defer root.Close()
			if a, e := OpenClosedRecoveryArchive(root, 1<<20); e == nil {
				a.Close()
				t.Fatal("live source ownership bypassed")
			}
			j.Close()
			before := recoveryFileHashes(t, dir)
			a, e := OpenClosedRecoveryArchive(root, 1<<20)
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close()
			var first []byte
			for pass := 0; pass < 2; pass++ {
				n, e := a.WalkRaw(context.Background(), func(d *Data, raw []byte) error {
					if d.ID != 7 || d.Data["tag"] != "original.tag" {
						t.Fatal("identity changed")
					}
					message := d.Data["message"].(map[string]interface{})
					if message["body"] != "original body" || message["large"] != int64(9007199254740993) || !bytes.Equal(message["bytes"].([]byte), []byte{0, 255}) {
						t.Fatal("original types changed")
					}
					if first == nil {
						first = append([]byte(nil), raw...)
					} else if !bytes.Equal(first, raw) {
						t.Fatal("raw bytes changed")
					}
					return nil
				})
				if e != nil || n != 1 {
					t.Fatal("closed archive walk", n, e)
				}
			}
			if !reflect.DeepEqual(before, recoveryFileHashes(t, dir)) {
				t.Fatal("source changed")
			}
		})
	}
}
func TestClosedRecoveryArchiveRejectsFreshOversizedTruncatedOrChangedSource(t *testing.T) {
	root, e := os.OpenRoot(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if a, e := OpenClosedRecoveryArchive(root, 4096); e == nil {
		a.Close()
		t.Fatal("fresh root initialized")
	}
	root.Close()
	dir, j := recoverySource(t, false, string(bytes.Repeat([]byte("x"), 8192)))
	j.Close()
	root, e = os.OpenRoot(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	a, e := OpenClosedRecoveryArchive(root, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.WalkRaw(context.Background(), func(*Data, []byte) error { return nil }); e == nil {
		t.Fatal("oversized record accepted")
	}
	a.Close()
	a, e = OpenClosedRecoveryArchive(root, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	file := a.files[0].path
	st, _ := os.Stat(file)
	if e = os.Truncate(file, st.Size()-1); e != nil {
		t.Fatal(e)
	}
	if _, e = a.WalkRaw(context.Background(), func(*Data, []byte) error { return nil }); e == nil {
		t.Fatal("captured prefix changed unnoticed")
	}
	a.Close()
	a, e = OpenClosedRecoveryArchive(root, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if n, e := a.WalkRaw(context.Background(), func(*Data, []byte) error { return nil }); e == nil || n != 0 {
		t.Fatal("truncated stream accepted", n, e)
	}
}

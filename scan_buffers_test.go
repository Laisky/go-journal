package journal

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
)

func scanBufferFile(t *testing.T, path string, wire []byte, compressed bool) *os.File {
	t.Helper()
	if compressed {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		if _, err := w.Write(wire); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		wire = b.Bytes()
	}
	if err := os.WriteFile(path, wire, 0600); err != nil {
		t.Fatal(err)
	}
	fp, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fp.Close() })
	return fp
}

func TestScanBufferStateAndSizeClasses(t *testing.T) {
	var buffers scanReaderBuffers
	dir := t.TempDir()
	missingID := msgp.AppendMapHeader(nil, 1)
	missingID = msgp.AppendString(missingID, "Data")
	missingID = msgp.AppendMapHeader(missingID, 0)
	big := bytes.Repeat(maxIDWire(17, "one-kib-"+string(bytes.Repeat([]byte("x"), 1024))), 4096)
	fixtures := []struct {
		wire []byte
		gzip bool
	}{
		{maxIDWire(999, "first"), false},
		{maxIDWire(7, "truncated")[:7], false},
		{missingID, false},
		{big, false},
		{maxIDWire(-31, "negative"), false},
		{maxIDWire(6, "gzip state is independent"), true},
		{maxIDWire(1, "last"), false},
	}
	var firstSmall *DataDecoder
	for n, fixture := range fixtures {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			fp := scanBufferFile(t, filepath.Join(dir, fmt.Sprint(n)), fixture.wire, fixture.gzip)
			info, err := fp.Stat()
			if err != nil {
				t.Fatal(err)
			}
			got, err := buffers.decoder(fp, info, fixture.gzip)
			if err != nil {
				t.Fatal(err)
			}
			defer buffers.release(got)
			if !fixture.gzip && info.Size() < BufSize {
				if firstSmall == nil {
					firstSmall = got
				} else if got != firstSmall {
					t.Fatal("small decoder not reused within this scan")
				}
			}
			fresh, err := os.Open(fp.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			want, err := NewDataDecoder(fresh, fixture.gzip)
			if err != nil {
				t.Fatal(err)
			}
			for step := 0; step < 5000; step++ {
				id, e := got.readRecordID()
				expected, referenceErr := scanReference(want)
				if (e == nil) != (referenceErr == nil) || (e == io.EOF) != (referenceErr == io.EOF) || incompleteRecord(e) != incompleteRecord(referenceErr) {
					t.Fatalf("error changed after reuse: %v / %v", e, referenceErr)
				}
				if e != nil {
					return
				}
				if id != expected {
					t.Fatalf("inherited record or file state: %d / %d", id, expected)
				}
			}
			t.Fatal("scan failed to end")
		})
	}
	if buffers.small == nil || buffers.large == nil || buffers.small == buffers.large {
		t.Fatal("size classes not independent")
	}
	for _, d := range []*DataDecoder{buffers.small, buffers.large} {
		if d.reader.R.Buffered() != 0 {
			t.Fatal("buffered record remained after release")
		}
	}
}

func TestScanBufferDoesNotRetainGrownOrGzipReaders(t *testing.T) {
	var buffers scanReaderBuffers
	fp := scanBufferFile(t, filepath.Join(t.TempDir(), "plain"), maxIDWire(8, "small"), false)
	info, _ := fp.Stat()
	d, err := buffers.decoder(fp, info, false)
	if err != nil {
		t.Fatal(err)
	}
	// Force the pinned reader's dynamic Peek growth, then hit a stored error.
	if _, err := d.reader.R.Peek(BufSize * 2); err == nil {
		t.Fatal("fixture unexpectedly large")
	}
	buffers.release(d)
	if buffers.small != nil || buffers.large != nil {
		t.Fatal("oversized lookahead retained")
	}
	gz := scanBufferFile(t, filepath.Join(t.TempDir(), "gzip"), maxIDWire(9, "gzip"), true)
	info, _ = gz.Stat()
	d, err = buffers.decoder(gz, info, true)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := d.readRecordID(); err != nil || id != 9 {
		t.Fatal("gzip reader changed", id, err)
	}
	buffers.release(d)
	if buffers.small != nil || buffers.large != nil {
		t.Fatal("gzip member state retained in plain cache")
	}
}

func TestConcurrentMultiSegmentMaxIDDoesNotConsumeState(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%t", compressed), func(t *testing.T) {
			ctx := context.Background()
			j, err := NewJournal(WithBufDirPath(t.TempDir()), WithIsCompress(compressed), WithIsAggresiveGC(false),
				WithFlushInterval(time.Hour), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if err := j.Start(ctx); err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			for id := int64(1); id <= 128; id++ {
				if err := j.WriteData(&Data{ID: id, Data: map[string]interface{}{"body": fmt.Sprint(id)}}); err != nil {
					t.Fatal(err)
				}
				if id%2 == 0 {
					if err := j.WriteId(id); err != nil {
						t.Fatal(err)
					}
				}
				if id%16 == 0 {
					if err := j.Rotate(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := 0; n < 8; n++ {
						if id, err := j.LoadMaxId(); err != nil || id != 128 {
							t.Errorf("concurrent frontier %d: %v", id, err)
						}
					}
				}()
			}
			wg.Wait()
			if !j.LockLegacy() {
				t.Fatal("cannot acquire replay lease")
			}
			defer j.UnLockLegacy()
			seen := make(map[int64]bool)
			for {
				var d Data
				err := j.LoadLegacyBuf(&d)
				if err == io.EOF {
					break
				}
				if err != nil || d.ID%2 != 1 || seen[d.ID] || d.Data["body"] != fmt.Sprint(d.ID) {
					t.Fatalf("scan consumed ACK or changed payload: %v, %v", d, err)
				}
				seen[d.ID] = true
				if err := j.WriteData(&d); err != nil {
					t.Fatal(err)
				}
				if err := j.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if len(seen) != 64 {
				t.Fatalf("pending set changed: %d", len(seen))
			}
		})
	}
}

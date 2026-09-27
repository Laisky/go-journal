package journal

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
)

// Construct fixtures independently of the production ACK encoder.
func ackScanWire(ids ...int64) []byte {
	var out bytes.Buffer
	for n, id := range ids {
		word := id
		if n > 0 {
			word -= ids[0]
		}
		if err := binary.Write(&out, binary.BigEndian, word); err != nil {
			panic(err)
		}
	}
	return out.Bytes()
}

func ackScanRead(name string, buffers *scanIDBuffers) (got []int64, err error) {
	err = readIDsFileWithBuffers(name, func(dec *IdsDecoder) error {
		for {
			id, err := dec.readID()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			got = append(got, id)
		}
	}, buffers)
	return
}

func TestACKScanReuseResetsBaseUnreadBytesAndErrors(t *testing.T) {
	var buffers scanIDBuffers
	dir := t.TempDir()
	fixtures := [][]byte{
		ackScanWire(9000, 9001, 0),
		ackScanWire(7, 2, math.MaxInt64),
		ackScanWire(100)[:3],
		ackScanWire(0, 5, 0),
		ackScanWire(-1),
		ackScanWire(12, 9, 3),
		append(ackScanWire(2), ackScanWire(-3)...), // negative reconstructed ID
		append(ackScanWire(math.MaxInt64), ackScanWire(1)...),
		ackScanWire(1, 2, 3),
	}
	for trim := 1; trim <= 7; trim++ {
		wire := ackScanWire(99, 100)
		fixtures = append(fixtures, wire[:len(wire)-trim], ackScanWire(4, 0))
	}
	many := make([]int64, (readBufferSize/8)+17)
	for n := range many {
		many[n] = int64(len(many) - n)
	}
	fixtures = append(fixtures, ackScanWire(many...), ackScanWire(2, 1))
	var first *IdsDecoder
	for n, wire := range fixtures {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			path := filepath.Join(dir, fmt.Sprintf("%d.ids", n))
			if err := os.WriteFile(path, wire, 0600); err != nil {
				t.Fatal(err)
			}
			want, expectedErr := ackScanRead(path, nil)
			got, err := ackScanRead(path, &buffers)
			if !reflect.DeepEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(expectedErr) {
				t.Fatalf("ACK file state leaked: got %v / %v; want %v / %v", got, err, want, expectedErr)
			}
			if first == nil {
				first = buffers.plain
			}
			if first != buffers.plain || first.reader.Size() != readBufferSize {
				t.Fatal("plaintext reader identity or size changed")
			}
			if first.baseID != -1 || first.word != [8]byte{} || first.reader.Buffered() != 0 {
				t.Fatal("ACK reader retained file state")
			}
		})
	}
	// Leave a complete unread offset and buffered data behind, not just EOF.
	firstPath := filepath.Join(dir, "0.ids")
	stop := errors.New("stop callback")
	err := readIDsFileWithBuffers(firstPath, func(d *IdsDecoder) error {
		if _, err := d.readID(); err != nil {
			return err
		}
		return stop
	}, &buffers)
	if errors.Cause(err) != stop {
		t.Fatal("consumer failure changed", err)
	}
	if first.baseID != -1 || first.reader.Buffered() != 0 {
		t.Fatal("callback error retained unread ACK state")
	}
	got, err := ackScanRead(filepath.Join(dir, "1.ids"), &buffers)
	if err != nil || !reflect.DeepEqual(got, []int64{7, 2, math.MaxInt64}) {
		t.Fatal("unread bytes or prior base survived callback error", got, err)
	}
}

func ackGzipBytes(t *testing.T, wire []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write(wire); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestACKScanGzipChecksumMembersAndEmptyFiles(t *testing.T) {
	var buffers scanIDBuffers
	dir := t.TempDir()
	wire := ackScanWire(100, 101, 99)
	valid := ackGzipBytes(t, wire)
	corrupt := bytes.Clone(valid)
	corrupt[len(corrupt)-8] ^= 1 // valid header, bad member checksum
	members := append(ackGzipBytes(t, wire[:8]), ackGzipBytes(t, wire[8:])...)
	fixtures := []struct {
		name string
		wire []byte
		bad  bool
	}{
		{"plain.ids", ackScanWire(1, 2), false},
		{"empty.gz", nil, false},
		{"header.gz", []byte{0x1f, 0x8b}, true},
		{"bad.gz", corrupt, true},
		{"members.gz", members, false},
		{"tail.gz", valid[:len(valid)-3], true},
		{"valid.gz", valid, false},
		{"empty.ids", nil, false},
		{"newbase.ids", ackScanWire(2, 0), false},
	}
	var first *IdsDecoder
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			path := filepath.Join(dir, fixture.name)
			if err := os.WriteFile(path, fixture.wire, 0600); err != nil {
				t.Fatal(err)
			}
			want, we := ackScanRead(path, nil)
			got, ge := ackScanRead(path, &buffers)
			if !reflect.DeepEqual(got, want) || fmt.Sprint(ge) != fmt.Sprint(we) || (ge != nil) != fixture.bad {
				t.Fatalf("gzip error/member policy changed: %v / %v, want %v / %v", got, ge, want, we)
			}
			if first == nil {
				first = buffers.plain
			}
			if buffers.plain != first || first.gzReader != nil {
				t.Fatal("gzip state entered the plaintext cache")
			}
		})
	}
	for _, path := range []string{dir, filepath.Join(dir, "missing.ids")} {
		_, expected := ackScanRead(path, nil)
		_, got := ackScanRead(path, &buffers)
		if got == nil || fmt.Sprint(got) != fmt.Sprint(expected) {
			t.Fatal("I/O failure policy changed", got, expected)
		}
	}
}

func TestACKOnlyPublicFrontierSurvivesConcurrentScansAndCleanup(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%t", compressed), func(t *testing.T) {
			dir := t.TempDir()
			open := func() *Journal {
				t.Helper()
				j, err := NewJournal(WithBufDirPath(dir), WithIsCompress(compressed), WithIsAggresiveGC(false),
					WithFlushInterval(time.Hour), WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				if err := j.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(j.Close)
				return j
			}
			j := open()
			const high int64 = 1 << 50
			for _, id := range []int64{high, 7, 100, 0, 2} {
				if err := j.WriteId(id); err != nil {
					t.Fatal(err)
				}
				if err := j.Rotate(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			var wg sync.WaitGroup
			for n := 0; n < 8; n++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 8; i++ {
						if got, err := j.LoadMaxId(); got != high || err != nil {
							t.Errorf("wrong ACK-only frontier: %d, %v", got, err)
						}
					}
				}()
			}
			wg.Wait()
			if !j.LockLegacy() {
				t.Fatal("replay lease unavailable")
			}
			var d Data
			if err := j.LoadLegacyBuf(&d); err != io.EOF {
				t.Fatal("ACK-only cleanup was not EOF", err)
			}
			j.UnLockLegacy()
			j.Close()
			j = open()
			if got, err := j.LoadMaxId(); got != high || err != nil {
				t.Fatal("cleanup discarded ACK frontier", got, err)
			}
		})
	}
}

package journal

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

// These tests deliberately inspect the file after Write, before Flush/Close.
// Changing a buffer size must not defer visibility or repair a bad write later.
func writerVisibleBytes(t *testing.T, name string, compressed bool) []byte {
	t.Helper()
	wire, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if !compressed {
		return wire
	}
	r, err := gzip.NewReader(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	plain, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("incomplete gzip member after successful Write:", err)
	}
	return plain
}

func writerNoise(n int) string {
	b := make([]byte, n)
	var state uint32 = 0x91abc823
	for i := range b {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		b[i] = byte(32 + state%95) // valid ASCII, deliberately not a repeated body
	}
	return string(b)
}

func TestWriterBufferRecordBoundaries(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, size := range []int{0, 255, 4095, 4096, 4097, 65535, 65536, 65537, (4 << 20) + 17} {
			t.Run(fmt.Sprintf("gzip=%t/payload=%d", compressed, size), func(t *testing.T) {
				fp, err := os.Create(filepath.Join(t.TempDir(), "data"))
				if err != nil {
					t.Fatal(err)
				}
				defer fp.Close()
				enc, err := NewDataEncoder(fp, compressed)
				if err != nil {
					t.Fatal(err)
				}
				defer enc.Close()
				var want bytes.Buffer
				for i, body := range []string{writerNoise(size), "tail 世界/café"} {
					d := &Data{ID: int64(i + 1), Data: map[string]interface{}{"body": body}}
					// Independent generated encoding, not the live encoder's bytes.
					if err := msgp.Encode(&want, d); err != nil {
						t.Fatal(err)
					}
					if err := enc.Write(d); err != nil {
						t.Fatal(err)
					}
					if got := writerVisibleBytes(t, fp.Name(), compressed); !bytes.Equal(got, want.Bytes()) {
						t.Fatalf("wire differs before Flush/Close after record %d: %d / %d bytes", i, len(got), want.Len())
					}
				}
			})
		}
	}
}

func TestWriterBufferAcknowledgementVisibility(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%t", compressed), func(t *testing.T) {
			fp, err := os.Create(filepath.Join(t.TempDir(), "ids"))
			if err != nil {
				t.Fatal(err)
			}
			defer fp.Close()
			enc, err := NewIdsEncoder(fp, compressed)
			if err != nil {
				t.Fatal(err)
			}
			defer enc.Close()
			var want bytes.Buffer
			for i, id := range []int64{2000, 1999, 0, 1 << 40, (1 << 63) - 1, 2} {
				delta := id
				if i > 0 {
					delta -= 2000
				}
				var word [8]byte
				binary.BigEndian.PutUint64(word[:], uint64(delta))
				want.Write(word[:])
				if err := enc.Write(id); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(writerVisibleBytes(t, fp.Name(), compressed), want.Bytes()) {
					t.Fatal("ACK stream differs before Flush/Close")
				}
			}
		})
	}
}

var errWriterBufferRejected = errors.New("reject fully staged custom value")

type writerRejectedValue struct {
	body  string
	calls *int
}

func (v writerRejectedValue) EncodeMsg(w *msgp.Writer) error {
	*v.calls++
	if err := w.WriteString(v.body); err != nil {
		return err
	}
	return errWriterBufferRejected
}

func TestWriterBufferRejectedLargeEncodingIsAtomic(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%t", compressed), func(t *testing.T) {
			fp, err := os.Create(filepath.Join(t.TempDir(), "data"))
			if err != nil {
				t.Fatal(err)
			}
			defer fp.Close()
			enc, err := NewDataEncoder(fp, compressed)
			if err != nil {
				t.Fatal(err)
			}
			defer enc.Close()
			good := &Data{ID: 1, Data: map[string]interface{}{"body": "before"}}
			if err := enc.Write(good); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(fp.Name())
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			bad := &Data{ID: 2, Data: map[string]interface{}{"body": writerRejectedValue{writerNoise((4 << 20) + 17), &calls}}}
			if err := enc.Write(bad); !errors.Is(err, errWriterBufferRejected) || calls != 1 {
				t.Fatalf("custom encoder calls=%d, error=%v", calls, err)
			}
			after, err := os.ReadFile(fp.Name())
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("encoding rejection changed live file", err)
			}
			if err := enc.Write(good); err != nil {
				t.Fatal("valid retry rejected", err)
			}
			var want bytes.Buffer
			if err := msgp.Encode(&want, good); err != nil {
				t.Fatal(err)
			}
			want.Write(bytes.Clone(want.Bytes()))
			if !bytes.Equal(writerVisibleBytes(t, fp.Name(), compressed), want.Bytes()) {
				t.Fatal("rejected record leaked into retry")
			}
		})
	}
}

type writerPartialFailure struct{ calls int }

func (w *writerPartialFailure) Write(p []byte) (int, error) {
	w.calls++
	return len(p) / 2, io.ErrClosedPipe
}

func TestWriterBufferAppendErrorPoisonsBothPaths(t *testing.T) {
	for _, size := range []int{16, 4096, 65536, (4 << 20) + 17} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			fp, err := os.Create(filepath.Join(t.TempDir(), "data"))
			if err != nil {
				t.Fatal(err)
			}
			defer fp.Close()
			enc, err := NewDataEncoder(fp, false)
			if err != nil {
				t.Fatal(err)
			}
			sink := new(writerPartialFailure)
			enc.writer.Reset(sink) // Keep the constructor's actual buffer size.
			d := &Data{ID: 1, Data: map[string]interface{}{"body": writerNoise(size)}}
			first := enc.Write(d)
			if !errors.Is(first, io.ErrClosedPipe) || sink.calls != 1 {
				t.Fatalf("missing live append failure: calls=%d, error=%v", sink.calls, first)
			}
			for _, err := range []error{enc.Write(d), enc.Flush(), enc.Close()} {
				if err != first || sink.calls != 1 {
					t.Fatal("poisoned stream retried or lost original failure", err)
				}
			}
		})
	}
}

// Construction allocation only: not an fsync or end-to-end latency benchmark.
func BenchmarkWriterPairConstruction(b *testing.B) {
	fp, err := os.Create(filepath.Join(b.TempDir(), "unused"))
	if err != nil {
		b.Fatal(err)
	}
	defer fp.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := NewDataEncoder(fp, false)
		if err != nil {
			b.Fatal(err)
		}
		ids, err := NewIdsEncoder(fp, false)
		if err != nil {
			b.Fatal(err)
		}
		if err := data.Close(); err != nil {
			b.Fatal(err)
		}
		if err := ids.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

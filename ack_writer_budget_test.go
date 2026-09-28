package journal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// These contracts also run against the unchanged baseline. They describe ACK
// wire and visibility behavior, not a required implementation buffer size.
type ackBudgetSink struct {
	bytes.Buffer
	calls    int
	fail     bool
	accepted int
	failure  error
}

func (s *ackBudgetSink) Write(p []byte) (int, error) {
	s.calls++
	if s.fail {
		s.Buffer.Write(p[:s.accepted])
		return s.accepted, s.failure
	}
	return s.Buffer.Write(p)
}

func ackBudgetEncoder(t *testing.T) (*IdsEncoder, *os.File) {
	t.Helper()
	fp, err := os.Create(filepath.Join(t.TempDir(), "ids"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fp.Close() })
	enc, err := NewIdsEncoder(fp, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { enc.Close() })
	return enc, fp
}

func TestACKWriterBudgetScalarCallShape(t *testing.T) {
	enc, _ := ackBudgetEncoder(t)
	sink := new(ackBudgetSink)
	enc.writer.Reset(sink) // Preserve the actual constructor's buffer size.
	ids := []int64{math.MaxInt64, 0, 1, math.MaxInt64 - 1, 19, 19}
	var want bytes.Buffer
	for i, id := range ids {
		delta := id
		if i != 0 {
			delta -= ids[0]
		}
		var word [8]byte
		binary.BigEndian.PutUint64(word[:], uint64(delta))
		want.Write(word[:])
		if err := enc.Write(id); err != nil {
			t.Fatal(err)
		}
		if sink.calls != i+1 || enc.writer.Buffered() != 0 {
			t.Fatal("ACK write did not reach sink exactly once before return")
		}
		if !bytes.Equal(sink.Bytes(), want.Bytes()) {
			t.Fatal("ACK signed-delta wire changed")
		}
	}
	calls, base := sink.calls, enc.baseID
	if enc.Write(-1) == nil || sink.calls != calls || enc.baseID != base {
		t.Fatal("invalid ACK modified output or base")
	}
	if err := enc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.calls != calls {
		t.Fatal("empty flush or close wrote extra bytes")
	}
	if !errors.Is(enc.Write(1), os.ErrClosed) {
		t.Fatal("write after close accepted")
	}
}

func TestACKWriterBudgetPartialFailureRemainsSticky(t *testing.T) {
	for accepted := 0; accepted <= 8; accepted++ {
		for _, failure := range []error{nil, io.ErrClosedPipe} {
			if accepted == 8 && failure == nil {
				continue
			}
			t.Run(fmt.Sprintf("n=%d/error=%v", accepted, failure), func(t *testing.T) {
				enc, _ := ackBudgetEncoder(t)
				sink := &ackBudgetSink{fail: true, accepted: accepted, failure: failure}
				enc.writer.Reset(sink)
				want := failure
				if want == nil {
					want = io.ErrShortWrite
				}
				if err := enc.Write(10); !errors.Is(err, want) {
					t.Fatalf("lost write failure: %v", err)
				}
				before := bytes.Clone(sink.Bytes())
				sink.fail = false
				for _, err := range []error{enc.Write(11), enc.Flush(), enc.Close()} {
					if !errors.Is(err, want) {
						t.Fatalf("lost sticky error: %v", err)
					}
				}
				if sink.calls != 1 || !bytes.Equal(before, sink.Bytes()) {
					t.Fatal("failed ACK stream performed a later write")
				}
			})
		}
	}
}

func TestACKWriterBudgetConcurrentWire(t *testing.T) {
	enc, fp := ackBudgetEncoder(t)
	const workers, records = 8, 64
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for n := 0; n < records; n++ {
				if err := enc.Write(int64(w*records + n)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Plain ACKs must be visible before explicit Flush/Close; decode independently.
	wire, err := os.ReadFile(fp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != workers*records*8 {
		t.Fatalf("ACK length=%d", len(wire))
	}
	base := int64(binary.BigEndian.Uint64(wire[:8]))
	seen := make(map[int64]bool)
	for i := 0; i < len(wire); i += 8 {
		id := int64(binary.BigEndian.Uint64(wire[i : i+8]))
		if i != 0 {
			id += base
		}
		if id < 0 || id >= workers*records || seen[id] {
			t.Fatalf("invented/duplicate ACK %d", id)
		}
		seen[id] = true
	}
}

// Fixed-work mechanism checks, not durable-throughput or production SLO claims.
func BenchmarkACKWriterResources(b *testing.B) {
	for _, name := range []string{"construct-plain", "construct-gzip", "construct-pair", "write-plain"} {
		b.Run(name, func(b *testing.B) {
			fp, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer fp.Close()
			var reusable *IdsEncoder
			if name == "write-plain" {
				reusable, err = NewIdsEncoder(fp, false)
				if err != nil {
					b.Fatal(err)
				}
				defer reusable.Close()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if reusable != nil {
					for n := 0; n < 1024; n++ {
						if err := reusable.Write(int64(n)); err != nil {
							b.Fatal(err)
						}
					}
					continue
				}
				var data *DataEncoder
				if name == "construct-pair" {
					data, err = NewDataEncoder(fp, false)
					if err != nil {
						b.Fatal(err)
					}
				}
				enc, err := NewIdsEncoder(fp, name == "construct-gzip")
				if err != nil {
					b.Fatal(err)
				}
				if err := enc.Close(); err != nil {
					b.Fatal(err)
				}
				if data != nil {
					if err := data.Close(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

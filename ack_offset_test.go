package journal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
)

// Compare with the original ReadFull implementation, not readID/readOffset:
// those helpers are the subject of this experiment.
func referenceACKOffset(r *bufio.Reader, word *[8]byte) (int64, error) {
	if _, err := io.ReadFull(r, word[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(word[:])), nil
}

type offsetReader struct {
	data  []byte
	chunk int
	err   error
	calls int
}

func (r *offsetReader) Read(p []byte) (int, error) {
	r.calls++
	if len(r.data) == 0 {
		if r.err != nil {
			err := r.err
			r.err = nil
			return 0, err
		}
		return 0, io.EOF
	}
	n := copy(p, r.data[:min(len(r.data), r.chunk)])
	r.data = r.data[n:]
	if len(r.data) == 0 && r.err != nil {
		err := r.err
		r.err = nil
		return n, err // pending error may accompany complete buffered words
	}
	return n, nil
}

func compareACKOffsets(t *testing.T, wire []byte, chunk, size int, terminal error) {
	t.Helper()
	a := &offsetReader{data: bytes.Clone(wire), chunk: chunk, err: terminal}
	b := &offsetReader{data: bytes.Clone(wire), chunk: chunk, err: terminal}
	d := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(a, size)}
	r := bufio.NewReaderSize(b, size)
	var word [8]byte
	for i := 0; i < len(wire)/8+4; i++ {
		got, ge := d.readOffset()
		want, we := referenceACKOffset(r, &word)
		if got != want || fmt.Sprint(ge) != fmt.Sprint(we) || a.calls != b.calls || d.reader.Buffered() != r.Buffered() {
			t.Fatalf("ACK offset/state mismatch at %d: %d/%v/%d vs %d/%v/%d", i, got, ge, a.calls, want, we, b.calls)
		}
		// Inspect only buffered bytes: do not cause reads or consume a pending error.
		gb, _ := d.reader.Peek(d.reader.Buffered())
		wb, _ := r.Peek(r.Buffered())
		if !bytes.Equal(gb, wb) || !bytes.Equal(a.data, b.data) {
			t.Fatal("ACK offset consumed a different suffix")
		}
	}
}

func TestACKOffsetExactReadsAndErrors(t *testing.T) {
	wire := make([]byte, 65552)
	for i := range wire {
		wire[i] = byte(i*37 + 19)
	}
	for _, length := range []int{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 63, 64, 65535, 65536, 65537, 65552} {
		for _, size := range []int{16, 17, 31, 65536} {
			for _, chunk := range []int{1, 7, 8, 17, 65536} {
				for _, end := range []error{io.EOF, io.ErrUnexpectedEOF, fmt.Errorf("device failure")} {
					compareACKOffsets(t, wire[:length], chunk, size, end)
				}
			}
		}
	}
}

type offsetCallback struct {
	values []int64
	stop   bool
}

func (s *offsetCallback) Add(id int) { s.AddInt64(int64(id)) }
func (s *offsetCallback) AddInt64(id int64) {
	s.values = append(s.values, id)
	if s.stop && len(s.values) == 2 {
		panic("consumer stopped")
	}
}
func (s *offsetCallback) GetLen() int               { return len(s.values) }
func (s *offsetCallback) CheckAndRemove(int64) bool { return false }

func TestACKOffsetConsumesBeforeConsumerAndPreservesTail(t *testing.T) {
	var wire []byte
	for _, word := range []uint64{97, 1, 2, 3} {
		wire = binary.BigEndian.AppendUint64(wire, word)
	}
	d := &IdsDecoder{baseID: -1, reader: bufio.NewReader(bytes.NewReader(wire))}
	set := &offsetCallback{stop: true}
	func() {
		defer func() {
			if got := recover(); got != "consumer stopped" {
				t.Fatalf("missing intended consumer stop: %v", got)
			}
		}()
		_ = d.ReadAllToInt64Set(set)
	}()
	if fmt.Sprint(set.values) != "[97 98]" {
		t.Fatal("callback sequence changed")
	}
	rest := &offsetCallback{}
	if err := d.ReadAllToInt64Set(rest); err != nil || fmt.Sprint(rest.values) != "[99 100]" {
		t.Fatalf("callback did not consume exactly its own record: %v %v", rest.values, err)
	}
}

func FuzzACKOffsetMatchesReadFull(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 97, 0, 1, 2, 3, 4, 5, 6, 7, 255}, uint8(17), uint8(31))
	f.Fuzz(func(t *testing.T, wire []byte, chunk, size uint8) {
		if len(wire) > 8192 {
			return
		}
		compareACKOffsets(t, wire, int(chunk)+1, int(size)+16, io.ErrUnexpectedEOF)
	})
}

func TestACKOffsetBufferedWordAdvances(t *testing.T) {
	wire := binary.BigEndian.AppendUint64(nil, 97)
	wire = binary.BigEndian.AppendUint64(wire, 11)
	d := &IdsDecoder{baseID: -1, reader: bufio.NewReader(bytes.NewReader(wire))}
	if _, err := d.reader.Peek(len(wire)); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int64{97, 11} {
		got, err := d.readOffset()
		if err != nil || got != want {
			t.Fatalf("buffered ACK cursor did not advance: %d/%v want %d", got, err, want)
		}
	}
}

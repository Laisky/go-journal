package journal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
)

func ackWordBytes(deltas ...int64) []byte {
	wire := make([]byte, len(deltas)*8)
	for i, delta := range deltas {
		binary.BigEndian.PutUint64(wire[8*i:], uint64(delta))
	}
	return wire
}

// Deliberately word-at-a-time, without the production helper or readID. Limit
// to the complete words already buffered so cursor/error behavior is comparable.
func referenceBufferedACKMaximum(r *bufio.Reader, base int64) (int64, error) {
	words := r.Buffered() / 8
	var maximum int64
	for i := 0; i < words; i++ {
		var word [8]byte
		if _, err := io.ReadFull(r, word[:]); err != nil {
			return 0, err
		}
		id := int64(binary.BigEndian.Uint64(word[:])) + base
		if id < 0 {
			return 0, errors.New("acknowledgement ID underflow or overflow")
		}
		if id > maximum {
			maximum = id
		}
	}
	return maximum, nil
}

type ackTerminalReader struct {
	wire  []byte
	err   error
	reads int
}

func (r *ackTerminalReader) Read(p []byte) (int, error) {
	r.reads++
	n := copy(p, r.wire)
	r.wire = r.wire[n:]
	if len(r.wire) == 0 {
		return n, r.err
	}
	return n, nil
}

func checkACKBuffered(t *testing.T, wire []byte, base int64, size int, terminal error) {
	t.Helper()
	x := &ackTerminalReader{wire: bytes.Clone(wire), err: terminal}
	y := &ackTerminalReader{wire: bytes.Clone(wire), err: terminal}
	a, b := bufio.NewReaderSize(x, size), bufio.NewReaderSize(y, size)
	// Prime once. In real LoadMaxId this happens while reading the first word.
	a.Peek(1)
	b.Peek(1)
	reads := x.reads
	got, ge := bufferedACKMaximum(a, base)
	want, we := referenceBufferedACKMaximum(b, base)
	if got != want || fmt.Sprint(ge) != fmt.Sprint(we) || a.Buffered() != b.Buffered() {
		t.Fatalf("value/error/cursor: (%d,%v,%d), want (%d,%v,%d)", got, ge, a.Buffered(), want, we, b.Buffered())
	}
	if x.reads != reads {
		t.Fatal("buffered scan performed read-ahead")
	}
	ga, gae := io.ReadAll(a)
	wb, wbe := io.ReadAll(b)
	if !bytes.Equal(ga, wb) || !errors.Is(gae, wbe) || x.reads != y.reads {
		t.Fatalf("changed remaining stream/error: %x %v / %x %v", ga, gae, wb, wbe)
	}
}

func TestACKBufferedMaximumBoundaries(t *testing.T) {
	for _, base := range []int64{0, 1, 900, math.MaxInt64 / 2, math.MaxInt64} {
		for _, deltas := range [][]int64{
			{}, {0}, {0, 1, -1, 4}, {math.MinInt64, 7}, {0, math.MaxInt64, 8},
			{4, -5, 6}, {0, 0, 0}, {3, 2, 1},
		} {
			for _, size := range []int{16, 17, 31, 64, 65536} {
				for tail := 0; tail < 8; tail++ {
					wire := append(ackWordBytes(deltas...), bytes.Repeat([]byte{0x3a}, tail)...)
					for _, terminal := range []error{io.EOF, io.ErrUnexpectedEOF, errors.New("device failure")} {
						checkACKBuffered(t, wire, base, size, terminal)
					}
				}
			}
		}
	}
}

func TestACKBufferedMaximumRetainsInvalidSuffix(t *testing.T) {
	r := bufio.NewReader(bytes.NewReader(ackWordBytes(4, -6, 9)))
	r.Peek(1)
	_, err := bufferedACKMaximum(r, 5)
	if err == nil {
		t.Fatal("invalid ACK delta accepted")
	}
	remaining, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(remaining, ackWordBytes(9)) {
		t.Fatal("invalid-word boundary lost")
	}
}

func TestACKBufferedMaximumNoAllocations(t *testing.T) {
	wire := ackWordBytes(2, 1, 0, 3)
	source := bytes.NewReader(wire)
	r := bufio.NewReaderSize(source, 64)
	var high int64
	if allocations := testing.AllocsPerRun(100, func() {
		source.Reset(wire)
		r.Reset(source)
		r.Peek(1)
		high, _ = bufferedACKMaximum(r, 10)
	}); allocations != 0 {
		t.Fatalf("allocations = %v", allocations)
	}
	if high != 13 {
		t.Fatal(high)
	}
}

func FuzzACKBufferedMaximum(f *testing.F) {
	f.Add(ackWordBytes(1, -1, math.MaxInt64), uint64(10), uint8(31))
	f.Add([]byte{0, 1, 2}, uint64(0), uint8(16))
	f.Fuzz(func(t *testing.T, wire []byte, base uint64, size uint8) {
		if len(wire) > 64<<10 {
			return
		}
		checkACKBuffered(t, wire, int64(base&math.MaxInt64), int(size)+16, io.EOF)
	})
}

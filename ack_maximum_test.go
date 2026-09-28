package journal

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/pkg/errors"
)

// The reference deliberately retains the accepted word-at-a-time path. Tests
// compare values, error classes/text, fixed base and the exact unread suffix.
func scalarACKMaximum(d *IdsDecoder) (int64, error) {
	var high int64
	for {
		id, err := d.readID()
		if err == io.EOF {
			return high, nil
		}
		if err != nil {
			return 0, errors.Wrap(err, "read ids")
		}
		if id > high {
			high = id
		}
	}
}

func compareACKMaximum(t *testing.T, a, b *IdsDecoder) {
	t.Helper()
	got, ge := a.LoadMaxId()
	want, we := scalarACKMaximum(b)
	if got != want || fmt.Sprint(ge) != fmt.Sprint(we) ||
		fmt.Sprint(errors.Cause(ge)) != fmt.Sprint(errors.Cause(we)) || a.baseID != b.baseID {
		t.Fatalf("ACK maximum differs: %d/%v/base=%d vs %d/%v/base=%d", got, ge, a.baseID, want, we, b.baseID)
	}
	ga, gae := io.ReadAll(a.reader)
	wb, wbe := io.ReadAll(b.reader)
	if !bytes.Equal(ga, wb) || fmt.Sprint(gae) != fmt.Sprint(wbe) {
		t.Fatalf("ACK maximum consumed suffix or error: %x/%v vs %x/%v", ga, gae, wb, wbe)
	}
}

func TestACKMaximumMatchesScalar(t *testing.T) {
	fixtures := [][]byte{nil, ackScanWire(0), ackScanWire(90, 1, 99, 0),
		ackScanWire(math.MaxInt64, 0, 10), ackWordBytes(-1, 10),
		ackWordBytes(5, 2, -6, 9), ackWordBytes(math.MaxInt64, 1, -2)}
	many := make([]int64, 20000)
	for i := range many {
		many[i] = int64(len(many) - i)
	}
	fixtures = append(fixtures, ackScanWire(many...))
	for size := 1; size <= 7; size++ {
		fixtures = append(fixtures, append(ackScanWire(3, 4), make([]byte, size)...))
	}
	for _, wire := range fixtures {
		for _, size := range []int{16, 17, 31, 4096, readBufferSize} {
			for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, errors.New("read device failure")} {
				a := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(&ackTerminalReader{wire: bytes.Clone(wire), err: err}, size)}
				b := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(&ackTerminalReader{wire: bytes.Clone(wire), err: err}, size)}
				compareACKMaximum(t, a, b)
			}
		}
	}
}

func TestACKMaximumGzipChecksumAndMembersMatchScalar(t *testing.T) {
	wire := ackScanWire(900, 1, 9000, 0)
	good := ackGzipBytes(t, wire)
	bad := bytes.Clone(good)
	bad[len(bad)-8] ^= 1
	multi := append(ackGzipBytes(t, wire[:8]), ackGzipBytes(t, wire[8:])...)
	for _, input := range [][]byte{good, bad, multi, good[:len(good)-3]} {
		x, e := gzip.NewReader(bytes.NewReader(input))
		if e != nil {
			t.Fatal(e)
		}
		y, e := gzip.NewReader(bytes.NewReader(input))
		if e != nil {
			t.Fatal(e)
		}
		a := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(x, 16), gzReader: x}
		b := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(y, 16), gzReader: y}
		compareACKMaximum(t, a, b)
		x.Close()
		y.Close()
	}
}

func FuzzACKMaximumMatchesScalar(f *testing.F) {
	f.Add(ackWordBytes(5, 2, -6, 9), uint8(16))
	f.Add(ackWordBytes(99, -90, 100), uint8(31))
	f.Fuzz(func(t *testing.T, wire []byte, size uint8) {
		if len(wire) > 64<<10 {
			return
		}
		a := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(bytes.NewReader(wire), int(size)+16)}
		b := &IdsDecoder{baseID: -1, reader: bufio.NewReaderSize(bytes.NewReader(wire), int(size)+16)}
		compareACKMaximum(t, a, b)
	})
}

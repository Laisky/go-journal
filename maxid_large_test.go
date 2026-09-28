package journal

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/tinylib/msgp/msgp"
)

// The original differential suite used 64 KiB readers. Exercise records above
// the old 128 KiB inspection cap while fully buffered, crossing read buffers,
// and larger than every read buffer. Generated decoding remains the oracle.
func TestIDScanLargeBufferBoundariesMatchGenerated(t *testing.T) {
	for _, size := range []int{(128 << 10) - 1, (128 << 10) + 17, (256 << 10) + 17, (1 << 20) + 17, (4 << 20) + 17} {
		first := maxIDWire(-31, string(bytes.Repeat([]byte("x"), size)))
		missingID := msgp.AppendMapHeader(nil, 1)
		missingID = msgp.AppendString(missingID, "Data")
		missingID = msgp.AppendMapHeader(missingID, 0)
		wire := append(bytes.Clone(first), missingID...)
		wire = append(wire, maxIDWire(99, "tail 世界")...)
		for _, buffer := range []int{64 << 10, 1 << 20, 4 << 20} {
			for _, trim := range []int{0, 1, 7} {
				t.Run(fmt.Sprintf("payload=%d/buffer=%d/trim=%d", size, buffer, trim), func(t *testing.T) {
					source := wire[:len(wire)-trim]
					a := &DataDecoder{reader: msgp.NewReaderSize(bytes.NewReader(source), buffer)}
					b := &DataDecoder{reader: msgp.NewReaderSize(bytes.NewReader(source), buffer)}
					for step := 0; step < 5; step++ {
						want, we := scanReference(a)
						got, ge := b.readRecordID()
						if (we == nil) != (ge == nil) || (we == io.EOF) != (ge == io.EOF) {
							t.Fatalf("error boundary: %v / %v", ge, we)
						}
						if we != nil {
							if incompleteRecord(we) != incompleteRecord(ge) {
								t.Fatalf("tail policy: %v / %v", ge, we)
							}
							return
						}
						if got != want {
							t.Fatalf("ID: %d / %d", got, want)
						}
					}
					t.Fatal("reader failed to finish")
				})
			}
		}
	}
}

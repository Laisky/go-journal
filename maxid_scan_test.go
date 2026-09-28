package journal

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
)

func maxIDWire(id int64, payload string) []byte {
	b := msgp.AppendMapHeader(nil, 2)
	b = msgp.AppendString(b, "Data")
	b = msgp.AppendMapHeader(b, 1)
	b = msgp.AppendString(b, "body")
	b = msgp.AppendString(b, payload)
	b = msgp.AppendString(b, "ID")
	return msgp.AppendInt64(b, id)
}
func scanReference(r *DataDecoder) (int64, error) { var d Data; err := r.Read(&d); return d.ID, err }
func compareIDReaders(t testing.TB, wire []byte, fragment int) {
	t.Helper()
	reader := func() *DataDecoder {
		var src io.Reader = bytes.NewReader(wire)
		if fragment > 0 {
			src = &idFragmentReader{r: src, n: fragment}
		}
		return &DataDecoder{reader: msgp.NewReaderSize(src, 64<<10)}
	}
	a, b := reader(), reader()
	for i := 0; i <= len(wire)+1; i++ {
		want, we := scanReference(a)
		got, ge := b.readRecordID()
		if (we == nil) != (ge == nil) || (we == io.EOF) != (ge == io.EOF) {
			t.Fatalf("error boundary differs: got %v, want %v", ge, we)
		}
		if we != nil {
			if incompleteRecord(we) != incompleteRecord(ge) {
				t.Fatalf("incomplete-tail policy differs: %v / %v", ge, we)
			}
			return
		}
		if got != want {
			t.Fatalf("ID differs: got %d want %d", got, want)
		}
	}
	t.Fatal("reader did not finish")
}

type idFragmentReader struct {
	r io.Reader
	n int
}

func (r *idFragmentReader) Read(b []byte) (int, error) {
	if len(b) > r.n {
		b = b[:r.n]
	}
	return r.r.Read(b)
}

func TestIDScanGeneratedDecoderEquivalence(t *testing.T) {
	canonical := maxIDWire(91, "café 世界")
	missingID := msgp.AppendMapHeader(nil, 1)
	missingID = msgp.AppendString(missingID, "Data")
	missingID = msgp.AppendMapHeader(missingID, 0)
	missingData := msgp.AppendMapHeader(nil, 1)
	missingData = msgp.AppendString(missingData, "ID")
	missingData = msgp.AppendInt64(missingData, 92)
	malformed := msgp.AppendMapHeader(nil, 2)
	malformed = msgp.AppendString(malformed, "Data")
	malformed = msgp.AppendMapHeader(malformed, 1)
	malformed = msgp.AppendInt64(malformed, 7)
	malformed = msgp.AppendString(malformed, "bad key")
	malformed = msgp.AppendString(malformed, "ID")
	malformed = msgp.AppendInt64(malformed, 99)
	for _, fragment := range []int{0, 1, 7, 1000} {
		for _, wire := range [][]byte{canonical, append(bytes.Clone(canonical), missingID...), append(bytes.Clone(canonical), missingData...), append(bytes.Clone(canonical), malformed...), maxIDWire(123, string(bytes.Repeat([]byte("x"), 256<<10)))} {
			compareIDReaders(t, wire, fragment)
		}
		for end := 0; end <= len(canonical); end++ {
			compareIDReaders(t, canonical[:end], fragment)
		}
	}
}

func TestIDScanPreservesPublicRecoveryAndACKs(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "gzip"}[compressed], func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()
			open := func() *Journal {
				t.Helper()
				j, e := NewJournal(WithBufDirPath(dir), WithIsCompress(compressed), WithIsAggresiveGC(false), WithRotateCheckInterval(time.Hour))
				if e != nil {
					t.Fatal(e)
				}
				if e = j.Start(ctx); e != nil {
					t.Fatal(e)
				}
				return j
			}
			first := open()
			for _, id := range []int64{900, 1, 40, 2} {
				if e := first.WriteData(&Data{ID: id, Data: map[string]interface{}{"text": "owned 世界", "id": id}}); e != nil {
					t.Fatal(e)
				}
			}
			if e := first.Sync(); e != nil {
				t.Fatal(e)
			}
			for _, id := range []int64{900, 2} {
				if e := first.WriteId(id); e != nil {
					t.Fatal(e)
				}
			}
			if e := first.Sync(); e != nil {
				t.Fatal(e)
			}
			first.Close()
			second := open()
			defer second.Close()
			for n := 0; n < 3; n++ {
				high, e := second.LoadMaxId()
				if e != nil || high != 900 {
					t.Fatalf("frontier %d %v", high, e)
				}
			}
			if !second.LockLegacy() {
				t.Fatal("lease")
			}
			defer second.UnLockLegacy()
			var ids []int64
			for {
				d := new(Data)
				e := second.LoadLegacyBuf(d)
				if e == io.EOF {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
				ids = append(ids, d.ID)
				if d.Data["text"] != "owned 世界" {
					t.Fatal("changed payload")
				}
				if e = second.WriteData(d); e != nil {
					t.Fatal(e)
				}
				if e = second.Sync(); e != nil {
					t.Fatal(e)
				}
			}
			if !reflect.DeepEqual(ids, []int64{1, 40}) {
				t.Fatalf("scan consumed ACKs or skipped pending records: %v", ids)
			}
		})
	}
}

func TestIDScanRejectsMalformedPayloadAndGzipChecksum(t *testing.T) {
	invalid := msgp.AppendMapHeader(nil, 2)
	invalid = msgp.AppendString(invalid, "ID")
	invalid = msgp.AppendInt64(invalid, 99)
	invalid = msgp.AppendString(invalid, "Data")
	invalid = msgp.AppendMapHeader(invalid, 1)
	invalid = msgp.AppendInt64(invalid, 7)
	invalid = msgp.AppendString(invalid, "not a string key")
	for _, badChecksum := range []bool{false, true} {
		wire := invalid
		name := "20260927_00000001.buf"
		if badChecksum {
			var b bytes.Buffer
			z := gzip.NewWriter(&b)
			if _, e := z.Write(maxIDWire(99, "ok")); e != nil {
				t.Fatal(e)
			}
			if e := z.Close(); e != nil {
				t.Fatal(e)
			}
			wire = b.Bytes()
			wire[len(wire)-8] ^= 1
			name += ".gz"
		}
		path := filepath.Join(t.TempDir(), name)
		if e := os.WriteFile(path, wire, 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := maxDataID(path, true); e == nil {
			t.Fatalf("accepted malformed payload/checksum: gzip=%v", badChecksum)
		}
		if _, e := os.Stat(path); e != nil {
			t.Fatal("scan removed corrupt source")
		}
	}
}

func FuzzIDScanMatchesGenerated(f *testing.F) {
	f.Add(maxIDWire(91, "seed 世界"))
	f.Add([]byte{0x82, 0xa2, 'I', 'D', 0x01})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, wire []byte) {
		if len(wire) > 4096 {
			return
		}
		// Avoid allocation bombs in the old decoder used as the differential oracle.
		// The shared conservative validator rejects unsupported/corrupt fuzz shapes;
		// malformed framing and typed-map negatives are covered deterministically.
		rest := wire
		for len(rest) > 0 {
			_, size, ok := inspectReplayRecord(rest)
			if !ok {
				return
			}
			rest = rest[size:]
		}
		compareIDReaders(t, wire, 0)
	})
}

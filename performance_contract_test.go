package journal_test

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	journal "github.com/Laisky/go-journal"
)

// These contracts deliberately exceed both the proposed read buffer and the
// original buffer. Buffer tuning must not change record size limits or bytes.
func TestPerformanceContractLargeRecords(t *testing.T) {
	for _, zipped := range []bool{false, true} {
		t.Run(fmt.Sprint(zipped), func(t *testing.T) {
			fp, err := os.CreateTemp(t.TempDir(), "records")
			if err != nil { t.Fatal(err) }
			defer fp.Close()
			enc, err := journal.NewDataEncoder(fp, zipped)
			if err != nil { t.Fatal(err) }
			var want []*journal.Data
			rng := rand.New(rand.NewSource(127))
			for i, size := range []int{0, 65535, 65536, 65537, (4 << 20) + 17} {
				body := make([]byte, size)
				_, _ = rng.Read(body)
				d := &journal.Data{ID: int64(i + 1), Data: map[string]interface{}{"body": body, "label": fmt.Sprint(i)}}
				if err = enc.Write(d); err != nil { t.Fatal(err) }
				want = append(want, d)
			}
			if err = enc.Close(); err != nil { t.Fatal(err) }
			if _, err = fp.Seek(0, io.SeekStart); err != nil { t.Fatal(err) }
			dec, err := journal.NewDataDecoder(fp, zipped)
			if err != nil { t.Fatal(err) }
			for _, expected := range want {
				var got journal.Data
				if err = dec.Read(&got); err != nil { t.Fatal(err) }
				if !reflect.DeepEqual(expected, &got) { t.Fatalf("record %d corrupted across buffer boundary", expected.ID) }
			}
			var extra journal.Data
			if err = dec.Read(&extra); err != io.EOF { t.Fatalf("expected EOF, got %v", err) }
		})
	}
}

type collectedIDs []int64
func (s *collectedIDs) Add(i int) { s.AddInt64(int64(i)) }
func (s *collectedIDs) AddInt64(i int64) { *s = append(*s, i) }
func (s *collectedIDs) GetLen() int { return len(*s) }
func (s *collectedIDs) CheckAndRemove(i int64) bool { panic("unexpected destructive lookup") }

func TestPerformanceContractIDsAcrossBuffers(t *testing.T) {
	for _, zipped := range []bool{false, true} {
		t.Run(fmt.Sprint(zipped), func(t *testing.T) {
			fp, err := os.CreateTemp(t.TempDir(), "ids")
			if err != nil { t.Fatal(err) }
			defer fp.Close()
			enc, err := journal.NewIdsEncoder(fp, zipped)
			if err != nil { t.Fatal(err) }
			ids := []int64{7000000000000, 0, 1, 9223372036854775807, 7000000000000}
			for i := 0; i < 10000; i++ { ids = append(ids, int64(i%997)) }
			var expected bytes.Buffer
			for i, id := range ids {
				var word [8]byte
				offset := id
				if i > 0 { offset -= ids[0] }
				binary.BigEndian.PutUint64(word[:], uint64(offset))
				expected.Write(word[:])
				if err = enc.Write(id); err != nil { t.Fatal(err) }
			}
			if err = enc.Close(); err != nil { t.Fatal(err) }
			if _, err = fp.Seek(0, 0); err != nil { t.Fatal(err) }
			var reader io.Reader = fp
			if zipped {
				gz, e := gzip.NewReader(fp)
				if e != nil { t.Fatal(e) }
				defer gz.Close()
				reader = gz
			}
			raw, err := io.ReadAll(reader)
			if err != nil { t.Fatal(err) }
			if !bytes.Equal(raw, expected.Bytes()) { t.Fatal("ID encoding changed") }
			if _, err = fp.Seek(0, 0); err != nil { t.Fatal(err) }
			dec, err := journal.NewIdsDecoder(fp, zipped)
			if err != nil { t.Fatal(err) }
			var got collectedIDs
			if err = dec.ReadAllToInt64Set(&got); err != nil { t.Fatal(err) }
			if !reflect.DeepEqual([]int64(got), ids) { t.Fatal("ID ordering/value changed") }
			if _, err = fp.Seek(0, 0); err != nil { t.Fatal(err) }
			dec, err = journal.NewIdsDecoder(fp, zipped)
			if err != nil { t.Fatal(err) }
			high, err := dec.LoadMaxId()
			if err != nil || high != 9223372036854775807 { t.Fatalf("high-water %d: %v", high, err) }
		})
	}
}

func TestPerformanceContractTruncatedID(t *testing.T) {
	for tail := 1; tail < 8; tail++ {
		t.Run(fmt.Sprint(tail), func(t *testing.T) {
			fp, err := os.CreateTemp(t.TempDir(), "partial")
			if err != nil { t.Fatal(err) }
			defer fp.Close()
			var raw [16]byte
			binary.BigEndian.PutUint64(raw[:8], 91)
			binary.BigEndian.PutUint64(raw[8:], 2)
			if _, err = fp.Write(raw[:8+tail]); err != nil { t.Fatal(err) }
			if _, err = fp.Seek(0, 0); err != nil { t.Fatal(err) }
			dec, err := journal.NewIdsDecoder(fp, false)
			if err != nil { t.Fatal(err) }
			var got collectedIDs
			if err = dec.ReadAllToInt64Set(&got); err == nil { t.Fatal("partial ID accepted as EOF") }
			if !reflect.DeepEqual([]int64(got), []int64{91}) { t.Fatalf("valid prefix changed: %v", got) }
		})
	}
}

package journal_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"

	journal "github.com/Laisky/go-journal"
)

// A validation sink isolates decoding/interface dispatch, not set allocation.
// The existing ACKReplay benchmarks separately exercise real TTL membership.
type ackOffsetSink struct {
	seen int64
	b    *testing.B
}

func (s *ackOffsetSink) Add(v int) { s.AddInt64(int64(v)) }
func (s *ackOffsetSink) AddInt64(v int64) {
	if v != 97+s.seen {
		s.b.Fatalf("changed ACK at %d: %d", s.seen, v)
	}
	s.seen++
}
func (s *ackOffsetSink) GetLen() int               { return int(s.seen) }
func (s *ackOffsetSink) CheckAndRemove(int64) bool { return false }

// Each operation opens and exhausts a real sealed ACK file and validates every
// ID. Construction and file open/close are included; fixture creation is not.
// These are warm-file decoding batches, not newly durable message deliveries.
func BenchmarkPublicACKOffset(b *testing.B) {
	for _, c := range []struct {
		name   string
		count  int
		gzip   bool
		bitmap bool
	}{
		{"sink-8k", 8192, false, false},
		{"sink-128k", 131072, false, false},
		{"bitmap-128k", 131072, false, true},
		{"gzip-8k", 8192, true, false},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.StopTimer()
			wire := make([]byte, c.count*8)
			for i := 0; i < c.count; i++ {
				v := uint64(i)
				if i == 0 {
					v = 97
				}
				binary.BigEndian.PutUint64(wire[i*8:], v)
			}
			if c.gzip {
				var compressed bytes.Buffer
				z := gzip.NewWriter(&compressed)
				if _, err := z.Write(wire); err != nil {
					b.Fatal(err)
				}
				if err := z.Close(); err != nil {
					b.Fatal(err)
				}
				wire = compressed.Bytes()
			}
			path := filepath.Join(b.TempDir(), "ids")
			fp, err := os.Create(path)
			if err != nil {
				b.Fatal(err)
			}
			if _, err = fp.Write(wire); err != nil {
				b.Fatal(err)
			}
			if err = fp.Sync(); err != nil {
				b.Fatal(err)
			}
			if err = fp.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			pprof.Do(context.Background(), pprof.Labels("phase", "public-ack-offset"), func(context.Context) {
				b.StartTimer()
				for n := 0; n < b.N; n++ {
					f, e := os.Open(path)
					if e != nil {
						b.Fatal(e)
					}
					d, e := journal.NewIdsDecoder(f, c.gzip)
					if e == nil && c.bitmap {
						bitmap, err := d.ReadAllToBmap()
						e = err
						if e == nil {
							if bitmap.GetCardinality() != uint64(c.count) {
								e = fmt.Errorf("wrong bitmap cardinality")
							} else {
								for i := 0; i < c.count; i++ {
									if !bitmap.Contains(uint32(97 + i)) {
										e = fmt.Errorf("bitmap lost ACK %d", i)
										break
									}
								}
							}
						}
					} else if e == nil {
						sink := &ackOffsetSink{b: b}
						e = d.ReadAllToInt64Set(sink)
						if e == nil && sink.seen != int64(c.count) {
							e = fmt.Errorf("missing ACKs: %d", sink.seen)
						}
					}
					closed := f.Close()
					if e != nil || closed != nil {
						b.Fatalf("decode/close: %v / %v", e, closed)
					}
				}
				b.StopTimer()
			})
		})
	}
}

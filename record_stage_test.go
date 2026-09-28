package journal

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordStageBoundariesAndReuse(t *testing.T) {
	s := newRecordStage()
	for _, size := range []int{0, 1, 4096, 65536, 128 << 10, 256 << 10, BufSize - 1, BufSize, BufSize + 1, 2*BufSize + 17, 19} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s.Reset()
			if cap(s.buf) != BufSize || len(s.buf) != 0 {
				t.Fatal("retained overflow storage")
			}
			want := strings.Repeat("x", size)
			if n, err := io.WriteString(&s, want); err != nil || n != size {
				t.Fatal(n, err)
			}
			if n, err := s.Write([]byte("tail 世界")); err != nil || n != len("tail 世界") {
				t.Fatal(n, err)
			}
			if string(s.Bytes()) != want+"tail 世界" {
				t.Fatal("wrong staged bytes")
			}
			if size <= BufSize-len("tail 世界") && &s.buf[:cap(s.buf)][0] != &s.arena[0] {
				t.Fatal("lost arena")
			}
		})
	}
	s.Reset()
	if cap(s.buf) != BufSize {
		t.Fatal("overflow not released")
	}
}

func TestRecordStageFragmentedAndBorrowedInput(t *testing.T) {
	s := newRecordStage()
	var want bytes.Buffer
	part := bytes.Repeat([]byte("x"), 4096)
	for i := 0; i < 3*BufSize/len(part)+3; i++ {
		part[0] = byte(i)
		want.Write(part)
		if _, err := s.Write(part); err != nil {
			t.Fatal(err)
		}
	}
	clear(part)
	if !bytes.Equal(s.Bytes(), want.Bytes()) {
		t.Fatal("retained caller slice or changed stream")
	}
	s.Reset()
	if s.Len() != 0 || cap(s.buf) != BufSize {
		t.Fatal("fragmented overflow retained")
	}
}

func TestRecordStageNoWarmAllocationsAndBoundedOverflow(t *testing.T) {
	s := newRecordStage()
	value := strings.Repeat("x", 256<<10)
	if n := testing.AllocsPerRun(20, func() { s.Reset(); s.WriteString(value); s.WriteString("tail") }); n != 0 {
		t.Fatal("warm staging allocations", n)
	}
	s.Reset()
	s.WriteString(strings.Repeat("y", BufSize+17))
	if cap(s.buf) > BufSize+17+4096 {
		t.Fatal("first overflow doubled arena", cap(s.buf))
	}
	s.Reset()
}

// A prepared payload and /dev/null isolate serialization, not durability or
// E2E throughput. Exactly the same benchmark is compiled against both versions.
func BenchmarkRecordStagingWrites(b *testing.B) {
	for _, size := range []int{1024, 65536, 262144, 1048576, BufSize + 17} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			fp, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				b.Fatal(err)
			}
			defer fp.Close()
			enc, err := NewDataEncoder(fp, false)
			if err != nil {
				b.Fatal(err)
			}
			defer enc.Close()
			d := &Data{ID: 1, Data: map[string]interface{}{"body": writerNoise(size)}}
			if err = enc.Write(d); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err = enc.Write(d); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Encoding can flush many private fragments, then reject. No fragment may
// escape to the live journal. The same body must be accepted on a later retry.
func TestRecordStageLargeCustomRejectionThenRetry(t *testing.T) {
	for _, size := range []int{256 << 10, BufSize - 17, BufSize + 17} {
		for _, gzip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/gzip=%t", size, gzip), func(t *testing.T) {
				fp, err := os.Create(filepath.Join(t.TempDir(), "data"))
				if err != nil {
					t.Fatal(err)
				}
				defer fp.Close()
				enc, err := NewDataEncoder(fp, gzip)
				if err != nil {
					t.Fatal(err)
				}
				defer enc.Close()
				calls := 0
				bad := &Data{ID: 1, Data: map[string]interface{}{"body": writerRejectedValue{writerNoise(size), &calls}}}
				if err = enc.Write(bad); err == nil || calls != 1 {
					t.Fatal("rejection semantics", err, calls)
				}
				info, err := fp.Stat()
				if err != nil || info.Size() != 0 {
					t.Fatal("rejection reached live writer", err)
				}
				good := &Data{ID: 2, Data: map[string]interface{}{"body": "retry"}}
				if err = enc.Write(good); err != nil {
					t.Fatal(err)
				}
				if err = enc.Flush(); err != nil {
					t.Fatal(err)
				}
				wire := writerVisibleBytes(t, fp.Name(), gzip)
				var got Data
				rest, err := got.UnmarshalMsg(wire)
				if err != nil || len(rest) != 0 || got.ID != 2 || got.Data["body"] != "retry" {
					t.Fatal("changed retry", got, err)
				}
			})
		}
	}
}

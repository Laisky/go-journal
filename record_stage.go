package journal

import (
	"bytes"
)

// recordStage owns the memory previously spent on DataEncoder's 4 MiB output
// buffer. Complete records are staged here before any live append. The fixed
// arena is private to one encoder and protected by its mutex; no caller memory
// is borrowed, and overflow storage is never retained between records.
//
// Keep the live heap budget rather than merely shrinking the output buffer:
// that earlier experiment saved construction bytes but increased GC work.
type recordStage struct {
	arena []byte
	buf   []byte
}

func newRecordStage() recordStage {
	arena := make([]byte, BufSize)
	return recordStage{arena: arena, buf: arena[:0]}
}

func (s *recordStage) Reset()        { s.buf = s.arena[:0] }
func (s *recordStage) Bytes() []byte { return s.buf }
func (s *recordStage) Len() int      { return len(s.buf) }

// reserve expands only when a record exceeds the fixed arena. First overflow
// is sized to the request plus a small trailer allowance: doubling a 4 MiB
// arena for a 4 MiB+header record would introduce a new allocation regression.
// Further growth is amortized for custom encoders that stream many small chunks.
func (s *recordStage) reserve(n int) []byte {
	old := len(s.buf)
	const maxInt = int(^uint(0) >> 1)
	if n < 0 || n > maxInt-old {
		panic(bytes.ErrTooLarge)
	}
	need := old + n
	if need > cap(s.buf) {
		size := need
		if cap(s.buf) > cap(s.arena) && cap(s.buf) <= maxInt/2 && size < 2*cap(s.buf) {
			size = 2 * cap(s.buf)
		}
		if size <= maxInt-4096 {
			size += 4096
		}
		grown := make([]byte, old, size)
		copy(grown, s.buf)
		s.buf = grown
	}
	s.buf = s.buf[:need]
	return s.buf[old:]
}

func (s *recordStage) Write(p []byte) (int, error) {
	copy(s.reserve(len(p)), p)
	return len(p), nil
}

// WriteString prevents io.WriteString in the pinned MessagePack encoder from
// allocating a temporary []byte for every large string field.
func (s *recordStage) WriteString(p string) (int, error) {
	copy(s.reserve(len(p)), p)
	return len(p), nil
}

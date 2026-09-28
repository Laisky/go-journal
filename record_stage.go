package journal

import (
	"bytes"
	"io"
)

// recordStage reassigns DataEncoder's original 4 MiB output-buffer budget to
// transactional staging. The prefix uses an encoder-private arena; only bytes
// beyond it allocate overflow storage. No bytes reach the live writer until the
// entire EncodeMsg succeeds. The encoder mutex owns both slices, and no caller
// memory or oversized spill is retained between records.
type recordStage struct {
	arena    []byte
	buf      []byte
	overflow bytes.Buffer
}

func newRecordStage() recordStage {
	arena := make([]byte, BufSize)
	return recordStage{arena: arena, buf: arena[:0]}
}

func (s *recordStage) Reset() {
	s.buf = s.arena[:0]
	s.overflow = bytes.Buffer{}
}

func (s *recordStage) Len() int { return len(s.buf) + s.overflow.Len() }

func (s *recordStage) prefix(n int) int {
	const maxInt = int(^uint(0) >> 1)
	if n < 0 || n > maxInt-s.Len() {
		panic(bytes.ErrTooLarge)
	}
	return min(n, cap(s.buf)-len(s.buf))
}

func (s *recordStage) Write(p []byte) (int, error) {
	n := s.prefix(len(p))
	s.buf = append(s.buf, p[:n]...)
	_, err := s.overflow.Write(p[n:])
	return len(p), err
}

// WriteString keeps large string fields off the temporary []byte path used by
// io.WriteString when an io.Writer does not implement io.StringWriter.
func (s *recordStage) WriteString(p string) (int, error) {
	n := s.prefix(len(p))
	s.buf = append(s.buf, p[:n]...)
	_, err := s.overflow.WriteString(p[n:])
	return len(p), err
}

// appendTo commits an already validated record without concatenating an arena
// and spill into another full-record allocation. A record larger than the arena
// can require two writes; the caller retains its mutex and poisons any failed
// live append. This never makes a claim of filesystem-atomic or durable writes.
func (s *recordStage) appendTo(w io.Writer) (total int, err error) {
	for _, part := range [2][]byte{s.buf, s.overflow.Bytes()} {
		if len(part) == 0 {
			continue
		}
		n, err := w.Write(part)
		if n < 0 || n > len(part) {
			return total, io.ErrShortWrite
		}
		total += n
		if err != nil {
			return total, err
		}
		if n != len(part) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

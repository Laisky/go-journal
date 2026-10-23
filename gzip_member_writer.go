package journal

import (
	"bufio"
	"compress/gzip"
	"io"
)

// Each flush publishes complete gzip members, preserving the existing on-disk
// stream contract. gzip.Writer.Flush alone does not finish a member/footer.
type gzipMemberWriter struct {
	output *bufio.Writer
	gzip   *gzip.Writer
}

func newGzipMemberWriter(output io.Writer) (*gzipMemberWriter, error) {
	buffered := bufio.NewWriterSize(output, BufSize)
	writer, err := gzip.NewWriterLevel(buffered, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	return &gzipMemberWriter{output: buffered, gzip: writer}, nil
}
func (w *gzipMemberWriter) Write(data []byte) (int, error) { return w.gzip.Write(data) }
func (w *gzipMemberWriter) Flush() error {
	if err := w.gzip.Close(); err != nil {
		return err
	}
	if err := w.output.Flush(); err != nil {
		return err
	}
	w.gzip.Reset(w.output)
	return nil
}
func (w *gzipMemberWriter) WriteFooter() error {
	if err := w.gzip.Close(); err != nil {
		return err
	}
	w.gzip.Reset(w.output)
	return nil
}

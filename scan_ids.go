package journal

import (
	"os"

	"github.com/pkg/errors"
)

// scanIDBuffers belongs to one synchronous acknowledgement-file traversal.
// Never keep it in Journal/LegacyLoader or a global pool. Each concurrent
// operation owns its buffer; gzip member/checksum state is never reused.
type scanIDBuffers struct {
	plain *IdsDecoder
}

func (b *scanIDBuffers) decoder(fp *os.File, compressed bool) (*IdsDecoder, error) {
	if b == nil || compressed {
		return NewIdsDecoder(fp, compressed)
	}
	if b.plain == nil {
		var err error
		b.plain, err = NewIdsDecoder(fp, false)
		return b.plain, err
	}
	b.plain.reader.Reset(fp)
	return b.plain, nil
}

func (b *scanIDBuffers) release(dec *IdsDecoder) {
	if b == nil || dec == nil || dec != b.plain {
		return
	}
	// The first word of EACH file is an absolute ID. Reusing the preceding
	// file's base would reinterpret it as a delta, potentially hiding work.
	dec.baseID = -1
	dec.word = [8]byte{}
	// Drop unread bytes, pending errors and the descriptor reference, while
	// retaining exactly the original 64 KiB plaintext read buffer.
	dec.reader.Reset(nil)
}

// Keep the original per-file open/stat/consume/close and error policy. The nil
// buffer path constructs a fresh decoder, including for all gzip files.
func readIDsFileWithBuffers(name string, consume func(*IdsDecoder) error, buffers *scanIDBuffers) (err error) {
	fp, err := os.Open(name)
	if err != nil {
		return errors.Wrap(err, "open acknowledgement file")
	}
	defer func() {
		if closeErr := fp.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	info, err := fp.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	dec, err := buffers.decoder(fp, isFileGZ(name))
	if err != nil {
		return errors.Wrapf(err, "decode acknowledgement header %s", name)
	}
	defer buffers.release(dec)
	if err := consume(dec); err != nil {
		return errors.Wrapf(err, "decode acknowledgement records %s", name)
	}
	return nil
}

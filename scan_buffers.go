package journal

import (
	"os"

	"github.com/tinylib/msgp/msgp"
)

// scanReaderBuffers belongs to ONE LoadMaxId call. It is neither a global pool
// nor Journal state. Reuse avoids allocating the same read-ahead buffer for
// every retained segment; the original small/large sizing policy is unchanged.
// Gzip readers remain independent so header/checksum/member state is never reused.
type scanReaderBuffers struct {
	small *DataDecoder
	large *DataDecoder
}

func (b *scanReaderBuffers) decoder(fp *os.File, info os.FileInfo, compressed bool) (*DataDecoder, error) {
	if b == nil || compressed {
		return NewDataDecoder(fp, compressed)
	}
	size, slot := readBufferSize, &b.small
	if info.Mode().IsRegular() && info.Size() >= int64(BufSize) {
		size, slot = BufSize, &b.large
	}
	if *slot == nil {
		decoder, err := NewDataDecoder(fp, false)
		if err != nil {
			return nil, err
		}
		*slot = decoder
	} else {
		(*slot).reader.R.Reset(fp)
	}
	// The file can change outside the supported ownership protocol. Preserve
	// the ordinary constructor's sizing in that case rather than reclassifying
	// a decoder or retaining a grown lookahead buffer in the wrong slot.
	if (*slot).reader.R.BufferSize() != size {
		decoder := *slot
		*slot = nil
		return decoder, nil
	}
	return *slot, nil
}

func (b *scanReaderBuffers) release(decoder *DataDecoder) {
	if b == nil || decoder == nil || decoder.gzReader != nil {
		return
	}
	r := decoder.reader.R
	// Discard read offsets, errors, seeker references and MessagePack scratch.
	// No descriptor or decoded payload remains reachable through the cache.
	r.Reset(nil)
	*decoder.reader = msgp.Reader{R: r}
	if b.small == decoder && r.BufferSize() != readBufferSize {
		b.small = nil
	}
	if b.large == decoder && r.BufferSize() != BufSize {
		b.large = nil
	}
}

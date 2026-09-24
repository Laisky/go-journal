package journal

/*
gzWriter -> writer -> fp
fp -> gzReader -> reader
*/

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/tinylib/msgp/msgp"
)

var (
	// once     = &sync.Once{}
	bitOrder = binary.BigEndian
)

const (
	defaultCompressNBlocks = 8
	// Readers need bounded lookahead, not the 4 MiB writer/compressor buffer.
	// Individual records may still exceed this size.
	readBufferSize = 64 << 10
	// Bound idle scratch retention, not the maximum accepted record size.
	maxRetainedRecordBuffer = 128 << 10
)

// BaseSerializer base serializer
type BaseSerializer struct {
	sync.Mutex
	isCompress bool
}

// DataEncoder data serializer
type DataEncoder struct {
	BaseSerializer
	// writeChan chan interface{}
	writer   *msgp.Writer
	gzWriter utils.CompressorItf
	record   bytes.Buffer // scratch owned by the encoder mutex
	writeErr error        // an incomplete live append must not accept a later record
}

// DataDecoder data deserializer
type DataDecoder struct {
	BaseSerializer
	// readChan chan interface{}
	reader   *msgp.Reader
	gzReader io.Reader
}

// IdsEncoder ids serializer
type IdsEncoder struct {
	BaseSerializer
	baseID   int64
	word     [8]byte // protected by the encoder mutex
	writer   *bufio.Writer
	gzWriter utils.CompressorItf
}

// IdsDecoder ids deserializer
type IdsDecoder struct {
	BaseSerializer
	baseID   int64
	word     [8]byte // decoder-local offset scratch space
	reader   *bufio.Reader
	gzReader io.Reader
}

// NewDataEncoder create new DataEncoder
func NewDataEncoder(fp *os.File, isCompress bool) (enc *DataEncoder, err error) {
	enc = &DataEncoder{
		BaseSerializer: BaseSerializer{
			isCompress: isCompress,
		},
	}
	if isCompress {
		if enc.gzWriter, err = utils.NewGZCompressor(
			fp,
			utils.WithCompressBufSizeByte(BufSize),
			utils.WithCompressLevel(gzip.BestSpeed),
			utils.WithPGzipNBlocks(defaultCompressNBlocks),
		); err != nil {
			return nil, err
		}
		enc.writer = msgp.NewWriterSize(enc.gzWriter, BufSize)
	} else {
		enc.writer = msgp.NewWriterSize(fp, BufSize)
	}
	return enc, nil
}

// NewIdsEncoder create new IdsEncoder
func NewIdsEncoder(fp *os.File, isCompress bool) (enc *IdsEncoder, err error) {
	enc = &IdsEncoder{
		BaseSerializer: BaseSerializer{
			isCompress: isCompress,
		},
		baseID: -1,
	}
	if isCompress {
		if enc.gzWriter, err = utils.NewGZCompressor(
			fp,
			utils.WithCompressBufSizeByte(BufSize),
			utils.WithCompressLevel(gzip.BestSpeed),
		); err != nil {
			return nil, err
		}
		enc.writer = bufio.NewWriterSize(enc.gzWriter, BufSize)
	} else {
		enc.writer = bufio.NewWriterSize(fp, BufSize)
	}
	return enc, nil
}

// NewIdsDecoder create new IdsDecoder
func NewIdsDecoder(fp *os.File, isCompress bool) (decoder *IdsDecoder, err error) {
	decoder = &IdsDecoder{
		BaseSerializer: BaseSerializer{
			isCompress: isCompress,
		},
		baseID: -1,
	}
	if isCompress {
		decoder.gzReader, err = gzip.NewReader(fp)
		if err != nil {
			return nil, errors.Wrap(err, "use gzip read ids fp")
		}
		decoder.reader = bufio.NewReaderSize(decoder.gzReader, readBufferSize)
	} else {
		decoder.reader = bufio.NewReaderSize(fp, readBufferSize)
	}

	return decoder, nil
}

// NewDataDecoder create new DataDecoder
func NewDataDecoder(fp *os.File, isCompress bool) (decoder *DataDecoder, err error) {
	decoder = &DataDecoder{
		BaseSerializer: BaseSerializer{
			isCompress: isCompress,
		},
	}
	if isCompress {
		decoder.gzReader, err = gzip.NewReader(fp)
		if err != nil {
			return nil, errors.Wrap(err, "use gzip read ids fp")
		}
		decoder.reader = msgp.NewReaderSize(decoder.gzReader, readBufferSize)
	} else {
		// Large uncompressed scans benefit from the original read-ahead size.
		// Keep small segments bounded; never use file size as a record limit.
		size := readBufferSize
		if info, statErr := fp.Stat(); statErr == nil && info.Mode().IsRegular() && info.Size() >= int64(BufSize) {
			size = BufSize
		}
		decoder.reader = msgp.NewReaderSize(fp, size)
	}
	return decoder, err
}

// Write serializes a complete record before touching the live stream. Encoding
// rejection is retryable; an I/O failure after append begins poisons this encoder.
func (enc *DataEncoder) Write(msg *Data) error {
	enc.Lock()
	defer enc.Unlock()
	if enc.writer == nil {
		return os.ErrClosed
	}
	if enc.writeErr != nil {
		return enc.writeErr
	}
	if msg == nil || msg.ID < 0 {
		return errors.New("data must be non-nil with a nonnegative ID")
	}
	enc.record.Reset()
	defer func() {
		if enc.record.Cap() > maxRetainedRecordBuffer {
			enc.record = bytes.Buffer{}
		} else {
			enc.record.Reset()
		}
	}()
	// Keep EncodeMsg semantics (including Encodable-only values), invoke custom
	// encoders exactly once, and discard all staged bytes when serialization fails.
	if err := msgp.Encode(&enc.record, msg); err != nil {
		return errors.Wrap(err, "encode journal data")
	}
	n, err := enc.writer.Write(enc.record.Bytes())
	if err == nil && n != enc.record.Len() {
		err = io.ErrShortWrite
	}
	if err != nil {
		enc.writeErr = errors.Wrap(err, "append journal record")
		return enc.writeErr
	}
	if err = enc.writer.Flush(); err != nil {
		enc.writeErr = errors.Wrap(err, "flush journal record")
		return enc.writeErr
	}
	if enc.isCompress {
		if err = enc.gzWriter.WriteFooter(); err != nil {
			enc.writeErr = errors.Wrap(err, "finish journal record")
			return enc.writeErr
		}
	}
	return nil
}

// Flush flushes encoded bytes, but cannot certify an already damaged stream.
func (enc *DataEncoder) Flush() error {
	enc.Lock()
	defer enc.Unlock()
	if enc.writer == nil {
		return os.ErrClosed
	}
	if enc.writeErr != nil {
		return enc.writeErr
	}
	if err := enc.writer.Flush(); err != nil {
		enc.writeErr = errors.Wrap(err, "flush data encoder")
		return enc.writeErr
	}
	if enc.isCompress {
		if err := enc.gzWriter.Flush(); err != nil {
			enc.writeErr = errors.Wrap(err, "flush data encoder gz")
			return enc.writeErr
		}
	}
	return nil
}

// Close releases scratch state and never flushes a known incomplete append.
func (enc *DataEncoder) Close() error {
	enc.Lock()
	defer enc.Unlock()
	if enc.writer == nil {
		return enc.writeErr
	}
	defer func() { enc.writer = nil; enc.record = bytes.Buffer{} }()
	if enc.writeErr != nil {
		return enc.writeErr
	}
	if err := enc.writer.Flush(); err != nil {
		enc.writeErr = errors.Wrap(err, "flush data encoder")
		return enc.writeErr
	}
	if enc.isCompress {
		if err := enc.gzWriter.Flush(); err != nil {
			enc.writeErr = errors.Wrap(err, "close data gz encoder")
			return enc.writeErr
		}
	}
	return nil
}

// Read deserialize data from fp
func (dec *DataDecoder) Read(data *Data) (err error) {
	if err = data.DecodeMsg(dec.reader); err == msgp.WrapError(io.EOF) {
		return io.EOF
	} else if err != nil {
		return err
	}

	return nil
}

// Write serialize id info fp
func (enc *IdsEncoder) Write(id int64) (err error) {
	if id < 0 {
		return fmt.Errorf("id should bigger than 0, but got `%v`", id)
	}

	enc.Lock()
	defer enc.Unlock()
	if enc.writer == nil {
		return os.ErrClosed
	}
	var offset int64
	if enc.baseID == -1 {
		enc.baseID = id
		offset = id // set first id as baseID
		Logger.Debug("set write base id", zap.Int64("baseID", id))
	} else {
		offset = id - enc.baseID // offset
	}

	bitOrder.PutUint64(enc.word[:], uint64(offset))
	if _, err = enc.writer.Write(enc.word[:]); err != nil {
		return errors.Wrap(err, "write ids")
	}
	if err = enc.writer.Flush(); err != nil {
		return errors.Wrap(err, "flush journal record")
	}
	if enc.isCompress {
		err = enc.gzWriter.WriteFooter()
	}

	// Logger.Debug("write id", zap.Int64("offset", offset), zap.Int64("id", id))
	return
}

// Flush flush buf to fp
func (enc *IdsEncoder) Flush() (err error) {
	enc.Lock()
	defer enc.Unlock()
	if enc.writer == nil {
		return os.ErrClosed
	}
	if err = enc.writer.Flush(); err != nil {
		return errors.Wrap(err, "flush ids encoder")
	}
	if enc.isCompress {
		if err = enc.gzWriter.Flush(); err != nil {
			return errors.Wrap(err, "flush ids encoder gz")
		}
	}

	return
}

// Close close ids gzip writer
func (enc *IdsEncoder) Close() (err error) {
	enc.Lock()
	defer enc.Unlock()
	if enc.writer == nil {
		return nil
	}
	if err = enc.writer.Flush(); err != nil {
		return errors.Wrap(err, "flush ids encoder")
	}
	if enc.isCompress {
		if err = enc.gzWriter.Flush(); err != nil {
			return errors.Wrap(err, "close ids gz encoder")
		}
	}
	enc.writer = nil
	return
}

// readOffset preserves EOF versus partial-record errors without allocating a
// temporary byte slice for every acknowledgement.
func (dec *IdsDecoder) readOffset() (int64, error) {
	if _, err := io.ReadFull(dec.reader, dec.word[:]); err != nil {
		return 0, err
	}
	return int64(bitOrder.Uint64(dec.word[:])), nil
}

// readID validates the signed-delta stream before exposing an identity.
func (dec *IdsDecoder) readID() (int64, error) {
	id, err := dec.readOffset()
	if err != nil {
		return 0, err
	}
	if dec.baseID == -1 {
		if id < 0 {
			return 0, errors.New("negative acknowledgement base ID")
		}
		dec.baseID = id
	} else {
		id += dec.baseID
		if id < 0 {
			return 0, errors.New("acknowledgement ID underflow or overflow")
		}
	}
	return id, nil
}

// LoadMaxId load the maxium id in all files
func (dec *IdsDecoder) LoadMaxId() (maxId int64, err error) {
	var id int64
	for {
		if id, err = dec.readID(); err == io.EOF {
			break
		} else if err != nil {
			return 0, errors.Wrap(err, "read ids")
		}

		// Logger.Debug("load new id", zap.Int64("id", id))
		if id > maxId {
			maxId = id
		}
	}

	return maxId, nil
}

// ReadAllToBmap read all ids in all files into bmap
func (dec *IdsDecoder) ReadAllToBmap() (ids *roaring.Bitmap, err error) {
	bitmap := roaring.New()
	var id int64
	for {
		if id, err = dec.readID(); err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "read ids")
		}

		// Logger.Debug("load new id", zap.Int64("id", id))
		if id > math.MaxUint32 {
			return nil, errors.New("acknowledgement ID does not fit uint32 bitmap")
		}
		bitmap.AddInt(int(id))
	}

	return bitmap, nil
}

// ReadAllToBmap read all ids in all files into set
func (dec *IdsDecoder) ReadAllToInt64Set(ids Int64SetItf) (err error) {
	var id int64
	for {
		if id, err = dec.readID(); err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "read ids")
		}

		// Logger.Debug("load new id", zap.Int64("id", id))
		ids.AddInt64(id)
	}

	return nil
}

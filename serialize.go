package journal

/*
gzWriter -> writer -> fp
fp -> gzReader -> reader
*/

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
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

// Write serialize data info fp
func (enc *DataEncoder) Write(msg *Data) (err error) {
	enc.Lock()
	defer enc.Unlock()
	if err = msg.EncodeMsg(enc.writer); err != nil {
		return errors.Wrap(err, "Encode journal data")
	}
	if err = enc.writer.Flush(); err != nil {
		return errors.Wrap(err, "flush journal record")
	}
	if enc.isCompress {
		err = enc.gzWriter.WriteFooter()
	}

	return
}

// Flush flush buf to fp
func (enc *DataEncoder) Flush() (err error) {
	enc.Lock()
	defer enc.Unlock()
	if err = enc.writer.Flush(); err != nil {
		return errors.Wrap(err, "flush data encoder")
	}
	if enc.isCompress {
		if err = enc.gzWriter.Flush(); err != nil {
			return errors.Wrap(err, "flush data encoder gz")
		}
	}
	return
}

// Close close data gzip writer
func (enc *DataEncoder) Close() (err error) {
	enc.Lock()
	defer enc.Unlock()
	if err = enc.writer.Flush(); err != nil {
		return errors.Wrap(err, "flush data encoder")
	}
	if enc.isCompress {
		if err = enc.gzWriter.Flush(); err != nil {
			return errors.Wrap(err, "close data gz encoder")
		}
	}
	enc.writer = nil
	return
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

// LoadMaxId load the maxium id in all files
func (dec *IdsDecoder) LoadMaxId() (maxId int64, err error) {
	var id int64
	for {
		if id, err = dec.readOffset(); err == io.EOF {
			break
		} else if err != nil {
			return 0, errors.Wrap(err, "read ids")
		}

		if dec.baseID == -1 {
			Logger.Debug("set baseID", zap.Int64("id", id))
			dec.baseID = id
		} else {
			id += dec.baseID
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
		if id, err = dec.readOffset(); err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "read ids")
		}

		if dec.baseID == -1 {
			// first id in head of file is baseID
			Logger.Debug("set baseID", zap.Int64("id", id))
			dec.baseID = id
		} else {
			// another ids in rest file are offsets
			id += dec.baseID
		}

		// Logger.Debug("load new id", zap.Int64("id", id))
		bitmap.AddInt(int(id))
	}

	return bitmap, nil
}

// ReadAllToBmap read all ids in all files into set
func (dec *IdsDecoder) ReadAllToInt64Set(ids Int64SetItf) (err error) {
	var id int64
	for {
		if id, err = dec.readOffset(); err == io.EOF {
			break
		} else if err != nil {
			return errors.Wrap(err, "read ids")
		}

		if dec.baseID == -1 {
			// first id in head of file is baseID
			Logger.Debug("set baseID", zap.Int64("id", id))
			dec.baseID = id
		} else {
			// another ids in rest file are offsets
			id += dec.baseID
		}

		// Logger.Debug("load new id", zap.Int64("id", id))
		ids.AddInt64(id)
	}

	return nil
}

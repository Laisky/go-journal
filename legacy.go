package journal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/pkg/errors"
	"github.com/tinylib/msgp/msgp"
)

// LegacyLoader loader to handle legacy data and ids
type LegacyLoader struct {
	// acquire write lock during reset.
	// acquire read lock during read/write data/ids files.
	sync.RWMutex
	logger *utils.LoggerType

	dataFNames, idsFNames []string
	isNeedReload,         // prepare datafp for `Load`
	isCompress,
	isReadyReload bool // alreddy update `dataFNames`
	ids                       Int64SetItf
	dataFileIdx, dataFilesLen int
	dataFp                    *os.File
	decoder                   *DataDecoder
}

// NewLegacyLoader create new LegacyLoader
func NewLegacyLoader(ctx context.Context,
	logger *utils.LoggerType,
	dataFNames, idsFNames []string,
	isCompress bool,
	committedIDTTL time.Duration,
) *LegacyLoader {
	l := &LegacyLoader{
		logger:        logger,
		dataFNames:    dataFNames,
		idsFNames:     idsFNames,
		isNeedReload:  true,
		isReadyReload: len(dataFNames) != 0 || len(idsFNames) != 0,
		isCompress:    isCompress,
		ids:           NewInt64SetWithTTL(ctx, committedIDTTL),
	}
	l.logger.Debug("new legacy loader",
		zap.Strings("dataFiles", dataFNames),
		zap.Strings("idsFiles", idsFNames))
	return l
}

// AddID add id in ids
func (l *LegacyLoader) AddID(id int64) {
	l.ids.AddInt64(id)
}

func (l *LegacyLoader) CheckAndRemove(id int64) bool {
	return l.ids.CheckAndRemove(id)
}

// Reset reset journal legacy link to existing files
func (l *LegacyLoader) Reset(dataFNames, idsFNames []string) {
	l.Lock()
	defer l.Unlock()

	l.logger.Debug("reset legacy loader",
		zap.Strings("data_files", dataFNames),
		zap.Strings("ids_files", idsFNames))
	l.dataFNames = dataFNames
	l.idsFNames = idsFNames
	l.isReadyReload = len(dataFNames) != 0 || len(idsFNames) != 0
}

// GetIdsLen return length of ids
func (l *LegacyLoader) GetIdsLen() int {
	return l.ids.GetLen()
}

// removeFiles retains failures for retry. A previously removed pathname is
// harmless during retry; unrelated failures must not become a successful EOF.
func (l *LegacyLoader) removeFiles(files []string) error {
	for _, name := range files {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			return errors.Wrapf(err, "remove legacy file %s", name)
		}
	}
	return nil
}

// Load load data from legacy
func (l *LegacyLoader) Load(data *Data) (err error) {
	l.Lock()
	defer l.Unlock()
	if data == nil {
		return errors.New("nil replay destination")
	}

	if l.isNeedReload {
		// legacy files not prepared
		if !l.isReadyReload {
			return io.EOF
		}

		if err = l.loadAllIDs(l.ids); err != nil {
			return errors.Wrap(err, "load acknowledgement snapshot")
		}
		l.isReadyReload = false

		// PrepareNewBufFile supplies only sealed predecessors, never the active writer.
		l.dataFilesLen = len(l.dataFNames)
		l.dataFileIdx = -1
		l.isNeedReload = false
	}

READ_NEW_FILE:
	if l.dataFp == nil {
		l.dataFileIdx++
		// all data files finished
		if l.dataFileIdx == l.dataFilesLen {
			l.logger.Debug("all data files finished")
			l.isNeedReload = true
			return io.EOF
		}

		l.logger.Debug("read new data file",
			zap.Strings("data_files", l.dataFNames),
			zap.String("fname", l.dataFNames[l.dataFileIdx]))
		l.dataFp, err = os.Open(l.dataFNames[l.dataFileIdx])
		if err != nil {
			l.dataFp = nil
			l.dataFileIdx--
			return errors.Wrap(err, "open legacy data file")
		}

		if stat, statErr := l.dataFp.Stat(); statErr == nil && stat.Size() == 0 {
			// A crash can leave an unused gzip segment with no header at all.
			l.dataFp.Close()
			l.dataFp = nil
			goto READ_NEW_FILE
		}
		if l.decoder, err = NewDataDecoder(l.dataFp, isFileGZ(l.dataFp.Name())); err != nil {
			l.dataFp.Close()
			l.dataFp = nil
			l.dataFileIdx--
			return errors.Wrap(err, "initialize legacy decoder")
		}
	}

READ_NEW_LINE:
	if err = l.decoder.Read(data); err != nil {
		if err != io.EOF && l.newestDataName() == l.dataFp.Name() && incompleteRecord(err) {
			if preserveErr := preserveIncomplete(l.dataFp.Name()); preserveErr != nil {
				return preserveErr
			}
			l.logger.Warn("recover complete records before interrupted final append", zap.Error(err), zap.String("file", l.dataFp.Name()))
			err = io.EOF
		}
		if err != io.EOF {
			// Corruption is not EOF. Retain the segment for repair/retry;
			// silently skipping it would let the journal delete unread data.
			l.dataFp.Close()
			l.dataFp = nil
			l.dataFileIdx--
			return errors.Wrap(err, "read legacy data")
		}

		// read new file
		if err = l.dataFp.Close(); err != nil {
			l.logger.Error("close file", zap.String("file", l.dataFp.Name()), zap.Error(err))
		}

		l.logger.Debug("finish read data file", zap.String("fname", l.dataFp.Name()))
		l.dataFp = nil
		goto READ_NEW_FILE
	}

	if l.ids.CheckAndRemove(data.ID) { // ignore committed data
		// l.logger.Debug("data already consumed", zap.Int64("id", id))
		goto READ_NEW_LINE
	}

	// l.logger.Debug("load unconsumed data", zap.Int64("id", id))
	return nil
}

// LoadMaxId includes sealed data as well as ACKs, without consuming either.
func (l *LegacyLoader) LoadMaxId() (maxId int64, err error) {
	l.RLock()
	defer l.RUnlock()
	l.logger.Debug("LoadMaxId...")
	startTs := utils.Clock.GetUTCNow()
	for _, name := range l.idsFNames {
		var id int64
		if err := readIDsFile(name, func(dec *IdsDecoder) error {
			var err error
			id, err = dec.LoadMaxId()
			return err
		}); err != nil {
			return 0, err
		}
		if id > maxId {
			maxId = id
		}
	}

	newest := l.newestDataName()
	for _, fname := range l.dataFNames {
		id, dataErr := maxDataID(fname, fname == newest)
		if dataErr != nil {
			return 0, dataErr
		}
		if id > maxId {
			maxId = id
		}
	}

	l.logger.Debug("load max id done",
		zap.Int64("max_id", maxId),
		zap.Float64("sec", utils.Clock.GetUTCNow().Sub(startTs).Seconds()))
	return maxId, nil
}

// LoadAllids read all ids from ids file into ids set
func (l *LegacyLoader) LoadAllids(ids Int64SetItf) error {
	l.RLock()
	defer l.RUnlock()
	return l.loadAllIDs(ids)
}

func (l *LegacyLoader) loadAllIDs(ids Int64SetItf) error {
	for _, name := range l.idsFNames {
		if err := readIDsFile(name, func(dec *IdsDecoder) error { return dec.ReadAllToInt64Set(ids) }); err != nil {
			return err
		}
	}
	return nil
}

// Scope each descriptor to one file rather than deferring all closes until the
// complete snapshot has been scanned. Unused zero-byte gzip files are valid.
func readIDsFile(name string, consume func(*IdsDecoder) error) (err error) {
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
	dec, err := NewIdsDecoder(fp, isFileGZ(name))
	if err != nil {
		return errors.Wrapf(err, "decode acknowledgement header %s", name)
	}
	if err := consume(dec); err != nil {
		return errors.Wrapf(err, "decode acknowledgement records %s", name)
	}
	return nil
}

// Clean remove old legacy files
func (l *LegacyLoader) Clean() error {
	l.Lock()
	defer l.Unlock()
	if l.dataFp != nil {
		if err := l.dataFp.Close(); err != nil {
			return err
		}
		l.dataFp = nil
	}
	l.decoder = nil
	if err := l.removeFiles(l.dataFNames); err != nil {
		return err
	}
	oldIDs := l.idsFNames
	if len(oldIDs) > 1 {
		oldIDs = oldIDs[:len(oldIDs)-1]
	} else {
		oldIDs = nil
	}
	// Preserve all ACK files while a data cleanup is still incomplete.
	if err := l.removeFiles(oldIDs); err != nil {
		return err
	}
	dirs := make(map[string]struct{})
	for _, files := range [][]string{l.dataFNames, oldIDs} {
		for _, name := range files {
			dirs[filepath.Dir(name)] = struct{}{}
		}
	}
	for dir := range dirs {
		fp, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = fp.Sync()
		closeErr := fp.Close()
		if err != nil {
			return errors.Wrap(err, "sync legacy cleanup")
		}
		if closeErr != nil {
			return closeErr
		}
	}
	// Clear the retry ledger only after all removals and directory barriers pass.
	l.dataFNames = nil
	if len(l.idsFNames) > 1 {
		l.idsFNames = l.idsFNames[len(l.idsFNames)-1:]
	}
	return nil
}

// maxDataID reads a sealed segment without consuming it or changing ACK state.
// An unreadable record is not permission to allocate potentially colliding IDs.
func maxDataID(name string, newest bool) (int64, error) {
	fp, err := os.Open(name)
	if err != nil {
		return 0, errors.Wrap(err, "open recovery data")
	}
	defer fp.Close()
	stat, err := fp.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() == 0 {
		return 0, nil
	}
	decoder, err := NewDataDecoder(fp, isFileGZ(name))
	if err != nil {
		return 0, errors.Wrap(err, "decode recovery data header")
	}
	var high int64
	for {
		d := &Data{}
		if err := decoder.Read(d); err != nil {
			if err == io.EOF {
				return high, nil
			}
			if newest && incompleteRecord(err) {
				if preserveErr := preserveIncomplete(name); preserveErr != nil {
					return 0, preserveErr
				}
				Logger.Warn("incomplete final append retained for inspection", zap.String("file", name), zap.Error(err))
				return high, nil
			}
			return 0, errors.Wrapf(err, "read recovery data %s", name)
		}
		if d.ID > high {
			high = d.ID
		}
	}
}

// A torn append is only recoverable at the tail of the newest nonempty segment.
// Invalid MessagePack types, gzip checksums and corruption in older segments
// remain errors. No bytes are truncated or discarded during diagnosis.
func incompleteRecord(err error) bool {
	cause := msgp.Cause(err)
	return cause == io.ErrUnexpectedEOF || (cause == io.EOF && err != io.EOF)
}

func (l *LegacyLoader) newestDataName() string {
	for i := len(l.dataFNames) - 1; i >= 0; i-- {
		info, err := os.Stat(l.dataFNames[i])
		if err != nil {
			return ""
		}
		if info.Size() != 0 {
			return l.dataFNames[i]
		}
	}
	return ""
}

// Hard-link the complete original segment before permitting prefix recovery.
// Cleanup can unlink the WAL name only after replacements are synchronized;
// this evidence name is never considered replayable input and is not removed.
func preserveIncomplete(name string) error {
	evidence := name + ".incomplete"
	if err := os.Link(name, evidence); err != nil {
		if !os.IsExist(err) {
			return errors.Wrap(err, "retain interrupted append")
		}
		a, ae := os.Stat(name)
		b, be := os.Stat(evidence)
		if ae != nil || be != nil || !os.SameFile(a, b) {
			return errors.New("interrupted append evidence path already belongs to another file")
		}
	}
	dir, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// closeReader releases a partially consumed replay stream during journal Close.
func (l *LegacyLoader) closeReader() {
	l.Lock()
	defer l.Unlock()
	if l.dataFp != nil {
		l.dataFp.Close()
		l.dataFp = nil
	}
	l.decoder = nil
}

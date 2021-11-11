package journal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/pkg/errors"
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
		isReadyReload: len(dataFNames) != 0,
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
	l.isReadyReload = len(dataFNames) != 0
}

// GetIdsLen return length of ids
func (l *LegacyLoader) GetIdsLen() int {
	return l.ids.GetLen()
}

// removeFile delete file, should run sync to avoid dirty files
func (l *LegacyLoader) removeFiles(fs []string) {
	for _, fpath := range fs {
		if err := os.Remove(fpath); err != nil {
			l.logger.Error("delete file",
				zap.String("file", fpath),
				zap.Error(err))
			continue
		}

		l.logger.Info("remove file", zap.String("file", fpath))
	}
}

// Load load data from legacy
func (l *LegacyLoader) Load(data *Data) (err error) {
	l.RLock()
	defer l.RUnlock()

	if l.isNeedReload {
		// legacy files not prepared
		if !l.isReadyReload {
			return io.EOF
		}

		l.isReadyReload = false
		if err = l.LoadAllids(l.ids); err != nil {
			l.logger.Error("load all ids", zap.Error(err))
		}

		l.dataFilesLen = len(l.dataFNames) - 1
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
			l.logger.Error("open data file", zap.Error(err))
			l.dataFp = nil
			goto READ_NEW_FILE
		}

		if l.decoder, err = NewDataDecoder(l.dataFp, isFileGZ(l.dataFp.Name())); err != nil {
			l.logger.Error("decode data file", zap.Error(err))
			l.dataFp = nil
			goto READ_NEW_FILE
		}
	}

READ_NEW_LINE:
	if err = l.decoder.Read(data); err != nil {
		if err != io.EOF {
			// current file is broken
			l.logger.Error("load data file", zap.Error(err))
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

// LoadMaxID load max id from all ids files
func (l *LegacyLoader) LoadMaxID() (maxID int64, err error) {
	l.logger.Debug("LoadMaxId")
	var (
		fp         *os.File
		id         int64
		idsDecoder *IdsDecoder
	)
	startTs := utils.Clock.GetUTCNow()
	for _, fname := range l.idsFNames {
		// l.logger.Debug("load ids from file", zap.String("fname", fname))
		if fp, err = os.Open(fname); err != nil {
			return 0, errors.Wrapf(err, "open file `%s` to load maxid", fname)
		}
		defer fp.Close()

		if idsDecoder, err = NewIdsDecoder(fp, isFileGZ(fp.Name())); err != nil {
			l.logger.Error("new ids decoder from file",
				zap.Error(err),
				zap.String("fname", fp.Name()),
			)
			continue
		}

		if id, err = idsDecoder.LoadMaxID(); err != nil {
			l.logger.Error("read ids decoder",
				zap.Error(err),
				zap.String("fname", fp.Name()),
			)
			continue
		}

		if id > maxID {
			maxID = id
		}
	}

	l.logger.Debug("load max id done",
		zap.Int64("max_id", maxID),
		zap.Float64("sec", utils.Clock.GetUTCNow().Sub(startTs).Seconds()))
	return id, nil
}

// LoadAllids read all ids from ids file into ids set
func (l *LegacyLoader) LoadAllids(ids Int64SetItf) (err error) {
	l.logger.Debug("call LoadAllids")
	var (
		errMsg     string
		fp         *os.File
		idsDecoder *IdsDecoder
	)

	startTs := utils.Clock.GetUTCNow()
	for _, fname := range l.idsFNames {
		// l.logger.Debug("load ids from file", zap.String("fname", fname))
		if fp != nil {
			if err = fp.Close(); err != nil {
				l.logger.Error("close file", zap.String("file", fp.Name()), zap.Error(err))
			}
		}

		fp, err = os.Open(fname)
		if err != nil {
			errMsg += errors.Wrapf(err, "open file `%s`", fname).Error() + ";"
			continue
		}

		if idsDecoder, err = NewIdsDecoder(fp, isFileGZ(fp.Name())); err != nil {
			errMsg += errors.Wrapf(err, "create ids decoder `%s`", fname).Error() + ";"
			continue
		}

		if err = idsDecoder.ReadAllToInt64Set(ids); err != nil {
			errMsg += errors.Wrapf(err, "load ids from `%s`", fname).Error() + ";"
			continue
		}
	}

	if fp != nil {
		if err = fp.Close(); err != nil {
			l.logger.Error("close file", zap.String("file", fp.Name()), zap.Error(err))
		}
	}

	l.logger.Debug("load all ids done",
		zap.Float64("sec", utils.Clock.GetUTCNow().Sub(startTs).Seconds()))
	if errMsg != "" {
		return fmt.Errorf("load all ids: " + errMsg)
	}

	return nil
}

// Clean remove old legacy files
func (l *LegacyLoader) Clean() error {
	l.Lock()
	defer l.Unlock()

	if len(l.dataFNames) > 1 {
		l.removeFiles(l.dataFNames[:len(l.dataFNames)-1])
		l.dataFNames = []string{l.dataFNames[len(l.dataFNames)-1]}
	}

	if len(l.idsFNames) > 1 {
		l.removeFiles(l.idsFNames[:len(l.idsFNames)-1])
		l.idsFNames = []string{l.idsFNames[len(l.idsFNames)-1]}
	}

	l.dataFp.Close()
	l.dataFp = nil // `Load` need this
	l.logger.Debug("clean all legacy files")
	return nil
}

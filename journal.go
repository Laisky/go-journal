package journal

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/pkg/errors"
)

var Logger *utils.LoggerType

func init() {
	var err error
	if Logger, err = utils.NewConsoleLoggerWithName("go-journal", "info"); err != nil {
		utils.Logger.Panic("new journal logger", zap.Error(err))
	}
}

// Journal redo log consist by msgs and committed ids
type Journal struct {
	// RWMutex journal rwlock.
	// acquire write lock when flush/rotate journal legacy.
	// acquire read lock when read/write journal legacy.
	sync.RWMutex
	*option

	stopChan               chan struct{}
	rotateLock, legacyLock *utils.Mutex
	dataFp, idsFp          *os.File // current writting journal file
	fsStat                 *bufFileStat
	legacy                 *LegacyLoader
	dataEnc                *DataEncoder
	idsEnc                 *IdsEncoder
	lastRotateAt           time.Time
}

// NewJournal create new Journal
func NewJournal(opts ...OptionFunc) (j *Journal, err error) {
	j = &Journal{
		stopChan:   make(chan struct{}),
		rotateLock: utils.NewMutex(),
		legacyLock: utils.NewMutex(),
		option:     newOption(),
	}

	for _, optf := range opts {
		if err = optf(j.option); err != nil {
			return nil, err
		}
	}

	j.logger.Info("new journal",
		zap.String("bufDirPath", j.bufDirPath),
		zap.Int64("bufSizeBytes", j.bufSizeBytes),
		zap.Bool("isAggresiveGC", j.isAggresiveGC),
		zap.Bool("isCompress", j.isCompress),
		zap.Duration("flushInterval", j.flushInterval),
		zap.Duration("rotateDuration", j.rotateDuration),
		zap.Duration("rotateCheckInterval", j.rotateCheckInterval),
		zap.Duration("committedIDTTL", j.committedIDTTL),
	)
	return j, nil
}

func (j *Journal) Start(ctx context.Context) (err error) {
	if err = j.initBufDir(ctx); err != nil {
		return errors.Wrap(err, "init buf directory")
	}

	go j.startFlushTrigger(ctx)
	go j.startRotateTrigger(ctx)
	return
}

func (j *Journal) Close() {
	j.logger.Info("close Journal")

	j.Lock()
	j.Flush()
	j.stopChan <- struct{}{}
	j.Unlock()
}

// initBufDir initialize buf directory and create buf files
func (j *Journal) initBufDir(ctx context.Context) (err error) {
	if err = fileutil.IsDirWriteable(j.bufDirPath); err != nil {
		return errors.Wrapf(err, "cannot write to `%s`", j.bufDirPath)
	}

	if err = j.Rotate(ctx); err != nil { // manually first run
		return errors.Wrapf(err, "init rotate in `%s`", j.bufDirPath)
	}

	return
}

// Flush flush journal files buffer to file
func (j *Journal) Flush() (err error) {
	if j.idsEnc != nil {
		// j.logger.Debug("flush ids")
		if err = j.idsEnc.Flush(); err != nil {
			err = errors.Wrap(err, "flush ids encoder")
		}
	}

	if j.dataEnc != nil {
		// j.logger.Debug("flush data")
		if dataErr := j.dataEnc.Flush(); dataErr != nil {
			err = errors.Wrap(err, "flush data encoder")
		}
	}

	return err
}

// flushAndClose flush journal files then close
func (j *Journal) flushAndClose() (err error) {
	j.logger.Debug("flushAndClose")
	if j.idsEnc != nil {
		if err = j.idsEnc.Close(); err != nil {
			err = errors.Wrap(err, "flush ids encoder")
		}
	}

	if j.dataEnc != nil {
		if dataErr := j.dataEnc.Close(); dataErr != nil {
			err = errors.Wrap(err, "flush data encoder")
		}
	}

	return err
}

func (j *Journal) startFlushTrigger(ctx context.Context) {
	j.logger.Info("start flush trigger", zap.Duration("interval", j.flushInterval))
	defer j.logger.Info("journal flush exit")

	defer j.Flush()
	var err error
	ticker := time.NewTicker(j.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-j.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.Lock()
			if err = j.Flush(); err != nil {
				j.logger.Error("flush journal", zap.Error(err))
			}
			j.Unlock()
		}
	}
}

func (j *Journal) startRotateTrigger(ctx context.Context) {
	j.logger.Info("start rotate trigger", zap.Duration("interval", j.rotateCheckInterval))
	defer j.logger.Info("journal rotate exit")

	ticker := time.NewTicker(j.rotateCheckInterval)
	defer ticker.Stop()
	var err error
	for {
		select {
		case <-j.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if j.isReadyToRotate() {
				if err = j.Rotate(ctx); err != nil {
					j.logger.Error("trigger rotate", zap.Error(err))
				}
			}
		}
	}
}

// LoadMaxId load max id from journal ids files
func (j *Journal) LoadMaxId() (int64, error) {
	return j.legacy.LoadMaxId()
}

// WriteData write data to journal
func (j *Journal) WriteData(data *Data) (err error) {
	j.RLock() // will blocked by flush & rotate
	defer j.RUnlock()

	if j.legacy.CheckAndRemove(data.ID) {
		return
	}

	// j.logger.Debug("write data", zap.Int64("id", GetId(*data)))
	return j.dataEnc.Write(data)
}

// WriteId write id to journal
func (j *Journal) WriteId(id int64) error {
	j.RLock() // will blocked by flush & rotate
	defer j.RUnlock()

	j.legacy.AddID(id)
	return j.idsEnc.Write(id)
}

// isReadyToRotate check whether is ready to start rotate.
// file size bigger `bufSizeBytes` or existing time logger than `rotateDuration`
func (j *Journal) isReadyToRotate() (ok bool) {
	j.RLock()
	defer j.RUnlock()

	if j.dataFp == nil {
		return true
	}

	if fi, err := j.dataFp.Stat(); err != nil {
		j.logger.Error("try to get file stat got error", zap.Error(err))
		ok = false
	} else if fi.Size() > j.bufSizeBytes ||
		utils.Clock.GetUTCNow().Sub(j.lastRotateAt) > j.rotateDuration {
		ok = true
	}

	j.logger.Debug("check isReadyToRotate",
		zap.Bool("ready", ok),
		zap.String("old_file", j.dataFp.Name()),
	)
	return
}

// Rotate create new data and ids buf file.
// this function is not threadsafe.
func (j *Journal) Rotate(ctx context.Context) (err error) {
	j.logger.Debug("call Rotate")
	// make sure no other rorate is running
	if !j.rotateLock.TryLock() {
		return nil
	}

	defer j.rotateLock.ForceRelease()
	// stop legacy processing
	j.Lock()
	defer j.Unlock()
	j.logger.Debug("starting to rotate")

	select {
	case <-j.stopChan:
		return
	case <-ctx.Done():
		return
	default:
	}

	if err = j.flushAndClose(); err != nil {
		return errors.Wrap(err, "flush and close journal")
	}

	j.lastRotateAt = utils.Clock.GetUTCNow()
	// scan and create files
	// acquired legacy lock means that there is no one reading legacy
	if j.LockLegacy() {
		j.logger.Debug("acquired legacy lock, create new file and refresh legacy loader",
			zap.String("dir", j.bufDirPath))
		// need to refresh legacy, so need scan=true
		if j.fsStat, err = PrepareNewBufFile(j.bufDirPath, j.fsStat, true, j.isCompress, j.bufSizeBytes); err != nil {
			j.UnLockLegacy()
			return errors.Wrap(err, "prepare new buf file")
		}

		j.refreshLegacyLoader(ctx)
		j.UnLockLegacy()
	} else {
		j.logger.Debug("can not acquire legacy lock, so only create new file",
			zap.String("dir", j.bufDirPath))
		// no need to scan old buf files
		if j.fsStat, err = PrepareNewBufFile(j.bufDirPath, j.fsStat, false, j.isCompress, j.bufSizeBytes); err != nil {
			return errors.Wrap(err, "prepare new buf file")
		}
	}

	// create & open data file
	if j.dataFp != nil {
		j.dataFp.Close()
	}
	j.dataFp = j.fsStat.NewDataFp
	if j.dataEnc, err = NewDataEncoder(j.dataFp, j.isCompress); err != nil {
		return errors.Wrapf(err, "create new data encoder `%s`", j.dataFp.Name())
	}

	// create & open ids file
	if j.idsFp != nil {
		j.idsFp.Close()
	}
	j.idsFp = j.fsStat.NewIDsFp
	if j.idsEnc, err = NewIdsEncoder(j.idsFp, j.isCompress); err != nil {
		return errors.Wrapf(err, "create new ids encoder `%s`", j.idsFp.Name())
	}

	return nil
}

// refreshLegacyLoader create or reset legacy loader
func (j *Journal) refreshLegacyLoader(ctx context.Context) {
	j.logger.Debug("call refreshLegacyLoader")
	if j.legacy == nil {
		j.logger.Debug("create new LegacyLoader",
			zap.Strings("data_files", j.fsStat.OldDataFnames),
			zap.Strings("ids_files", j.fsStat.OldIDsDataFnames),
		)
		j.legacy = NewLegacyLoader(
			ctx,
			j.logger,
			j.fsStat.OldDataFnames,
			j.fsStat.OldIDsDataFnames,
			j.isCompress,
			j.committedIDTTL,
		)
	} else {
		j.legacy.Reset(j.fsStat.OldDataFnames, j.fsStat.OldIDsDataFnames)
		if j.isAggresiveGC {
			utils.TriggerGC()
		}
	}
}

// LockLegacy lock legacy to prevent rotate, clean
func (j *Journal) LockLegacy() bool {
	j.logger.Debug("call LockLegacy")
	return j.legacyLock.TryLock()
}

// IsLegacyRunning check whether running legacy loading
func (j *Journal) IsLegacyRunning() bool {
	j.logger.Debug("call IsLegacyRunning")
	return j.legacyLock.IsLocked()
}

// UnLockLegacy release legacy lock
func (j *Journal) UnLockLegacy() bool {
	j.logger.Debug("call UnLockLegacy")
	return j.legacyLock.TryRelease()
}

// GetMetric monitor inteface
func (j *Journal) GetMetric() map[string]interface{} {
	return map[string]interface{}{
		"idsSetLen": j.legacy.GetIdsLen(),
	}
}

// LoadLegacyBuf load legacy data one by one
// ⚠️Warn: should call `j.LockLegacy()` before invoke this method
func (j *Journal) LoadLegacyBuf(data *Data) (err error) {
	if !j.IsLegacyRunning() {
		j.logger.Panic("should call `j.LockLegacy()` first")
	}

	j.RLock()
	defer j.RUnlock()

	if j.legacy == nil {
		j.UnLockLegacy()
		return io.EOF
	}

	if err = j.legacy.Load(data); err == io.EOF {
		j.logger.Debug("load all legacy data")
		if err = j.legacy.Clean(); err != nil {
			j.logger.Error("clean legacy", zap.Error(err))
		}

		j.UnLockLegacy()
		return io.EOF
	} else if err != nil {
		j.UnLockLegacy()
		return errors.Wrap(err, "load legacy data")
	}

	return nil
}

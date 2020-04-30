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

// Journal redo log consist by msgs and committed ids
type Journal struct {
	sync.RWMutex // journal rwlock
	*option

	logger                 *utils.LoggerType
	stopChan               chan struct{}
	rotateLock, legacyLock *utils.Mutex
	dataFp, idsFp          *os.File // current writting journal file
	fsStat                 *BufFileStat
	legacy                 *LegacyLoader
	dataEnc                *DataEncoder
	idsEnc                 *IdsEncoder
	latestRotateT          time.Time
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

	j.logger = utils.Logger.Named(j.name)
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
	return
}

func (j *Journal) Start(ctx context.Context) {
	j.initBufDir(ctx)
	go j.runFlushTrigger(ctx)
	go j.runRotateTrigger(ctx)
}

func (j *Journal) Close() {
	j.logger.Info("close Journal")
	j.Lock()
	j.Flush()
	j.stopChan <- struct{}{}
	j.Unlock()
}

// initBufDir initialize buf directory and create buf files
func (j *Journal) initBufDir(ctx context.Context) {
	var err error
	if err = fileutil.TouchDirAll(j.bufDirPath); err != nil {
		j.logger.Panic("try to prepare dir got error",
			zap.String("dir_path", j.bufDirPath),
			zap.Error(err))
	}

	if err = j.Rotate(ctx); err != nil { // manually first run
		j.logger.Panic("try to call rotate got error", zap.Error(err))
	}
}

// Flush flush journal files
func (j *Journal) Flush() (err error) {
	if j.idsEnc != nil {
		// j.logger.Debug("flush ids")
		if err = j.idsEnc.Flush(); err != nil {
			err = errors.Wrap(err, "try to flush ids got error")
		}
	}

	if j.dataEnc != nil {
		// j.logger.Debug("flush data")
		if dataErr := j.dataEnc.Flush(); dataErr != nil {
			err = errors.Wrap(err, "try to flush data got error")
		}
	}

	return err
}

// flushAndClose flush journal files
func (j *Journal) flushAndClose() (err error) {
	j.logger.Debug("flushAndClose")
	if j.idsEnc != nil {
		if err = j.idsEnc.Close(); err != nil {
			err = errors.Wrap(err, "try to close ids got error")
		}
	}

	if j.dataEnc != nil {
		if dataErr := j.dataEnc.Close(); dataErr != nil {
			err = errors.Wrap(err, "try to close data got error")
		}
	}

	return err
}

func (j *Journal) runFlushTrigger(ctx context.Context) {
	defer j.Flush()
	defer j.logger.Info("journal flush exit")

	var err error
	for {
		j.Lock()
		select {
		case <-j.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}

		if err = j.Flush(); err != nil {
			j.logger.Error("try to flush ids&data got error", zap.Error(err))
		}
		j.Unlock()
		time.Sleep(j.flushInterval)
	}
}

func (j *Journal) runRotateTrigger(ctx context.Context) {
	defer j.Flush()
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

	if j.legacy.IsIDExists(data.ID) {
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
		utils.Clock.GetUTCNow().Sub(j.latestRotateT) > j.rotateDuration {
		ok = true
	}

	j.logger.Debug("check isReadyToRotate",
		zap.Bool("ready", ok),
		zap.String("file", j.dataFp.Name()),
	)
	return
}

/*Rotate create new data and ids buf file
this function is not threadsafe.
*/
func (j *Journal) Rotate(ctx context.Context) (err error) {
	j.logger.Debug("try to starting to rotate")
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
		return errors.Wrap(err, "try to flush journal got error")
	}

	j.latestRotateT = utils.Clock.GetUTCNow()
	// scan and create files
	if j.LockLegacy() {
		j.logger.Debug("acquired legacy lock, create new file and refresh legacy loader",
			zap.String("dir", j.bufDirPath))
		// need to refresh legacy, so need scan=true
		if j.fsStat, err = PrepareNewBufFile(j.bufDirPath, j.fsStat, true, j.isCompress, j.bufSizeBytes); err != nil {
			j.UnLockLegacy()
			return errors.Wrap(err, "call PrepareNewBufFile got error")
		}
		j.refreshLegacyLoader(ctx)
		j.UnLockLegacy()
	} else {
		j.logger.Debug("can not acquire legacy lock, so only create new file",
			zap.String("dir", j.bufDirPath))
		// no need to scan old buf files
		if j.fsStat, err = PrepareNewBufFile(j.bufDirPath, j.fsStat, false, j.isCompress, j.bufSizeBytes); err != nil {
			return errors.Wrap(err, "call PrepareNewBufFile got error")
		}
	}

	// create & open data file
	if j.dataFp != nil {
		j.dataFp.Close()
	}
	j.dataFp = j.fsStat.NewDataFp
	if j.dataEnc, err = NewDataEncoder(j.dataFp, j.isCompress); err != nil {
		return errors.Wrap(err, "try to create new data encoder got error")
	}

	// create & open ids file
	if j.idsFp != nil {
		j.idsFp.Close()
	}
	j.idsFp = j.fsStat.NewIDsFp
	if j.idsEnc, err = NewIdsEncoder(j.idsFp, j.isCompress); err != nil {
		return errors.Wrap(err, "try to create new ids encoder got error")
	}

	return nil
}

// refreshLegacyLoader create or reset legacy loader
func (j *Journal) refreshLegacyLoader(ctx context.Context) {
	j.logger.Debug("refreshLegacyLoader")
	if j.legacy == nil {
		j.legacy = NewLegacyLoader(ctx, j.fsStat.OldDataFnames, j.fsStat.OldIdsDataFname, j.isCompress, j.committedIDTTL)
	} else {
		j.legacy.Reset(j.fsStat.OldDataFnames, j.fsStat.OldIdsDataFname)
		if j.isAggresiveGC {
			utils.TriggerGC()
		}
	}
}

// LockLegacy lock legacy to prevent rotate
func (j *Journal) LockLegacy() bool {
	j.logger.Debug("try to lock legacy")
	return j.legacyLock.TryLock()
}

// IsLegacyRunning check whether running legacy loading
func (j *Journal) IsLegacyRunning() bool {
	j.logger.Debug("IsLegacyRunning")
	return j.legacyLock.IsLocked()
}

// UnLockLegacy release legacy lock
func (j *Journal) UnLockLegacy() bool {
	j.logger.Debug("try to unlock legacy")
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
		j.logger.Debug("LoadLegacyBuf done")
		if err = j.legacy.Clean(); err != nil {
			j.logger.Error("clean buf files got error", zap.Error(err))
		}
		j.UnLockLegacy()
		return io.EOF
	} else if err != nil {
		j.UnLockLegacy()
		return errors.Wrap(err, "load legacy journal got error")
	}

	return nil
}

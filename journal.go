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

	stopChan      chan struct{}
	closeOnce     sync.Once
	lifecycleMu   sync.Mutex
	workers       sync.WaitGroup
	started       bool
	dirLock       *fileutil.LockedFile
	legacyLock    *utils.Mutex
	dataFp, idsFp *os.File // current writting journal file
	fsStat        *bufFileStat
	legacy        *LegacyLoader
	dataEnc       *DataEncoder
	idsEnc        *IdsEncoder
	lastRotateAt  time.Time
}

// NewJournal create new Journal
func NewJournal(opts ...OptionFunc) (j *Journal, err error) {
	j = &Journal{
		stopChan:   make(chan struct{}),
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
	j.lifecycleMu.Lock()
	defer j.lifecycleMu.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if j.started {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A directory is a single WAL ownership domain. Keep the lock inode after
	// Close: unlinking it would let a third opener bypass an existing owner.
	lock, err := fileutil.TryLockFile(filepath.Join(j.bufDirPath, ".journal.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return errors.Wrap(err, "lock journal directory")
	}
	j.Lock()
	j.dirLock = lock
	j.Unlock()

	if err = j.initBufDir(ctx); err != nil {
		j.Lock()
		j.dirLock.Close()
		j.dirLock = nil
		j.Unlock()
		return errors.Wrap(err, "init buf directory")
	}

	j.started = true
	j.workers.Add(2)
	go func() { defer j.workers.Done(); j.startFlushTrigger(ctx) }()
	go func() { defer j.workers.Done(); j.startRotateTrigger(ctx) }()
	return
}

// Close broadcasts shutdown, waits for both workers, and is idempotent.
func (j *Journal) Close() {
	j.closeOnce.Do(func() {
		j.lifecycleMu.Lock()
		close(j.stopChan)
		j.lifecycleMu.Unlock()
		j.workers.Wait()
		j.Lock()
		defer j.Unlock()
		if err := j.flushLocked(); err != nil {
			j.logger.Error("flush closing journal", zap.Error(err))
		}
		if j.dataFp != nil {
			j.dataFp.Close()
			j.dataFp = nil
		}
		if j.idsFp != nil {
			j.idsFp.Close()
			j.idsFp = nil
		}
		j.dataEnc, j.idsEnc = nil, nil
		if j.legacy != nil {
			j.legacy.closeReader()
		}
		if j.legacy != nil {
			if closer, ok := j.legacy.ids.(interface{ Close() }); ok {
				closer.Close()
			}
		}
		if j.dirLock != nil {
			j.dirLock.Close()
			j.dirLock = nil
		}

	})
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
func (j *Journal) Flush() error {
	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	return j.flushLocked()
}

// flushLocked is used only while holding the journal lock.
func (j *Journal) flushLocked() (err error) {
	if j.idsEnc != nil {
		// j.logger.Debug("flush ids")
		if err = j.idsEnc.Flush(); err != nil {
			err = errors.Wrap(err, "flush ids encoder")
		}
	}

	if j.dataEnc != nil {
		// j.logger.Debug("flush data")
		if dataErr := j.dataEnc.Flush(); dataErr != nil {
			err = errors.Wrap(dataErr, "flush data encoder")
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
			err = errors.Wrap(dataErr, "flush data encoder")
		}
	}

	return err
}

func (j *Journal) startFlushTrigger(ctx context.Context) {
	j.logger.Info("start flush trigger", zap.Duration("interval", j.flushInterval))
	defer j.logger.Info("journal flush exit")

	defer func() { j.Lock(); defer j.Unlock(); j.flushLocked() }()
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
			if err = j.flushLocked(); err != nil {
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

// LoadMaxId includes all retained data and acknowledgement records.
func (j *Journal) LoadMaxId() (int64, error) {
	j.RLock()
	defer j.RUnlock()
	select {
	case <-j.stopChan:
		return 0, os.ErrClosed
	default:
	}
	if j.legacy == nil {
		return 0, ErrNotStarted
	}
	return j.legacy.LoadMaxId()
}

// WriteData write data to journal
func (j *Journal) WriteData(data *Data) (err error) {
	j.RLock() // will blocked by flush & rotate
	defer j.RUnlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}

	if j.dataEnc == nil || j.legacy == nil {
		return ErrNotStarted
	}
	if data == nil || data.ID < 0 {
		return errors.New("data must be non-nil with a nonnegative ID")
	}
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
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}

	if j.idsEnc == nil || j.legacy == nil {
		return ErrNotStarted
	}
	if err := j.idsEnc.Write(id); err != nil {
		return err
	}
	j.legacy.AddID(id)
	return nil
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

// Rotate serializes rotation with writes, Flush, Sync and Close. A failed
// replacement-file preparation leaves the current synchronized writer usable.
func (j *Journal) Rotate(ctx context.Context) error {
	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if j.dirLock == nil {
		return ErrNotStarted
	}
	scan := j.LockLegacy()
	if scan {
		defer j.UnLockLegacy()
	} else if j.legacy == nil {
		return ErrDuringRotate
	}
	next, err := PrepareNewBufFile(j.bufDirPath, j.fsStat, scan, j.isCompress, j.bufSizeBytes)
	if err != nil {
		return errors.Wrap(err, "prepare new journal files")
	}
	installed := false
	defer func() {
		if !installed {
			next.NewDataFp.Close()
			next.NewIDsFp.Close()
			os.Remove(next.NewDataFp.Name())
			os.Remove(next.NewIDsFp.Name())
		}
	}()
	dataEnc, err := NewDataEncoder(next.NewDataFp, j.isCompress)
	if err != nil {
		return err
	}
	idsEnc, err := NewIdsEncoder(next.NewIDsFp, j.isCompress)
	if err != nil {
		return err
	}
	if err := j.syncLocked(); err != nil {
		return errors.Wrap(err, "sync journal before rotation")
	}
	// Flush has already finished every compressed member. Do not perform another
	// encoder Close after Sync (it appends another empty gzip member). Releasing
	// these private encoders after the successful barrier is sufficient.
	oldData, oldIDs := j.dataFp, j.idsFp
	j.fsStat = next
	j.dataFp, j.idsFp = next.NewDataFp, next.NewIDsFp
	j.dataEnc, j.idsEnc = dataEnc, idsEnc
	installed = true
	if scan {
		j.refreshLegacyLoader(ctx)
	}
	j.lastRotateAt = utils.Clock.GetUTCNow()
	var closeErr error
	for _, fp := range []*os.File{oldData, oldIDs} {
		if fp != nil {
			if err := fp.Close(); err != nil {
				closeErr = errors.Wrap(err, "close rotated journal")
			}
		}
	}
	return closeErr
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
	select {
	case <-j.stopChan:
		return false
	default:
	}
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
	j.RLock()
	defer j.RUnlock()
	if j.legacy == nil {
		return map[string]interface{}{"idsSetLen": 0}
	}
	return map[string]interface{}{
		"idsSetLen": j.legacy.GetIdsLen(),
	}
}

// LoadLegacyBuf load legacy data one by one
// ⚠️Warn: should call `j.LockLegacy()` before invoke this method
func (j *Journal) LoadLegacyBuf(data *Data) (err error) {
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if !j.IsLegacyRunning() {
		j.logger.Panic("should call `j.LockLegacy()` first")
	}

	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		j.UnLockLegacy()
		return os.ErrClosed
	default:
	}

	if j.legacy == nil {
		j.UnLockLegacy()
		return io.EOF
	}

	if err = j.legacy.Load(data); err == io.EOF {
		j.logger.Debug("load all legacy data")
		if err = j.syncLocked(); err != nil {
			j.UnLockLegacy()
			return errors.Wrap(err, "sync replay before cleanup")
		}
		if err = j.legacy.Clean(); err != nil {
			j.UnLockLegacy()
			return errors.Wrap(err, "clean legacy")
		}

		j.UnLockLegacy()
		return io.EOF
	} else if err != nil {
		j.UnLockLegacy()
		return errors.Wrap(err, "load legacy data")
	}

	return nil
}

// Sync makes completed writes durable. It does not wait for work queued in
// another goroutine: replay callers must WriteData before requesting cleanup.
func (j *Journal) Sync() error {
	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if j.dataEnc == nil || j.idsEnc == nil {
		return ErrNotStarted
	}
	return j.syncLocked()
}
func (j *Journal) syncLocked() error {
	if err := j.flushLocked(); err != nil {
		return err
	}
	for _, fp := range []*os.File{j.dataFp, j.idsFp} {
		if fp != nil {
			if err := fp.Sync(); err != nil {
				return errors.Wrap(err, "sync journal file")
			}
		}
	}
	if j.dataFp != nil || j.idsFp != nil {
		dir, err := os.Open(j.bufDirPath)
		if err != nil {
			return errors.Wrap(err, "open journal directory for sync")
		}
		defer dir.Close()
		if err := dir.Sync(); err != nil {
			return errors.Wrap(err, "sync journal directory")
		}
	}
	return nil
}

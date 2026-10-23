package journal

import (
	"compress/gzip"
	"context"
	"github.com/tinylib/msgp/msgp"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/pkg/errors"
)

// Journal redo log consist by msgs and committed ids
type Journal struct {
	dataSequence            atomic.Uint64
	snapshotDataSequence    atomic.Uint64
	retainedAllAcknowledged atomic.Bool
	retainedUnackSeen       bool

	activeDataWritten  atomic.Bool
	activeDataEstimate atomic.Int64
	retainedACKDirty   atomic.Bool
	// RWMutex journal rwlock.
	// acquire write lock when flush/rotate journal legacy.
	// acquire read lock when read/write journal legacy.
	sync.RWMutex
	*option

	stopChan       chan struct{}
	closeOnce      sync.Once
	lifecycleMu    sync.Mutex
	workers        sync.WaitGroup
	started        bool
	dirLock        *os.File
	ownedRoot      *os.Root
	ownedDirectory *rootedDirectory
	disk           journalFS
	legacyLock     *utils.Mutex
	dataFp, idsFp  *os.File // current writting journal file
	fsStat         *bufFileStat
	legacy         *LegacyLoader
	dataEnc        *DataEncoder
	idsEnc         *IdsEncoder
	lastRotateAt   time.Time
	syncGroup      syncBarrierGroup
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

	if j.root != nil {
		j.ownedRoot, err = j.root.OpenRoot(".")
		if err != nil {
			return nil, errors.Wrap(err, "duplicate journal directory")
		}
		j.bufDirPath = filepath.Clean(j.ownedRoot.Name())
		j.disk, j.ownedDirectory = newRootedFS(j.ownedRoot)
		j.root = nil // Do not keep the caller's borrowed handle.
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
	if j.ownedRoot == nil {
		root, err := os.OpenRoot(j.bufDirPath)
		if err != nil {
			return errors.Wrap(err, "open journal directory")
		}
		j.ownedRoot = root
		j.bufDirPath = filepath.Clean(root.Name())
		j.disk, j.ownedDirectory = newRootedFS(root)
	}
	lock, err := j.disk.OpenFile(filepath.Join(j.bufDirPath, ".journal.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err == nil {
		err = lockJournalFile(lock)
		if err != nil {
			lock.Close()
		}
	}
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
		if j.ownedDirectory != nil {
			j.ownedDirectory.Close()
		}
		if j.ownedRoot != nil {
			j.ownedRoot.Close()
		}

	})
}

// initBufDir initialize buf directory and create buf files
func (j *Journal) initBufDir(ctx context.Context) (err error) {
	if err = j.Rotate(ctx); err != nil { // manually first run
		return errors.Wrapf(err, "init rotate in `%s`", j.bufDirPath)
	}

	return
}

// Flush flush journal files buffer to file
func (j *Journal) Flush() error {
	if j.option != nil && j.storageGuard != nil {
		release, err := j.storageGuard("flush", 64<<10)
		if err != nil {
			return err
		}
		defer release()
	}
	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if j.dataEnc == nil && j.idsEnc == nil {
		return ErrNotStarted
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
			release := func() {}
			if j.option != nil && j.storageGuard != nil {
				release, err = j.storageGuard("flush", 64<<10)
				if err != nil {
					j.logger.Error("journal flush budget refused", zap.Error(err))
					continue
				}
			}
			j.Lock()
			if err = j.flushLocked(); err != nil {
				j.logger.Error("flush journal", zap.Error(err))
			}
			j.Unlock()
			release()
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
	err = j.dataEnc.Write(data)
	if err == nil {
		if j.option == nil || j.storageGuard == nil {
			j.activeDataWritten.Store(true)
			j.dataSequence.Add(1)
		}
	}
	return err
}

// WriteId write id to journal
func (j *Journal) WriteId(id int64) error {
	if j.option != nil && j.storageGuard != nil {
		release, e := j.storageGuard("ack", 64<<10)
		if e != nil {
			return e
		}
		defer release()
	}
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
	j.retainedACKDirty.Store(true)
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
	if j.option != nil && j.storageGuard != nil {
		release, e := j.storageGuard("rotation", 64<<10)
		if e != nil {
			return e
		}
		defer release()
	}
	return j.rotateUnderGuard(ctx)
}
func (j *Journal) rotateUnderGuard(ctx context.Context) error {
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
	preallocate := j.bufSizeBytes
	if j.option != nil && j.storageGuard != nil {
		preallocate = 0
	} // No speculative disk allocation outside the funded mutation.
	next, err := prepareNewBufFile(j.disk, j.bufDirPath, j.fsStat, scan, j.isCompress, preallocate)
	if err != nil {
		return errors.Wrap(err, "prepare new journal files")
	}
	installed := false
	defer func() {
		if !installed {
			next.NewDataFp.Close()
			next.NewIDsFp.Close()
			j.disk.Remove(next.NewDataFp.Name())
			j.disk.Remove(next.NewIDsFp.Name())
		}
	}()
	dataEnc, err := NewDataEncoder(next.NewDataFp, j.isCompress)
	if err != nil {
		return err
	}
	if j.option != nil && j.storageGuard != nil {
		dataEnc.beforeAppend = j.guardPreparedData
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
	j.activeDataWritten.Store(false)
	j.activeDataEstimate.Store(0)
	j.retainedACKDirty.Store(false)
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
		j.legacy.disk = j.disk
	} else {
		j.legacy.Reset(j.fsStat.OldDataFnames, j.fsStat.OldIDsDataFnames)
		if j.isAggresiveGC {
			utils.TriggerGC()
		}
	}
	j.snapshotDataSequence.Store(j.dataSequence.Load())
	j.retainedUnackSeen = false
	j.retainedAllAcknowledged.Store(len(j.fsStat.OldDataFnames) == 0)

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
// Overlapping callers may share a barrier; sequential calls never reuse a
// completed result. Each shared barrier still synchronizes both files and dir.
func (j *Journal) Sync() error {
	if j.option != nil && j.storageGuard != nil {
		release, e := j.storageGuard("sync", 64<<10)
		if e != nil {
			return e
		}
		defer release()
	}
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	return j.syncGroup.run(&j.RWMutex, j.syncBarrierLocked)
}

// syncBarrierLocked runs with exclusive writer ownership. Overlapping Sync
// callers can share this result only until ownership is released.
func (j *Journal) syncBarrierLocked() error {
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
		dir, err := j.disk.Open(j.bufDirPath)
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

// LoadLegacyBufRetained reads a frozen snapshot WITHOUT retiring any segment at
// EOF. The caller must hold LockLegacy and may retry by rotating after EOF.
// It must not call CleanLegacyBuf until every unresolved record is durably
// acknowledged or has its complete terminal evidence retained elsewhere.
func (j *Journal) LoadLegacyBufRetained(data *Data) error {
	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if !j.IsLegacyRunning() {
		return errors.New("retained replay requires legacy ownership")
	}
	if j.legacy == nil {
		return io.EOF
	}
	err := j.legacy.Load(data)
	if err == nil {
		j.retainedUnackSeen = true
	}
	if err == io.EOF {
		j.retainedAllAcknowledged.Store(!j.retainedUnackSeen)
	}
	return err
}

// CleanLegacyBuf synchronizes current ACKs before retiring the caller-proven
// resolved frozen snapshot. It never unlocks the caller's legacy ownership.
func (j *Journal) CleanLegacyBuf() error {
	if j.option != nil && j.storageGuard != nil {
		release, e := j.storageGuard("cleanup", 64<<10)
		if e != nil {
			return e
		}
		defer release()
	}
	j.Lock()
	defer j.Unlock()
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	if !j.IsLegacyRunning() {
		return errors.New("retained cleanup requires legacy ownership")
	}
	if j.legacy == nil {
		return nil
	}
	if err := j.syncLocked(); err != nil {
		return errors.Wrap(err, "sync retained replay before cleanup")
	}
	return j.legacy.Clean()
}

// PrepareRetainedReplay rewinds only at caller-proven snapshot EOF. An empty
// active data writer is reused, so repeated sink outages do not manufacture
// empty segments. Current synchronized ACKs are included in the next snapshot.
// The caller serializes this method with its replay cursor and admission.
func (j *Journal) PrepareRetainedReplay(ctx context.Context) (bool, error) {
	j.Lock()
	select {
	case <-j.stopChan:
		j.Unlock()
		return false, os.ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		j.Unlock()
		return false, err
	}
	if j.activeDataWritten.Load() {
		j.Unlock()
		return true, j.Rotate(ctx)
	}
	if !j.LockLegacy() {
		j.Unlock()
		return false, ErrDuringRotate
	}
	defer j.UnLockLegacy()
	defer j.Unlock()
	if j.retainedACKDirty.Load() {
		if err := j.syncLocked(); err != nil {
			return false, err
		}
		j.retainedACKDirty.Store(false)
	}
	entries, err := j.disk.ReadDir(j.bufDirPath)
	if err != nil {
		return false, err
	}
	var dataNames, idNames []string
	for _, e := range entries {
		name := e.Name()
		full := filepath.Join(j.bufDirPath, name)
		if dataFileNameReg.MatchString(name) && full != j.dataFp.Name() {
			dataNames = append(dataNames, full)
		}
		if idsFileNameReg.MatchString(name) {
			idNames = append(idNames, full)
		}
	}
	if j.legacy == nil {
		return false, ErrNotStarted
	}
	j.legacy.Reset(dataNames, idNames)
	j.snapshotDataSequence.Store(j.dataSequence.Load())
	j.retainedUnackSeen = false
	j.retainedAllAcknowledged.Store(false)
	return false, nil
}

// HasRetainedData is a conservative metadata observation for a quiescent owner.
// Nonempty sealed files remain obligations until an acknowledged cleanup pass.
func (j *Journal) HasRetainedData() (bool, error) {
	j.RLock()
	defer j.RUnlock()
	if j.activeDataWritten.Load() {
		return true, nil
	}
	if j.dataFp == nil {
		return true, os.ErrClosed
	}
	entries, err := j.disk.ReadDir(j.bufDirPath)
	if err != nil {
		return true, err
	}
	for _, e := range entries {
		if dataFileNameReg.MatchString(e.Name()) && filepath.Join(j.bufDirPath, e.Name()) != j.dataFp.Name() {
			st, err := j.disk.Stat(filepath.Join(j.bufDirPath, e.Name()))
			if err != nil {
				return true, err
			}
			if st.Size() > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

// guardPreparedData runs AFTER EncodeMsg succeeded exactly once, BEFORE append.
// It never invokes a user encoder or serializes the record a second time.
func (j *Journal) guardPreparedData(n int64) (func(), error) {
	if 2*n+1024 > j.bufSizeBytes {
		return nil, errors.New("journal record exceeds bounded segment capacity")
	}
	release, err := j.storageGuard("data", n)
	if err != nil {
		return nil, err
	}
	j.dataSequence.Add(1)
	j.activeDataWritten.Store(true)
	j.activeDataEstimate.Add(2*n + 1024)
	return release, nil
}
func (j *Journal) RotateIfFull(ctx context.Context) error {
	if j.storageGuard != nil && j.activeDataEstimate.Load() >= j.bufSizeBytes {
		return j.Rotate(ctx)
	}
	return nil
}

// HasUnacknowledgedData is logical transport progress, not a durable-sink proof.
// Atomic-only observation is safe while the owner's mutation barrier is held.
func (j *Journal) HasUnacknowledgedData() bool {
	return !j.retainedAllAcknowledged.Load() || j.dataSequence.Load() != j.snapshotDataSequence.Load() || j.activeDataWritten.Load()
}

// ReadRecoveryRecords captures bounded synchronized file prefixes, then reads
// original records INCLUDING acknowledged ones. It does not mutate ACK state or
// delete any segment. Callback must not reenter journal I/O.
func (j *Journal) ReadRecoveryRecords(ctx context.Context, visit func(*Data) error) (int, error) {
	if visit == nil {
		return 0, errors.New("recovery callback required")
	}
	release := func() {}
	if j.option != nil && j.storageGuard != nil {
		var err error
		release, err = j.storageGuard("recovery", 64<<10)
		if err != nil {
			return 0, err
		}
	}
	defer release()
	j.Lock()
	if !j.LockLegacy() {
		j.Unlock()
		return 0, ErrDuringRotate
	}
	defer j.UnLockLegacy()
	if err := j.syncLocked(); err != nil {
		j.Unlock()
		return 0, err
	}
	entries, err := j.disk.ReadDir(j.bufDirPath)
	if err != nil {
		j.Unlock()
		return 0, err
	}
	type prefix struct {
		path string
		size int64
	}
	var prefixes []prefix
	for _, entry := range entries {
		if !dataFileNameReg.MatchString(entry.Name()) {
			continue
		}
		st, e := j.disk.Stat(filepath.Join(j.bufDirPath, entry.Name()))
		if e != nil {
			j.Unlock()
			return 0, e
		}
		if !st.Mode().IsRegular() {
			j.Unlock()
			return 0, errors.New("invalid retained recovery file")
		}
		if st.Size() > 0 {
			prefixes = append(prefixes, prefix{filepath.Join(j.bufDirPath, entry.Name()), st.Size()})
		}
	}
	j.Unlock()
	n := 0
	for _, p := range prefixes {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		f, e := j.disk.Open(p.path)
		if e != nil {
			return n, e
		}
		e = func() error {
			defer f.Close()
			var reader io.Reader = io.NewSectionReader(f, 0, p.size)
			if isFileGZ(p.path) {
				gz, e := gzip.NewReader(reader)
				if e != nil {
					return e
				}
				defer gz.Close()
				reader = gz
			}
			decoder := &DataDecoder{reader: msgp.NewReaderSize(reader, readBufferSize)}
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				d := new(Data)
				err := decoder.Read(d)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if err = visit(d); err != nil {
					return err
				}
				n++
			}
		}()
		if e != nil {
			return n, e
		}
	}
	return n, nil
}

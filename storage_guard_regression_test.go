package journal

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tinylib/msgp/msgp"
)

// These behavioral controls exercise exported storage/recovery contracts.
// Keep heavy process/power-loss qualification outside the fast CI job.
func guardRegressionJournal(t *testing.T, guard StorageGuard) (*Journal, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	opts := []OptionFunc{
		WithBufDirPath(t.TempDir()),
		WithBufSizeByte(1 << 20),
		WithRotateDuration(time.Hour),
		WithRotateCheckInterval(time.Hour),
		WithFlushInterval(time.Hour),
	}
	if guard != nil {
		opts = append(opts, WithStorageGuard(guard))
	}
	j, err := NewJournal(opts...)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err = j.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); j.Close() })
	return j, cancel
}

type heldGuardEncode struct {
	entered chan struct{}
	resume  chan struct{}
}

func (v heldGuardEncode) EncodeMsg(w *msgp.Writer) error {
	close(v.entered)
	<-v.resume
	return w.WriteBool(true)
}

func TestStorageGuardRegressionContracts(t *testing.T) {
	t.Run("nil_release_must_not_panic", func(t *testing.T) {
		j, _ := guardRegressionJournal(t, func(kind string, n int64) (func(), error) {
			if kind == "data" {
				return nil, nil
			}
			return func() {}, nil
		})
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("invalid guard callback caused panic: %v", r)
			}
		}()
		err := j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"value": true}})
		if err == nil {
			t.Error("nil release was silently treated as a successful grant")
		}
	})
	t.Run("successful_cleanup_releases_snapshot_obligation", func(t *testing.T) {
		j, ctxCancel := guardRegressionJournal(t, nil)
		_ = ctxCancel
		if err := j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"value": "one"}}); err != nil {
			t.Fatal(err)
		}
		if err := j.Sync(); err != nil {
			t.Fatal(err)
		}
		if rotated, err := j.PrepareRetainedReplay(context.Background()); err != nil || !rotated {
			t.Fatalf("prepare: rotated=%t err=%v", rotated, err)
		}
		if !j.LockLegacy() {
			t.Fatal("missing legacy ownership")
		}
		defer j.UnLockLegacy()
		got := new(Data)
		if err := j.LoadLegacyBufRetained(got); err != nil || got.ID != 1 {
			t.Fatalf("read: %#v %v", got, err)
		}
		if err := j.LoadLegacyBufRetained(new(Data)); err != io.EOF {
			t.Fatalf("expected EOF: %v", err)
		}
		if err := j.WriteId(1); err != nil {
			t.Fatal(err)
		}
		if err := j.CleanLegacyBuf(); err != nil {
			t.Fatal(err)
		}
		if has, err := j.HasRetainedData(); err != nil || has {
			t.Fatalf("cleanup left sealed data: %t, %v", has, err)
		}
		if j.HasUnacknowledgedData() {
			t.Error("no remaining data, but snapshot state blocks reconciliation indefinitely")
		}
	})
	t.Run("dirty_ACK_refresh_honors_guard", func(t *testing.T) {
		var deny atomic.Bool
		j, _ := guardRegressionJournal(t, func(kind string, n int64) (func(), error) {
			if kind == "sync" && deny.Load() {
				return nil, errors.New("synthetic capacity refusal")
			}
			return func() {}, nil
		})
		if err := j.WriteData(&Data{ID: 2, Data: map[string]interface{}{"value": "two"}}); err != nil {
			t.Fatal(err)
		}
		if err := j.Sync(); err != nil {
			t.Fatal(err)
		}
		if rotated, err := j.PrepareRetainedReplay(context.Background()); err != nil || !rotated {
			t.Fatalf("prepare: %t %v", rotated, err)
		}
		if err := j.WriteId(2); err != nil {
			t.Fatal(err)
		}
		deny.Store(true)
		_, err := j.PrepareRetainedReplay(context.Background())
		deny.Store(false)
		if err == nil {
			t.Error("dirty ACKs were synchronized without requesting a storage grant")
		}
	})
	t.Run("legacy_automatic_cleanup_is_guarded", func(t *testing.T) {
		j, _ := guardRegressionJournal(t, func(kind string, n int64) (func(), error) {
			if kind == "cleanup" {
				return nil, errors.New("synthetic cleanup refusal")
			}
			return func() {}, nil
		})
		if err := j.WriteData(&Data{ID: 3, Data: map[string]interface{}{"value": "three"}}); err != nil {
			t.Fatal(err)
		}
		if err := j.Sync(); err != nil {
			t.Fatal(err)
		}
		if rotated, err := j.PrepareRetainedReplay(context.Background()); err != nil || !rotated {
			t.Fatalf("prepare: %t %v", rotated, err)
		}
		old := j.fsStat.OldDataFnames[0]
		if !j.LockLegacy() {
			t.Fatal("missing legacy ownership")
		}
		if err := j.LoadLegacyBuf(new(Data)); err != nil {
			j.UnLockLegacy()
			t.Fatal(err)
		}
		err := j.LoadLegacyBuf(new(Data)) // Legacy API releases ownership on EOF/error.
		if err == nil || errors.Is(err, io.EOF) {
			t.Error("legacy EOF retired records despite denied cleanup grant")
		}
		if _, err := os.Stat(old); err != nil {
			t.Errorf("original segment removed after denied cleanup: %v", err)
		}
	})
	t.Run("shutdown_flush_honors_guard", func(t *testing.T) {
		var flushes atomic.Int32
		j, cancel := guardRegressionJournal(t, func(kind string, n int64) (func(), error) {
			if kind == "flush" {
				flushes.Add(1)
			}
			return func() {}, nil
		})
		cancel()
		j.workers.Wait()
		if flushes.Load() == 0 {
			t.Error("flush worker's shutdown path bypassed the storage guard")
		}
	})
	t.Run("close_waits_for_recovery_reader", func(t *testing.T) {
		j, _ := guardRegressionJournal(t, nil)
		if err := j.WriteData(&Data{ID: 5, Data: map[string]interface{}{"value": "five"}}); err != nil {
			t.Fatal(err)
		}
		if err := j.Sync(); err != nil {
			t.Fatal(err)
		}
		entered, resume := make(chan struct{}), make(chan struct{})
		readDone := make(chan error, 1)
		go func() {
			_, err := j.ReadRecoveryRecords(context.Background(), func(*Data) error {
				close(entered)
				<-resume
				return nil
			})
			readDone <- err
		}()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("reader never reached callback")
		}
		closeDone := make(chan struct{})
		go func() { j.Close(); close(closeDone) }()
		select {
		case <-closeDone:
			t.Error("Close invalidated the source while recovery callback was active")
		case <-time.After(150 * time.Millisecond):
		}
		close(resume)
		if err := <-readDone; err != nil {
			t.Errorf("recovery read failed during shutdown: %v", err)
		}
		select {
		case <-closeDone:
		case <-time.After(3 * time.Second):
			t.Error("close did not complete after reader returned")
		}
	})
	t.Run("writer_rotation_guard_lock_order", func(t *testing.T) {
		var lease sync.Mutex
		var armed atomic.Bool
		rotationGranted := make(chan struct{}, 1)
		guard := func(kind string, n int64) (func(), error) {
			if kind != "data" && kind != "rotation" {
				return func() {}, nil
			}
			lease.Lock()
			if kind == "rotation" && armed.Load() {
				select {
				case rotationGranted <- struct{}{}:
				default:
				}
			}
			return lease.Unlock, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		j, err := NewJournal(
			WithBufDirPath(t.TempDir()), WithBufSizeByte(1<<20),
			WithFlushInterval(time.Hour), WithRotateDuration(time.Hour),
			WithRotateCheckInterval(time.Hour), WithStorageGuard(guard),
		)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err = j.Start(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		armed.Store(true)
		// The old writer takes journal.RLock then waits inside the encoder.
		// The old rotator takes the external guard before journal.Lock.
		// New code must choose one lock order for both operations.
		value := heldGuardEncode{entered: make(chan struct{}), resume: make(chan struct{})}
		writeDone := make(chan error, 1)
		go func() {
			writeDone <- j.WriteData(&Data{ID: 9, Data: map[string]interface{}{"payload": value}})
		}()
		select {
		case <-value.entered:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("writer did not enter serialization")
		}
		rotateDone := make(chan error, 1)
		go func() { rotateDone <- j.Rotate(ctx) }()
		// If the old rotation acquired the guard, it waits for the writer's
		// journal read lock. On fixed ordering the rotation waits for that
		// read lock before attempting the guard, so there is no grant yet.
		select {
		case <-rotationGranted:
		case <-time.After(150 * time.Millisecond):
		}
		close(value.resume)
		select {
		case err = <-writeDone:
			if err != nil {
				t.Errorf("write: %v", err)
			}
		case <-time.After(750 * time.Millisecond):
			cancel()
			t.Fatal("deadlock: writer held journal read lock while rotation held storage lease")
		}
		select {
		case err = <-rotateDone:
			if err != nil {
				t.Errorf("rotation: %v", err)
			}
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("rotation did not finish")
		}
		cancel()
		j.Close()
	})
}

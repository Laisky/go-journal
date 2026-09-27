package journal

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func awaitSyncGroup(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("sync group did not finish")
	}
}

func TestSyncGroupSharesOnlyAnOverlappingBarrier(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "error"}[failure], func(t *testing.T) {
			var g syncBarrierGroup
			var lock sync.Mutex
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls int
			var expected error
			if failure {
				expected = errors.New("fsync failed")
			}
			var ownerErr error
			go func() {
				defer close(finished)
				ownerErr = g.run(&lock, func() error { calls++; close(entered); <-release; return expected })
			}()
			awaitSyncGroup(t, entered)
			// Deterministic registration, not a sleep-based guess that followers
			// started before completion. run uses this exact join primitive.
			followers := make([]*syncBarrierFlight, 32)
			for i := range followers {
				var owner bool
				followers[i], owner = g.join()
				if owner {
					t.Fatal("overlapping caller opened another barrier")
				}
				select {
				case <-followers[i].done:
					t.Fatal("returned before durable barrier completed")
				default:
				}
			}
			close(release)
			awaitSyncGroup(t, finished)
			if ownerErr != expected || calls != 1 {
				t.Fatal("owner result or barrier count changed", ownerErr, calls)
			}
			for _, f := range followers {
				awaitSyncGroup(t, f.done)
				if f.err != expected {
					t.Fatal("follower lost barrier error", f.err)
				}
			}
			for i := 0; i < 3; i++ {
				if err := g.run(&lock, func() error { calls++; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 4 {
				t.Fatal("sequential Sync reused a cached barrier", calls)
			}
		})
	}
}

type syncPublicationLock struct {
	sync.Mutex
	beforeUnlock func()
}

func (l *syncPublicationLock) Unlock() {
	l.beforeUnlock()
	l.Mutex.Unlock()
}

func TestSyncGroupPublishesBeforeWriterCanProceed(t *testing.T) {
	var g syncBarrierGroup
	var observed *syncBarrierFlight
	lock := &syncPublicationLock{beforeUnlock: func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.active != nil {
			t.Error("old barrier remains joinable after releasing writer exclusion")
		}
		select {
		case <-observed.done:
		default:
			t.Error("barrier result not published before writer exclusion ends")
		}
	}}
	if err := g.run(lock, func() error {
		var owner bool
		observed, owner = g.join()
		if owner {
			t.Fatal("lost active barrier")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncGroupAbortedOwnerUnblocksFollowers(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		t.Run(map[bool]string{false: "panic", true: "goexit"}[goexit], func(t *testing.T) {
			var g syncBarrierGroup
			var lock sync.Mutex
			var follower *syncBarrierFlight
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				defer func() { _ = recover() }()
				g.run(&lock, func() error {
					follower, _ = g.join()
					if goexit {
						runtime.Goexit()
					}
					panic("barrier panic")
				})
			}()
			awaitSyncGroup(t, finished)
			awaitSyncGroup(t, follower.done)
			if !errors.Is(follower.err, errSyncBarrierAborted) {
				t.Fatal("aborted barrier reported success", follower.err)
			}
			if err := g.run(&lock, func() error { return nil }); err != nil {
				t.Fatal("aborted owner poisoned later calls", err)
			}
		})
	}
}

func TestSyncGroupEveryReturnCoversItsCompletedWrites(t *testing.T) {
	var g syncBarrierGroup
	var journal sync.RWMutex
	var appended, durable uint64
	var violations atomic.Int64
	var calls atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 128; n++ {
				journal.RLock()
				target := atomic.AddUint64(&appended, 1)
				journal.RUnlock()
				if err := g.run(&journal, func() error {
					atomic.StoreUint64(&durable, atomic.LoadUint64(&appended))
					calls.Add(1)
					runtime.Gosched() // force overlap after the durable snapshot
					return nil
				}); err != nil || atomic.LoadUint64(&durable) < target {
					violations.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if violations.Load() != 0 || durable != 4096 || calls.Load() == 0 {
		t.Fatalf("uncovered writes=%d durable=%d barriers=%d", violations.Load(), durable, calls.Load())
	}
}

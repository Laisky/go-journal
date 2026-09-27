package journal

import (
	"errors"
	"sync"
)

var errSyncBarrierAborted = errors.New("journal sync barrier did not complete")

// syncBarrierGroup shares only an overlapping barrier, never a cached success.
// The owner MUST publish completion before releasing the journal write lock:
// otherwise a later writer could join a barrier that did not cover its data.
// There is no batching timer, worker goroutine, dirty-bit cache or weaker fsync.
type syncBarrierGroup struct {
	mu     sync.Mutex
	active *syncBarrierFlight
}

type syncBarrierFlight struct {
	done chan struct{}
	err  error // published by closing done; immutable afterwards
}

func (g *syncBarrierGroup) join() (*syncBarrierFlight, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active != nil {
		return g.active, false
	}
	f := &syncBarrierFlight{done: make(chan struct{})}
	g.active = f
	return f, true
}

func (g *syncBarrierGroup) complete(f *syncBarrierFlight, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f.err = err
	g.active = nil
	close(f.done)
}

func (g *syncBarrierGroup) run(lock sync.Locker, barrier func() error) error {
	f, owner := g.join()
	if !owner {
		<-f.done
		return f.err
	}
	// Never wait for the journal lock while holding the group mutex. Writes,
	// rotation, cleanup and close keep using the existing journal lock.
	lock.Lock()
	err := errSyncBarrierAborted
	defer func() {
		// Also release waiters on a panic/Goexit without claiming durability.
		// The owner's panic is not swallowed; followers receive a failure.
		g.complete(f, err)
		lock.Unlock()
	}()
	err = barrier()
	return err
}

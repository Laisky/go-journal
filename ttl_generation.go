package journal

import (
	"sync"
	"sync/atomic"
)

// ttlGeneration retains sync.Map's concurrent lookup path, but stores one
// stable atomic deadline cell per key. Refreshing a current-generation key
// updates that cell instead of replacing a trie entry. A temporary unpublished
// cell is still allocated per attempt; this is not an allocation-free claim.
// The caller holds Int64SetWithTTL's generation lock: current generations are
// only inserted/refreshed, old generations are only read/deleted. Consequently
// a successful refresh cannot race deletion of that cell from the same map.
// Neither a generation nor a published cell is copied, pooled, or reused.
type ttlGeneration struct {
	entries sync.Map // int64 -> *atomic.Int64
}

func newTTLGeneration() *ttlGeneration { return &ttlGeneration{} }

func (g *ttlGeneration) Load(id int64) (int64, bool) {
	value, ok := g.entries.Load(id)
	if !ok {
		return 0, false
	}
	return value.(*atomic.Int64).Load(), true
}

func (g *ttlGeneration) Swap(id, deadline int64) (int64, bool) {
	// One lookup for cold insertion. An unpublished candidate cell on a refresh
	// is a bounded allocation tradeoff, measured against both cold and hot work.
	cell := new(atomic.Int64)
	cell.Store(deadline) // fully initialized before publication
	value, loaded := g.entries.LoadOrStore(id, cell)
	if loaded {
		// Another insertion won; publish this refresh in the winning cell.
		return value.(*atomic.Int64).Swap(deadline), true
	}
	return 0, false
}

func (g *ttlGeneration) Store(id, deadline int64) { g.Swap(id, deadline) }

func (g *ttlGeneration) LoadAndDelete(id int64) (int64, bool) {
	value, loaded := g.entries.LoadAndDelete(id)
	if !loaded {
		return 0, false
	}
	return value.(*atomic.Int64).Load(), true
}

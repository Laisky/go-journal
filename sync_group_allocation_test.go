package journal

import (
	"sync"
	"testing"
)

func TestSyncGroupUncontendedDoesNotAllocate(t *testing.T) {
	var group syncBarrierGroup
	var lock sync.Mutex
	barrier := func() error { return nil }
	allocations := testing.AllocsPerRun(1000, func() {
		if err := group.run(&lock, barrier); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("uncontended coordination allocated %.1f objects per call", allocations)
	}
}

func TestSyncGroupRetiresGenerationBeforeUnlock(t *testing.T) {
	var group syncBarrierGroup
	lock := &syncPublicationLock{beforeUnlock: func() {
		group.mu.Lock()
		defer group.mu.Unlock()
		if group.running || group.active != nil {
			t.Error("completed generation remains joinable after unlocking")
		}
	}}
	if err := group.run(lock, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSyncGroupUncontended(b *testing.B) {
	var group syncBarrierGroup
	var lock sync.Mutex
	barrier := func() error { return nil }
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := group.run(&lock, barrier); err != nil {
			b.Fatal(err)
		}
	}
}

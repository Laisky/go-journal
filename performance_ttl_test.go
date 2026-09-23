package journal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

func TestPerformanceContractConcurrentRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := journal.NewInt64SetWithTTL(ctx, time.Hour)
	defer s.Close()
	const unique = 1024
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 4; round++ {
				for id := 0; id < unique; id++ {
					s.AddInt64(int64(id))
				}
			}
		}()
	}
	wg.Wait()
	if s.GetLen() != unique {
		t.Fatalf("duplicate refresh changed cardinality: %d", s.GetLen())
	}
	for repeat := 0; repeat < 3; repeat++ {
		for id := 0; id < unique; id++ {
			if !s.CheckAndRemove(int64(id)) {
				t.Fatalf("confirmation was consumed for ID %d", id)
			}
		}
	}
	if s.CheckAndRemove(-1) || s.GetLen() != unique {
		t.Fatal("lookup changed membership/cardinality")
	}
}

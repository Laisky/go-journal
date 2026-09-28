package journal_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Both implementations execute these public contracts without source adapters.
func TestTTLGenerationPublicExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := journal.NewInt64SetWithTTL(ctx, time.Second)
		synctest.Wait() // establish the generation ticker at virtual t=0
		time.Sleep(750 * time.Millisecond)
		s.AddInt64(17)
		s.AddInt64(18)
		time.Sleep(250 * time.Millisecond)
		synctest.Wait() // old generation still has 750 ms of valid lifetime
		s.Close()
		synctest.Wait()
		if !s.CheckAndRemove(17) || !s.CheckAndRemove(17) {
			t.Fatal("old membership consumed before expiry")
		}
		time.Sleep(750 * time.Millisecond)
		if s.CheckAndRemove(17) {
			t.Fatal("expired generation accepted")
		}
		if s.CheckAndRemove(17) || s.GetLen() != 1 {
			t.Fatal("expiration cardinality changed")
		}
		s.AddInt64(18)
		if !s.CheckAndRemove(18) || s.GetLen() != 2 {
			t.Fatal("current generation must shadow expired old entry")
		}
	})
}

func TestTTLGenerationPublicConcurrentRefresh(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := journal.NewInt64SetWithTTL(ctx, time.Hour)
	defer s.Close()
	const keys = 1024
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < keys*4; n++ {
				id := int64(n % keys)
				s.AddInt64(id)
				if !s.CheckAndRemove(id) {
					t.Error("completed refresh not visible")
					return
				}
			}
		}()
	}
	wg.Wait()
	if s.GetLen() != keys {
		t.Fatalf("duplicate refresh counted twice: %d", s.GetLen())
	}
}

func TestTTLGenerationPublicSignedKeysAndRotations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := journal.NewInt64SetWithTTL(ctx, time.Second)
		defer s.Close()
		synctest.Wait()
		keys := []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}
		for _, id := range keys {
			s.AddInt64(id)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		for _, id := range keys {
			if s.CheckAndRemove(id) {
				t.Fatal("expired generation accepted")
			}
			s.AddInt64(id)
			if !s.CheckAndRemove(id) || !s.CheckAndRemove(id) {
				t.Fatal("signed-key refresh lost")
			}
		}
		if s.GetLen() != len(keys) {
			t.Fatal("retired generation count survived deletion")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if s.GetLen() != 0 {
			t.Fatal("two rotations did not retire old storage")
		}
	})
}

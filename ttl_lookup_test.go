package journal

import (
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Current-generation membership is deliberately not consumptive and does not
// inspect deadlines; the generation rotation defines when expiration matters.
// These contracts execute unchanged against both revisions.
func TestTTLLookupCurrentGenerationAndMisses(t *testing.T) {
	s := &Int64SetWithTTL{ng: newTTLGeneration(), og: newTTLGeneration()}
	for _, id := range []int64{math.MinInt64, 0, 1, math.MaxInt64} {
		s.ng.Store(id, int64(1)) // even a past deadline remains valid in current generation
		s.ngN++
	}
	s.og.Store(int64(20), int64(1))
	s.ogN = 1
	for n := 0; n < 3; n++ {
		for _, id := range []int64{math.MinInt64, 0, 1, math.MaxInt64} {
			if !s.CheckAndRemove(id) {
				t.Fatalf("current ACK consumed or expired: %d", id)
			}
		}
		if s.CheckAndRemove(20) {
			t.Fatal("expired old ACK accepted")
		}
		if s.CheckAndRemove(100) {
			t.Fatal("unknown ACK accepted")
		}
		if s.GetLen() != 4 {
			t.Fatalf("cardinality=%d", s.GetLen())
		}
	}
	s.og = nil
	if s.CheckAndRemove(100) {
		t.Fatal("missing old generation invented an ACK")
	}
}

func TestTTLLookupOldGenerationDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Int64SetWithTTL{ng: newTTLGeneration(), og: newTTLGeneration(), ogN: 2}
		deadline := time.Now().Add(250 * time.Millisecond).UnixNano()
		s.og.Store(int64(17), deadline)
		s.og.Store(int64(18), deadline)
		if !s.CheckAndRemove(17) || !s.CheckAndRemove(17) {
			t.Fatal("live old ACK consumed")
		}
		time.Sleep(250 * time.Millisecond)
		if s.CheckAndRemove(17) {
			t.Fatal("expired old ACK accepted")
		}
		if s.CheckAndRemove(17) || s.GetLen() != 1 {
			t.Fatal("expiry removal count changed")
		}
		s.ng.Store(int64(18), deadline)
		s.ngN = 1
		if !s.CheckAndRemove(18) || s.GetLen() != 2 {
			t.Fatal("current generation must shadow old expiry")
		}
	})
}

func TestTTLLookupConcurrentExpirationAndCurrentHits(t *testing.T) {
	const count, workers = 512, 8
	s := &Int64SetWithTTL{ng: newTTLGeneration(), og: newTTLGeneration(), ogN: count, ngN: count}
	for i := 0; i < count; i++ {
		s.og.Store(int64(i), int64(1))
		s.ng.Store(int64(i+count), int64(1))
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < count; i++ {
				if s.CheckAndRemove(int64(i)) {
					t.Error("expired old ACK accepted")
				}
				if !s.CheckAndRemove(int64(i + count)) {
					t.Error("current ACK disappeared")
				}
			}
		}()
	}
	wg.Wait()
	if s.GetLen() != count {
		t.Fatalf("concurrent expiration cardinality=%d", s.GetLen())
	}
}

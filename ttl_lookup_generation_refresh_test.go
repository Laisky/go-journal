package journal_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Membership in the current generation alone cannot prove refresh correctness:
// inspect it after rotation, when the refreshed deadline must still be live.
func TestTTLGenerationPublicRefreshDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := journal.NewInt64SetWithTTL(ctx, time.Second)
		synctest.Wait()
		s.AddInt64(7)
		time.Sleep(750 * time.Millisecond)
		s.AddInt64(7)
		if s.GetLen() != 1 {
			t.Fatal("refresh changed unique count")
		}
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		s.Close()
		synctest.Wait()
		time.Sleep(250 * time.Millisecond)
		if !s.CheckAndRemove(7) || !s.CheckAndRemove(7) {
			t.Fatal("refreshed deadline lost after rotation")
		}
		time.Sleep(500 * time.Millisecond)
		if s.CheckAndRemove(7) || s.GetLen() != 0 {
			t.Fatal("refreshed deadline failed exact expiration")
		}
	})
}

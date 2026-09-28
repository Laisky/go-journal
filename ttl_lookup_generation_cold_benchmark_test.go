package journal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Cold batches include construction, 8192 unique AddInt64 calls and Close.
// No populated-map setup is excluded: a refresh-only benchmark cannot justify
// the cost of inserting a new ACK generation. This is not durable throughput.
func BenchmarkPublicTTLColdInsert(b *testing.B) {
	for _, c := range []struct {
		name    string
		workers int
	}{{"cold-serial", 1}, {"cold-parallel8", 8}} {
		b.Run(c.name, func(b *testing.B) {
			b.StopTimer()
			if err := journal.Logger.ChangeLevel("error"); err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			const count = 8192
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				s := journal.NewInt64SetWithTTL(ctx, time.Hour)
				var wg sync.WaitGroup
				for w := 0; w < c.workers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for id := w; id < count; id += c.workers {
							s.AddInt64(int64(id))
						}
					}(w)
				}
				wg.Wait()
				s.Close()
				if s.GetLen() != count {
					b.Fatal("cold insertion cardinality changed")
				}
			}
			b.StopTimer()
		})
	}
}

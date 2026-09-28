package journal_test

import (
	"context"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Each fixed batch has 8192 refreshes and 8192 membership queries. The hot-key
// case deliberately contends on a single shard; it must not be hidden by the
// disjoint-key case. This is not a durability or delivery benchmark.
func BenchmarkPublicTTLGeneration(b *testing.B) {
	for _, c := range []struct {
		name    string
		workers int
		hot     bool
	}{
		{"refresh-serial", 1, false}, {"refresh-parallel8", 8, false},
		{"refresh-parallel32", 32, false}, {"hot-key32", 32, true},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.StopTimer()
			if err := journal.Logger.ChangeLevel("error"); err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := journal.NewInt64SetWithTTL(ctx, time.Hour)
			defer s.Close()
			const count = 8192
			keys := count
			if c.hot {
				keys = 1
			}
			for i := 0; i < keys; i++ {
				s.AddInt64(int64(i))
			}
			b.ReportAllocs()
			b.ResetTimer()
			pprof.Do(ctx, pprof.Labels("phase", "public-ttl-generation"), func(context.Context) {
				b.StartTimer()
				var wg sync.WaitGroup
				for w := 0; w < c.workers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for n := 0; n < b.N; n++ {
							for i := w; i < count; i += c.workers {
								id := int64(i)
								if c.hot {
									id = 0
								}
								s.AddInt64(id)
								if !s.CheckAndRemove(id) {
									b.Error("refreshed ACK missing")
									return
								}
							}
						}
					}(w)
				}
				wg.Wait()
				b.StopTimer()
			})
			if s.GetLen() != keys {
				b.Fatal("refresh cardinality changed", s.GetLen())
			}
		})
	}
}

package journal_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Each serial operation contains 8192 public membership queries. Parallel
// operations contain four times that work. Setup and cancellation are excluded.
// This isolates CPU work; it is not a persistence or delivery benchmark.
func BenchmarkPublicACKMembership(b *testing.B) {
	for _, name := range []string{"current-hits", "no-old-misses", "parallel-hits"} {
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			if err := journal.Logger.ChangeLevel("error"); err != nil {
				b.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := journal.NewInt64SetWithTTL(ctx, time.Hour)
			defer s.Close()
			const count = 8192
			for i := 0; i < count; i++ {
				s.AddInt64(int64(i))
			}
			workers := 1
			if name == "parallel-hits" {
				workers = 4
			}
			missing := name == "no-old-misses"
			b.ReportAllocs()
			b.ResetTimer()
			pprof.Do(ctx, pprof.Labels("phase", "public-ack-membership"), func(context.Context) {
				b.StartTimer()
				var wg sync.WaitGroup
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for n := 0; n < b.N; n++ {
							for id := 0; id < count; id++ {
								key := int64(id)
								if missing {
									key += count
								}
								if s.CheckAndRemove(key) == missing {
									b.Error("membership changed")
									return
								}
							}
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
			})
			if s.GetLen() != count {
				b.Fatal("membership count changed")
			}
		})
	}
}

// This exercises the exported recovery loader over sealed, fsynced files. Each
// operation resets and exhausts the same snapshot. Pending records are verified
// exactly; repeated scans are not counted as new downstream deliveries.
func BenchmarkPublicACKReplay(b *testing.B) {
	for _, c := range []struct {
		name      string
		count     int
		all, gzip bool
	}{
		{"all-acked", 8192, true, false}, {"sparse-acked", 8192, false, false}, {"gzip-control", 1024, true, true},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.StopTimer()
			if err := journal.Logger.ChangeLevel("error"); err != nil {
				b.Fatal(err)
			}
			dir := b.TempDir()
			suffix := ""
			if c.gzip {
				suffix = ".gz"
			}
			dataName := filepath.Join(dir, "data"+suffix)
			ackName := filepath.Join(dir, "ids"+suffix)
			fp, err := os.Create(dataName)
			if err != nil {
				b.Fatal(err)
			}
			ap, err := os.Create(ackName)
			if err != nil {
				fp.Close()
				b.Fatal(err)
			}
			data, err := journal.NewDataEncoder(fp, c.gzip)
			if err != nil {
				b.Fatal(err)
			}
			ack, err := journal.NewIdsEncoder(ap, c.gzip)
			if err != nil {
				b.Fatal(err)
			}
			for id := 1; id <= c.count; id++ {
				if err = data.Write(&journal.Data{ID: int64(id), Data: map[string]interface{}{"body": "checked"}}); err != nil {
					b.Fatal(err)
				}
				if c.all || id%2 == 0 {
					if err = ack.Write(int64(id)); err != nil {
						b.Fatal(err)
					}
				}
			}
			for _, err := range []error{data.Close(), ack.Close(), fp.Sync(), ap.Sync(), fp.Close(), ap.Close()} {
				if err != nil {
					b.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			loader := journal.NewLegacyLoader(ctx, journal.Logger, []string{dataName}, []string{ackName}, c.gzip, time.Hour)
			b.ReportAllocs()
			b.ResetTimer()
			pprof.Do(ctx, pprof.Labels("phase", "public-ack-replay"), func(context.Context) {
				b.StartTimer()
				for n := 0; n < b.N; n++ {
					loader.Reset([]string{dataName}, []string{ackName})
					pending := 0
					var d journal.Data
					for {
						err := loader.Load(&d)
						if err == io.EOF {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
						pending++
						if c.all || d.ID != int64(2*pending-1) || d.Data["body"] != "checked" {
							b.Fatal("wrong pending recovery record", d.ID)
						}
					}
					expected := 0
					if !c.all {
						expected = c.count / 2
					}
					if pending != expected {
						b.Fatal(fmt.Sprint("pending=", pending, " want=", expected))
					}
				}
				b.StopTimer()
			})
			if err := loader.Clean(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

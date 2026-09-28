package journal_test

import (
	"context"
	"fmt"
	"io"
	"runtime/pprof"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Full public-API recovery scan over real ACK files. Fixture construction uses
// WriteId -> Sync -> Rotate and Close/reopen, not private files/decoder helpers.
// Timed work is only repeated LoadMaxId; setup and final cleanup are excluded.
// ACK-only history represents already-completed downstream operations, not new
// message throughput. Warm-cache fixed-work scans are not a production SLO.
func BenchmarkPublicACKFrontier(b *testing.B) {
	for _, c := range []struct {
		name            string
		count, segments int
		gzip            bool
	}{
		{"plain-8k", 8192, 1, false}, {"plain-128k", 131072, 1, false},
		{"plain-128k-segments", 131072, 8, false}, {"gzip-control", 2048, 1, true},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.StopTimer()
			if err := journal.Logger.ChangeLevel("error"); err != nil {
				b.Fatal(err)
			}
			dir := b.TempDir()
			open := func() *journal.Journal {
				j, err := journal.NewJournal(journal.WithBufDirPath(dir), journal.WithIsCompress(c.gzip), journal.WithIsAggresiveGC(false), journal.WithBufSizeByte(65536), journal.WithFlushInterval(time.Hour), journal.WithRotateCheckInterval(time.Hour))
				if err != nil {
					b.Fatal(err)
				}
				if err = j.Start(context.Background()); err != nil {
					b.Fatal(err)
				}
				return j
			}
			j := open()
			for n := 0; n < c.count; n++ {
				// An old file owns the highest ID; decreasing values exercise signed deltas.
				if err := j.WriteId(int64(c.count - n)); err != nil {
					b.Fatal(err)
				}
				if (n+1)%(c.count/c.segments) == 0 {
					if err := j.Sync(); err != nil {
						b.Fatal(err)
					}
					if err := j.Rotate(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
			}
			j.Close()
			j = open()
			defer j.Close()
			if high, err := j.LoadMaxId(); err != nil || high != int64(c.count) {
				b.Fatal("pre-scan frontier", high, err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(c.count * 8))
			b.ResetTimer()
			pprof.Do(context.Background(), pprof.Labels("phase", "public-ack-scan"), func(context.Context) {
				b.StartTimer()
				for n := 0; n < b.N; n++ {
					if high, err := j.LoadMaxId(); err != nil || high != int64(c.count) {
						b.Fatal("scan frontier", high, err)
					}
				}
				b.StopTimer()
			})
			if !j.LockLegacy() {
				b.Fatal("missing lease")
			}
			if err := j.LoadLegacyBuf(new(journal.Data)); err != io.EOF {
				b.Fatal("invented pending record", err)
			}
			j.UnLockLegacy()
			j.Close()
			j = open()
			if high, err := j.LoadMaxId(); err != nil || high != int64(c.count) {
				b.Fatal(fmt.Sprint("cleanup/reopen frontier ", high, err))
			}
			j.Close()
		})
	}
}

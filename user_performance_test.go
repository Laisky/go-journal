package journal_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	journal "github.com/Laisky/go-journal"
)

// Same caller workload on both revisions. Setup, warm-up and final cleanup are
// excluded; Write (and, where requested, encoder Flush plus file Sync) stay timed.
func BenchmarkUserJournalAppend(b *testing.B) {
	for _, gz := range []bool{false, true} {
		for _, size := range []int{2048, 65536, (4 << 20) + 17} {
			for _, durable := range []bool{false, true} {
				if durable && size != 2048 {
					continue
				}
				b.Run(fmt.Sprintf("gzip=%v/bytes=%d/sync=%v", gz, size, durable), func(b *testing.B) {
					fp, err := os.CreateTemp(b.TempDir(), "records")
					if err != nil {
						b.Fatal(err)
					}
					defer fp.Close()
					enc, err := journal.NewDataEncoder(fp, gz)
					if err != nil {
						b.Fatal(err)
					}
					defer func() {
						if err := enc.Close(); err != nil {
							b.Fatal(err)
						}
					}()
					body := make([]byte, size)
					rng := rand.New(rand.NewSource(127))
					_, _ = rng.Read(body)
					record := &journal.Data{ID: 1, Data: map[string]interface{}{"payload": body, "source": "benchmark"}}
					if err = enc.Write(record); err != nil {
						b.Fatal(err)
					}
					if err = enc.Flush(); err != nil {
						b.Fatal(err)
					}
					if err = fp.Sync(); err != nil {
						b.Fatal(err)
					}
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						record.ID = int64(n + 2)
						if err = enc.Write(record); err != nil {
							b.Fatal(err)
						}
						if durable {
							if err = enc.Flush(); err != nil {
								b.Fatal(err)
							}
							if err = fp.Sync(); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
				})
			}
		}
	}
}

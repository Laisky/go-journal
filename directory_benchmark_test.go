package journal_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	journal "github.com/Laisky/go-journal"
)

// The complete exported snapshot/preparation operation, not an in-memory
// filename parser. Each iteration enumerates/validates all files, chooses the
// next frontier, creates two real files, then closes/removes them. Persistent
// fixtures represent sealed segments. Directory caches are intentionally warm.
func BenchmarkPublicDirectorySnapshot(b *testing.B) {
	for _, count := range []int{16, 256, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			b.StopTimer()
			if err := journal.Logger.ChangeLevel("error"); err != nil {
				b.Fatal(err)
			}
			dir := b.TempDir()
			for n := 0; n < count/2; n++ {
				for _, suffix := range []string{"buf", "ids"} {
					name := fmt.Sprintf("20990101_%08d.%s", n+1, suffix)
					if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				s, err := journal.PrepareNewBufFile(dir, nil, true, false, 0)
				if err != nil {
					b.Fatal(err)
				}
				if len(s.OldDataFnames) != count/2 || len(s.OldIDsDataFnames) != count/2 {
					b.Fatal("changed snapshot work")
				}
				for _, fp := range []*os.File{s.NewDataFp, s.NewIDsFp} {
					if err := fp.Close(); err != nil {
						b.Fatal(err)
					}
					if err := os.Remove(fp.Name()); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
		})
	}
}

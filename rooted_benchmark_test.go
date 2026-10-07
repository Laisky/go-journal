package journal

import (
	"os"
	"path/filepath"
	"testing"
)

// Keep the same open/stat/close work on all three paths. Comparing the direct
// Root API with the journal adapter separates confinement cost from adapter
// overhead without caching descriptors or skipping filesystem observations.
func BenchmarkJournalFilesystemOpen(b *testing.B) {
	dir := b.TempDir()
	name := filepath.Join(dir, "record")
	if err := os.WriteFile(name, []byte("record"), 0600); err != nil {
		b.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer root.Close()
	adapter := rootedFS{root: root}
	for _, disk := range []struct {
		name string
		open func(string) (*os.File, error)
	}{
		{"path", os.Open},
		{"root-direct", func(string) (*os.File, error) { return root.Open("record") }},
		{"root-adapter", adapter.Open},
	} {
		b.Run(disk.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				fp, err := disk.open(name)
				if err != nil {
					b.Fatal(err)
				}
				info, statErr := fp.Stat()
				closeErr := fp.Close()
				if statErr != nil || closeErr != nil || info.Size() != 6 {
					b.Fatalf("stat=%v close=%v info=%v", statErr, closeErr, info)
				}
			}
		})
	}
}

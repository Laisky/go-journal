//go:build linux

package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	journal "github.com/Laisky/go-journal"
)

func TestSeedRotationProducesDistinctSegments(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("gzip=%t", compressed), func(t *testing.T) {
			dir := t.TempDir()
			j, err := journal.NewJournal(journal.WithBufDirPath(dir), journal.WithIsCompress(compressed), journal.WithIsAggresiveGC(false))
			if err != nil {
				t.Fatal(err)
			}
			if err := j.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			o := options{Mode: "seed", Count: 65, Payload: 32, Writers: 1, AckPercent: 0, RotateEvery: 16}
			r := new(result)
			if err := seed(j, o, r, http.DefaultClient); err != nil {
				t.Fatal(err)
			}
			if r.Rotations != 4 {
				t.Fatalf("reported rotations: %d", r.Rotations)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			segments := 0
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".buf") || strings.HasSuffix(entry.Name(), ".buf.gz") {
					info, err := os.Stat(filepath.Join(dir, entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
					if info.Size() > 0 {
						segments++
					}
				}
			}
			if segments != 5 {
				t.Fatalf("actual nonempty segment count: got %d, want 5", segments)
			}
			if err := j.Rotate(context.Background()); err != nil {
				t.Fatal(err)
			}
			if high, err := j.LoadMaxId(); high != 65 || err != nil {
				t.Fatal("frontier", high, err)
			}
			if !j.LockLegacy() {
				t.Fatal("lease")
			}
			defer j.UnLockLegacy()
			seen := make(map[int64]bool)
			for {
				var d journal.Data
				err := j.LoadLegacyBuf(&d)
				if err == io.EOF {
					break
				}
				if err != nil || seen[d.ID] || validate(&d, 32) != nil {
					t.Fatal("segment replay changed", d.ID, err)
				}
				seen[d.ID] = true
				if err := j.WriteData(&d); err != nil {
					t.Fatal(err)
				}
				if err := j.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if len(seen) != 65 {
				t.Fatal("missing replayed records", len(seen))
			}
		})
	}
}

package journal_test

import (
	"context"
	"fmt"
	journal "github.com/Laisky/go-journal"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalBorrowedRootSurvivesReplacementBeforeStart(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			base := t.TempDir()
			active, retained, outside := filepath.Join(base, "active"), filepath.Join(base, "retained"), filepath.Join(base, "outside")
			behaviorCheck(t, os.Mkdir(active, 0700))
			behaviorCheck(t, os.Mkdir(outside, 0700))
			root, err := os.OpenRoot(active)
			behaviorCheck(t, err)
			j, err := journal.NewJournal(journal.WithRoot(root), journal.WithBufSizeByte(4096), journal.WithIsCompress(gz), journal.WithIsAggresiveGC(false))
			behaviorCheck(t, err)
			defer j.Close()
			behaviorCheck(t, root.Close())
			behaviorCheck(t, os.Rename(active, retained))
			behaviorCheck(t, os.Symlink(outside, active))
			behaviorCheck(t, j.Start(context.Background()))
			behaviorCheck(t, j.WriteData(behaviorData(73)))
			behaviorCheck(t, j.Sync())
			behaviorCheck(t, j.Rotate(context.Background()))
			got := behaviorReplay(t, j)
			if len(got) != 1 || got[73] == nil {
				t.Fatal(got)
			}
			j.Close()
			files, err := os.ReadDir(outside)
			behaviorCheck(t, err)
			if len(files) != 0 {
				t.Fatal("borrowed root lost before start")
			}
			again := behaviorStart(t, retained, gz)
			got = behaviorReplay(t, again)
			if len(got) != 1 || got[73] == nil {
				t.Fatal(got)
			}
		})
	}
}

func TestJournalRootOptionsValidateWithoutConsumingHandle(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	behaviorCheck(t, err)
	defer root.Close()
	for _, options := range [][]journal.OptionFunc{{journal.WithRoot(nil)}, {journal.WithRoot(root), journal.WithBufSizeByte(-1)}} {
		j, err := journal.NewJournal(options...)
		if err == nil {
			j.Close()
			t.Fatal("invalid option accepted")
		}
	}
	_, err = root.Stat(".")
	behaviorCheck(t, err)
	behaviorCheck(t, root.Close())
	j, err := journal.NewJournal(journal.WithRoot(root))
	if err == nil {
		j.Close()
		t.Fatal("closed root accepted")
	}
}

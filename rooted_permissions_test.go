//go:build linux || darwin

package journal_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	journal "github.com/Laisky/go-journal"
)

// Umask is process-wide. Change it only in an isolated helper process, never
// around concurrent journal goroutines in the parent test binary.
func TestJournalPrivateCreationWithUmask(t *testing.T) {
	if value := os.Getenv("JOURNAL_PRIVATE_UMASK"); value != "" {
		mask, err := strconv.ParseInt(value, 8, 32)
		behaviorCheck(t, err)
		syscall.Umask(int(mask))
		for _, gz := range []bool{false, true} {
			dir := filepath.Join(t.TempDir(), "private")
			behaviorCheck(t, journal.PrepareDir(dir))
			info, err := os.Stat(dir)
			behaviorCheck(t, err)
			if info.Mode().Perm() != 0700 {
				t.Fatalf("directory mode %#o", info.Mode().Perm())
			}
			j := behaviorStart(t, dir, gz)
			behaviorCheck(t, j.WriteData(behaviorData(1)))
			behaviorCheck(t, j.WriteId(1))
			behaviorCheck(t, j.Sync())
			behaviorCheck(t, j.Rotate(t.Context()))
			j.Close()
			again := behaviorStart(t, dir, gz)
			again.Close()
			files, err := os.ReadDir(dir)
			behaviorCheck(t, err)
			if len(files) < 3 {
				t.Fatal("empty mode fixture")
			}
			for _, f := range files {
				i, err := f.Info()
				behaviorCheck(t, err)
				if i.Mode().Perm() != 0600 {
					t.Fatalf("%s mode %#o", f.Name(), i.Mode().Perm())
				}
			}
			// PrepareDir and normal journal operations must not leave a wider umask.
			probe := filepath.Join(t.TempDir(), "unrelated")
			behaviorCheck(t, os.WriteFile(probe, nil, 0666))
			p, err := os.Stat(probe)
			behaviorCheck(t, err)
			if p.Mode().Perm() != os.FileMode(0666 & ^int(mask)) {
				t.Fatal("journal changed process umask")
			}
		}
		fmt.Println("PRIVATE_MODE_CHILD_EXECUTED")
		return
	}
	for _, mask := range []string{"0000", "0077"} {
		t.Run(mask, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestJournalPrivateCreationWithUmask$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "JOURNAL_PRIVATE_UMASK="+mask)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("mode helper: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), "PRIVATE_MODE_CHILD_EXECUTED") {
				t.Fatal("helper was not executed")
			}
		})
	}
}

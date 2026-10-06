//go:build linux || darwin

package journal_test

import (
	"context"
	"fmt"
	journal "github.com/Laisky/go-journal"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// Changing umask is process-wide: each case runs in its own test subprocess.
// The fixture uses tiny synthetic records and public durable/replay APIs.
func TestRegressionPrivateSegmentCreation(t *testing.T) {
	if mask := os.Getenv("JOURNAL_PERMISSION_MASK"); mask != "" {
		n, err := strconv.ParseUint(mask, 8, 32)
		if err != nil {
			t.Fatal(err)
		}
		syscall.Umask(int(n))
		for _, gz := range []bool{false, true} {
			t.Run(fmt.Sprint(gz), func(t *testing.T) {
				root := filepath.Join(t.TempDir(), "private")
				if err := journal.PrepareDir(root); err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(root)
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0700 {
					t.Errorf("PRIVATE_MODE_REGRESSION: new directory mode=%04o", info.Mode().Perm())
				}
				j := behaviorStart(t, root, gz)
				d := behaviorData(71)
				behaviorCheck(t, j.WriteData(d))
				behaviorCheck(t, j.WriteData(behaviorData(72)))
				behaviorCheck(t, j.WriteId(72))
				behaviorCheck(t, j.Sync())
				behaviorCheck(t, j.Rotate(context.Background()))
				j.Close()
				j = behaviorStart(t, root, gz)
				got := behaviorReplay(t, j)
				if len(got) != 1 || got[71] == nil {
					t.Fatal("permission change broke replay/ACKs", got)
				}
				behaviorCheck(t, j.Sync())
				j.Close()
				entries, err := os.ReadDir(root)
				if err != nil {
					t.Fatal(err)
				}
				segments := 0
				for _, e := range entries {
					if !strings.Contains(e.Name(), ".buf") && !strings.Contains(e.Name(), ".ids") {
						continue
					}
					segments++
					info, err := e.Info()
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != 0600 {
						t.Errorf("PRIVATE_MODE_REGRESSION: %s mode=%04o", e.Name(), info.Mode().Perm())
					}
				}
				if segments < 2 {
					t.Fatal("no data/ack segment control")
				}
				old := syscall.Umask(int(n))
				syscall.Umask(old)
				if old != int(n) {
					t.Fatal("PrepareDir mutated process umask")
				}
			})
		}
		return
	}
	for _, mask := range []string{"0000", "0022", "0077"} {
		t.Run(mask, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRegressionPrivateSegmentCreation$", "-test.v")
			cmd.Env = append(os.Environ(), "JOURNAL_PERMISSION_MASK="+mask)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("PRIVATE_MODE_REGRESSION: isolated mask=%s: %v\n%s", mask, err, out)
			}
		})
	}
}

func TestPermissionPolicyNeverChmodsExistingEvidence(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "historical-evidence")
	if err := os.WriteFile(marker, []byte("retain"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := journal.PrepareDir(dir); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(dir)
	if info.Mode().Perm() != 0755 {
		t.Fatal("silently changed existing directory")
	}
	b, err := os.ReadFile(marker)
	if err != nil || string(b) != "retain" {
		t.Fatal("changed historical evidence")
	}
}

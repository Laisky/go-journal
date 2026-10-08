package journal

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// Run the actual public benchmarks with multiple calibrated iterations. This
// catches shared replay leases, missing loops, fixed paths and invalid direct IO.
func TestLegacyBenchmarksCompleteBoundedWork(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$", "-test.bench=^(BenchmarkJournal|BenchmarkFSPreallocate)$", "-test.benchtime=4x", "-test.timeout=30s")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bounded journal/filesystem benchmarks failed: %v\n%s", err, out)
	}
	for _, name := range []string{"BenchmarkJournal/store", "BenchmarkJournal/load", "BenchmarkFSPreallocate/normal", "BenchmarkFSPreallocate/preallocate"} {
		if !strings.Contains(string(out), name) {
			t.Errorf("benchmark missing: %s\n%s", name, out)
		}
	}
	if len(regexp.MustCompile(`1(?:[.]0+)? records/op`).FindAllString(string(out), -1)) != 2 {
		t.Fatalf("benchmarks did not verify one record for each requested operation:\n%s", out)
	}
}

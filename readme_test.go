package journal_test

import (
	"bytes"
	"context"
	"go/format"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Compile the actual README block, not a separately maintained example. Its
// public-API assertions must succeed for both supported serialization modes.
func TestReadmeQuickstart(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	blocks := regexp.MustCompile("(?ms)^```go\\n(.*?)^```[ \\t]*$").FindAllSubmatch(readme, -1)
	if len(blocks) != 1 {
		t.Fatalf("expected one complete Go quickstart block, found %d", len(blocks))
	}
	source := blocks[0][1]
	formatted, err := format.Source(source)
	if err != nil {
		t.Fatalf("quickstart syntax: %v", err)
	}
	if !bytes.Equal(source, formatted) {
		t.Fatal("README Go block is not gofmt-formatted")
	}
	dir := t.TempDir()
	mainFile := filepath.Join(dir, "main.go")
	if err := os.WriteFile(mainFile, source, 0600); err != nil {
		t.Fatal(err)
	}
	goBin := filepath.Join(runtime.GOROOT(), "bin", "go")
	executable := filepath.Join(dir, "quickstart")
	if runtime.GOOS == "windows" {
		goBin += ".exe"
		executable += ".exe"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, goBin, "build", "-mod=readonly", "-o", executable, mainFile)
	// Use this checkout's native graph; do not allow an inherited workspace or
	// GOFLAGS override to silently select a different implementation.
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=", "GOTOOLCHAIN=local")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build README consumer: %v\n%s", err, output)
	}
	for _, mode := range []string{"plain", "gzip"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{}
			if mode == "gzip" {
				args = append(args, "-gzip")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("run README consumer: %v\n%s", err, output)
			}
			var lines []string
			for _, line := range strings.Split(string(output), "\n") {
				if strings.HasPrefix(line, "example:") {
					lines = append(lines, line)
				}
			}
			want := "example: recovered maximum ID = 2\nexample: pending record = 2 (task-2)"
			if got := strings.Join(lines, "\n"); got != want {
				t.Fatalf("README output mismatch:\ngot: %s\nwant: %s\nfull output:\n%s", got, want, output)
			}
		})
	}
}

// Local documentation links and navigation should work in a fresh checkout.
// External URLs and live badges are intentionally not network-tested by CI.
func TestReadmeLocalLinks(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	text := regexp.MustCompile("(?ms)^```[^\\n]*\\n.*?^```[ \\t]*$").ReplaceAll(readme, nil)
	headings := regexp.MustCompile(`(?m)^#{1,6} +(.+)$`)
	punctuation := regexp.MustCompile(`[^a-z0-9_ -]`)
	anchors := map[string]bool{}
	for _, match := range headings.FindAllSubmatch(text, -1) {
		anchor := strings.ToLower(string(match[1]))
		anchor = punctuation.ReplaceAllString(anchor, "")
		anchors[strings.ReplaceAll(anchor, " ", "-")] = true
	}
	links := regexp.MustCompile(`\[[^\]\n]*\]\(([^\s)]+)\)`).FindAllSubmatch(text, -1)
	localLinks := 0
	for _, match := range links {
		raw := string(match[1])
		link, err := url.Parse(raw)
		if err != nil {
			t.Errorf("invalid link %q: %v", raw, err)
			continue
		}
		if link.IsAbs() || link.Host != "" {
			continue
		}
		localLinks++
		if link.Path == "" {
			if !anchors[link.Fragment] {
				t.Errorf("missing README heading for %q", raw)
			}
			continue
		}
		if _, err := os.Stat(filepath.FromSlash(link.Path)); err != nil {
			t.Errorf("broken local link %q: %v", raw, err)
		}
	}
	if localLinks == 0 {
		t.Fatal("no local navigation/documentation links checked")
	}
}

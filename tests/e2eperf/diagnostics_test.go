//go:build linux

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestDiagnosticsModesAndFinalization(t *testing.T) {
	for _, kind := range []string{"", "cpu", "trace", "contention", "diagnostic"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			d, err := startDiagnostics(dir, kind)
			if err != nil { t.Fatal(err) }
			defer d.close()
			sentinel := errors.New("operation failed")
			called := 0
			values, got := d.phase("test-phase", func() ([]int64, error) {
				r := d.region("test-operation")
				defer r.end()
				called++
				return []int64{42}, sentinel
			})
			if called != 1 || !errors.Is(got, sentinel) || len(values) != 1 || values[0] != 42 { t.Fatal("diagnostics changed operation behavior") }
			if err := d.close(); err != nil { t.Fatal(err) }
			if err := d.close(); err != nil { t.Fatal("close is not idempotent:", err) }
			entries, err := os.ReadDir(dir)
			if err != nil { t.Fatal(err) }
			if kind == "" {
				if len(entries) != 0 { t.Fatal("disabled diagnostics wrote files") }
				return
			}
			raw, err := os.ReadFile(filepath.Join(dir, "diagnostics.json"))
			if err != nil { t.Fatal(err) }
			var meta map[string]interface{}
			if json.Unmarshal(raw, &meta) != nil || meta["diagnostic_only"] != true || meta["mode"] != kind { t.Fatal("invalid metadata") }
			expected := map[string]bool{"alloc-before.pprof":true, "alloc.pprof":true, "heap.pprof":true, "diagnostics.json":true}
			if kind == "cpu" || kind == "diagnostic" { expected["cpu.pprof"] = true }
			if kind == "trace" { expected["trace.out"] = true }
			if kind == "contention" { expected["block.pprof"] = true; expected["mutex.pprof"] = true }
			if len(entries) != len(expected) { t.Fatal("unexpected artifact count") }
			for _, entry := range entries {
				if !expected[entry.Name()] { t.Fatalf("unexpected artifact %s", entry.Name()) }
				data, e := os.ReadFile(filepath.Join(dir, entry.Name()))
				if e != nil { t.Fatal(e) }
				if entry.Name() == "trace.out" && !bytes.HasPrefix(data, []byte("go 1.")) { t.Fatal("invalid execution trace") }
				if filepath.Ext(entry.Name()) == ".pprof" {
					reader, e := gzip.NewReader(bytes.NewReader(data))
					if e != nil { t.Fatal(e) }
					decoded, e := io.ReadAll(reader)
					reader.Close()
					if e != nil || len(decoded) == 0 { t.Fatal("invalid pprof", e) }
				}
			}
		})
	}
}

func TestDiagnosticsRejectUnknownModeAndPreserveExistingFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := startDiagnostics(dir, "invalid"); err == nil { t.Fatal("unknown mode accepted") }
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 { t.Fatal("invalid mode wrote files") }
	path := filepath.Join(dir, "cpu.pprof")
	if err := os.WriteFile(path, []byte("preserve-me"), 0600); err != nil { t.Fatal(err) }
	if _, err := startDiagnostics(dir, "cpu"); err == nil { t.Fatal("existing evidence overwritten") }
	got, _ := os.ReadFile(path)
	if string(got) != "preserve-me" { t.Fatal("evidence changed") }
}

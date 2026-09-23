//go:build linux

package journal_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The parent owns the input manifest. Receipts come from a separate application
// process, and SIGKILL must not get help from deferred Close/flush calls.
func TestUserE2ERejectedPayloadCrashRecovery(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			want := map[int64]string{1: "before/世界", 3: "after-rejection/café", 2: "reuse-rejected-id"}
			evidence := t.TempDir()
			if root := os.Getenv("JOURNAL_BEHAVIOR_EVIDENCE"); root != "" {
				behaviorCheck(t, os.MkdirAll(root, 0700))
				var err error
				evidence, err = os.MkdirTemp(root, "rejected-")
				behaviorCheck(t, err)
			}
			manifest, err := json.Marshal(want)
			behaviorCheck(t, err)
			behaviorCheck(t, os.WriteFile(filepath.Join(evidence, "manifest.json"), manifest, 0600))
			c := behaviorWorker(t, dir, gz)
			c.ok(behaviorCommand{Op: "append", ID: 1, Payload: want[1]})
			for _, prefix := range []string{"small prefix", strings.Repeat("a", (5<<20)+17)} {
				if r := c.call(behaviorCommand{Op: "reject", ID: 2, Payload: prefix}); r.Error == "" {
					t.Fatal("invalid payload accepted")
				}
			}
			c.ok(behaviorCommand{Op: "append", ID: 3, Payload: want[3]})
			c.ok(behaviorCommand{Op: "append", ID: 2, Payload: want[2]})
			c.kill()
			c = behaviorWorker(t, dir, gz)
			r := c.ok(behaviorCommand{Op: "replay"})
			behaviorCheck(t, behaviorReconcile(want, r.Records))
			sink, err := os.Create(filepath.Join(evidence, "downstream.jsonl"))
			behaviorCheck(t, err)
			encoder := json.NewEncoder(sink)
			for _, record := range r.Records {
				behaviorCheck(t, encoder.Encode(record))
			}
			behaviorCheck(t, sink.Sync())
			behaviorCheck(t, sink.Close())
			// Replaying and reclaiming the old segment must not lose the replacement.
			c.kill()
			c = behaviorWorker(t, dir, !gz)
			behaviorCheck(t, behaviorReconcile(want, c.ok(behaviorCommand{Op: "replay"}).Records))
			c.kill()
		})
	}
}

// A kernel-enforced partial append is not an encoding rejection. The current
// stream must fail closed instead of accepting data behind an incomplete record.
func TestUserE2EPartialAppendDoesNotAcceptLaterRecords(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			c := behaviorWorker(t, dir, gz)
			c.ok(behaviorCommand{Op: "append", ID: 1, Payload: "synchronized prefix"})
			files := userNames(t, dir, ".buf")
			if len(files) != 1 {
				t.Fatal(files)
			}
			info, err := os.Stat(files[0])
			behaviorCheck(t, err)
			c.ok(behaviorCommand{Op: "softFileLimit", Limit: uint64(info.Size() + 128)})
			rng := rand.New(rand.NewSource(5297470428))
			body := make([]byte, (5<<20)+17)
			const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
			for i := range body {
				body[i] = alphabet[rng.Intn(len(alphabet))]
			}
			if r := c.call(behaviorCommand{Op: "append", ID: 2, Payload: string(body)}); r.Error == "" {
				t.Fatal("kernel-rejected append reported success")
			}
			c.ok(behaviorCommand{Op: "restoreFileLimit"})
			if r := c.call(behaviorCommand{Op: "append", ID: 3, Payload: "must not be accepted"}); r.Error == "" {
				t.Fatal("accepted a record behind a partial append")
			}
			if r := c.call(behaviorCommand{Op: "sync"}); r.Error == "" {
				t.Fatal("damaged stream reported a successful Sync")
			}
			c.kill()
			c = behaviorWorker(t, dir, gz)
			behaviorCheck(t, behaviorReconcile(map[int64]string{1: "synchronized prefix"}, c.ok(behaviorCommand{Op: "replay"}).Records))
			c.kill()
		})
	}
}

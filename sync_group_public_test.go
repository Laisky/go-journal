//go:build linux

package journal_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

// Deliberately no Rotate, final Sync or Close between the last individual
// successful Sync and the parent's SIGKILL. A later barrier must not repair a
// false durability claim made by a coalesced call.
func TestSyncGroupCrashChild(t *testing.T) {
	if os.Getenv("JOURNAL_SYNC_GROUP_CHILD") != "1" {
		return
	}
	journal.Logger.ChangeLevel("error")
	ack, err := strconv.Atoi(os.Getenv("JOURNAL_SYNC_GROUP_ACK"))
	if err != nil {
		t.Fatal(err)
	}
	j := behaviorStart(t, os.Getenv("JOURNAL_SYNC_GROUP_DIR"), os.Getenv("JOURNAL_SYNC_GROUP_GZIP") == "true")
	start := make(chan struct{})
	failed := make(chan error, 16)
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for n := 0; n < 16; n++ {
				id := int64(w*16 + n + 1)
				if err := j.WriteData(behaviorData(id)); err != nil {
					failed <- err
					return
				}
				if err := j.Sync(); err != nil {
					failed <- err
					return
				}
				if int(id%100) < ack {
					if err := j.WriteId(id); err != nil {
						failed <- err
						return
					}
					if err := j.Sync(); err != nil {
						failed <- err
						return
					}
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()
	close(failed)
	for err := range failed {
		t.Fatal(err)
	}
	fmt.Println("SYNC_GROUP_DURABLE")
	for {
		time.Sleep(time.Hour)
	}
}

func TestSyncGroupCrashAfterIndividualBarriers(t *testing.T) {
	for _, gzip := range []bool{false, true} {
		for _, ack := range []int{0, 50, 100} {
			t.Run(fmt.Sprintf("gzip=%v/ack=%d", gzip, ack), func(t *testing.T) {
				dir := t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestSyncGroupCrashChild$")
				cmd.Env = append(os.Environ(), "JOURNAL_SYNC_GROUP_CHILD=1", "JOURNAL_SYNC_GROUP_DIR="+dir,
					"JOURNAL_SYNC_GROUP_GZIP="+strconv.FormatBool(gzip), "JOURNAL_SYNC_GROUP_ACK="+strconv.Itoa(ack))
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				out, err := cmd.StdoutPipe()
				behaviorCheck(t, err)
				behaviorCheck(t, cmd.Start())
				defer func() {
					if cmd.ProcessState == nil {
						cmd.Process.Kill()
						cmd.Wait()
					}
				}()
				checkpoint := make(chan bool, 1)
				go func() {
					scanner := bufio.NewScanner(out)
					for scanner.Scan() {
						if strings.TrimSpace(scanner.Text()) == "SYNC_GROUP_DURABLE" {
							checkpoint <- true
							return
						}
					}
					checkpoint <- false
				}()
				select {
				case ok := <-checkpoint:
					if !ok {
						cmd.Wait()
						t.Fatalf("child never completed its barriers: %s", stderr.String())
					}
				case <-time.After(20 * time.Second):
					t.Fatal("child barrier deadline")
				}
				behaviorCheck(t, cmd.Process.Kill())
				if err := cmd.Wait(); err == nil {
					t.Fatal("worker was not killed")
				}
				j := behaviorStart(t, dir, gzip)
				high, err := j.LoadMaxId()
				behaviorCheck(t, err)
				if high != 256 {
					t.Fatalf("frontier %d, want 256", high)
				}
				if !j.LockLegacy() {
					t.Fatal("replay lease")
				}
				seen := map[int64]bool{}
				for {
					d := new(journal.Data)
					err := j.LoadLegacyBuf(d)
					if err == io.EOF {
						break
					}
					behaviorCheck(t, err)
					if d.ID < 1 || d.ID > 256 || int(d.ID%100) < ack || seen[d.ID] || !reflect.DeepEqual(d, behaviorData(d.ID)) {
						t.Fatalf("unexpected/changed replay: %#v", d)
					}
					seen[d.ID] = true
					behaviorCheck(t, j.WriteData(d))
					behaviorCheck(t, j.Sync())
				}
				for id := int64(1); id <= 256; id++ {
					if seen[id] != (int(id%100) >= ack) {
						t.Fatalf("missing/unexpected record %d", id)
					}
				}
			})
		}
	}
}

func TestSyncGroupDirectoryRenamePreservesDurability(t *testing.T) {
	dir := t.TempDir()
	j := behaviorStart(t, dir, false)
	behaviorCheck(t, j.WriteData(behaviorData(1)))
	behaviorCheck(t, j.Sync())
	moved := dir + "-moved"
	behaviorCheck(t, os.Rename(dir, moved))
	defer os.Rename(moved, dir)
	start := make(chan struct{})
	results := make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func() { <-start; results <- j.Sync() }()
	}
	close(start)
	for i := 0; i < 32; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("renamed owned directory lost its barrier: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failed barrier did not release callers")
		}
	}
	behaviorCheck(t, os.Rename(moved, dir))
	behaviorCheck(t, j.Sync())
	behaviorCheck(t, j.Rotate(context.Background()))
	behaviorCheck(t, j.WriteData(behaviorData(2)))
	behaviorCheck(t, j.Sync())
}

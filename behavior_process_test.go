//go:build linux

package journal_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
)

type behaviorCommand struct {
	Op      string
	ID      int64
	Payload string
	Limit   uint64
}
type behaviorRecord struct {
	ID      int64
	Payload string
}
type behaviorReply struct {
	Error   string
	Records []behaviorRecord
	High    int64
}

// This process is a minimal application of the public library. The parent owns
// the original manifest and the downstream ledger. A receipt is issued only
// after Sync; SIGKILL runs no cleanup, and recovery uses a fresh library instance.
func TestBehaviorProcessHelper(t *testing.T) {
	if os.Getenv("JOURNAL_BEHAVIOR_CHILD") != "1" {
		return
	}
	signal.Ignore(syscall.SIGXFSZ)
	journal.Logger.ChangeLevel("error")
	reply := func(r behaviorReply) {
		b, err := json.Marshal(r)
		if err != nil {
			panic(err)
		}
		fmt.Printf("JOURNAL_REPLY %s\n", b)
	}
	j, err := journal.NewJournal(journal.WithBufDirPath(os.Getenv("JOURNAL_BEHAVIOR_DIR")),
		journal.WithIsCompress(os.Getenv("JOURNAL_BEHAVIOR_GZIP") == "true"), journal.WithIsAggresiveGC(false),
		journal.WithBufSizeByte(1<<20), journal.WithFlushInterval(time.Hour), journal.WithRotateDuration(time.Hour), journal.WithRotateCheckInterval(time.Hour))
	if err == nil {
		err = j.Start(context.Background())
	}
	if err != nil {
		reply(behaviorReply{Error: err.Error()})
		if j != nil {
			j.Close()
		}
		return
	}
	defer j.Close()
	reply(behaviorReply{})
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	for scanner.Scan() {
		var cmd behaviorCommand
		var r behaviorReply
		if err = json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			t.Fatal(err)
		}
		switch cmd.Op {
		case "append":
			err = j.WriteData(&journal.Data{ID: cmd.ID, Data: map[string]interface{}{"payload": cmd.Payload}})
			if err == nil {
				err = j.Sync()
			}
		case "ack":
			err = j.WriteId(cmd.ID)
			if err == nil {
				err = j.Sync()
			}
		case "rotate":
			err = j.Rotate(context.Background())
		case "max":
			r.High, err = j.LoadMaxId()
		case "fileLimit":
			err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: cmd.Limit, Max: cmd.Limit})
		case "fdLimit":
			err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: cmd.Limit, Max: cmd.Limit})
		case "replay":
			if !j.LockLegacy() {
				err = fmt.Errorf("replay lease unavailable")
				break
			}
			for {
				d := new(journal.Data)
				err = j.LoadLegacyBuf(d)
				if err != nil {
					break
				}
				payload, ok := d.Data["payload"].(string)
				if !ok {
					err = fmt.Errorf("invalid recovered payload: %T", d.Data["payload"])
					break
				}
				r.Records = append(r.Records, behaviorRecord{d.ID, payload})
				// Replacement ownership must be completed before the next read/cleanup.
				if err = j.WriteData(d); err != nil {
					break
				}
			}
			if err == io.EOF {
				err = nil
			}
		default:
			err = fmt.Errorf("unknown operation %q", cmd.Op)
		}
		if err != nil {
			r.Error = err.Error()
		}
		reply(r)
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

type behaviorChild struct {
	t        *testing.T
	cmd      *exec.Cmd
	input    io.WriteCloser
	output   *bufio.Scanner
	logName  string
	finished bool
}

func behaviorLaunch(t *testing.T, dir string, gz bool) (*behaviorChild, behaviorReply) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBehaviorProcessHelper$", "-test.timeout=40s")
	cmd.Env = append(os.Environ(), "JOURNAL_BEHAVIOR_CHILD=1", "JOURNAL_BEHAVIOR_DIR="+dir, fmt.Sprintf("JOURNAL_BEHAVIOR_GZIP=%v", gz))
	input, err := cmd.StdinPipe()
	behaviorCheck(t, err)
	output, err := cmd.StdoutPipe()
	behaviorCheck(t, err)
	logName := filepath.Join(t.TempDir(), "child.stderr")
	logFile, err := os.Create(logName)
	behaviorCheck(t, err)
	cmd.Stderr = logFile
	c := &behaviorChild{t: t, cmd: cmd, input: input, output: bufio.NewScanner(output), logName: logName}
	c.output.Buffer(make([]byte, 4096), 8<<20)
	behaviorCheck(t, cmd.Start())
	t.Cleanup(func() {
		if !c.finished {
			c.kill()
		}
		logFile.Close()
		b, _ := os.ReadFile(logName)
		if strings.Contains(string(b), "DATA RACE") {
			t.Errorf("child race:\n%s", b)
		}
	})
	r := c.read()
	if r.Error != "" {
		behaviorCheck(t, cmd.Wait())
		c.finished = true
	}
	return c, r
}
func behaviorWorker(t *testing.T, dir string, gz bool) *behaviorChild {
	t.Helper()
	c, r := behaviorLaunch(t, dir, gz)
	if r.Error != "" {
		t.Fatal(r.Error)
	}
	return c
}
func (c *behaviorChild) read() behaviorReply {
	c.t.Helper()
	for c.output.Scan() {
		line := c.output.Text()
		if !strings.HasPrefix(line, "JOURNAL_REPLY ") {
			continue
		}
		var r behaviorReply
		behaviorCheck(c.t, json.Unmarshal([]byte(strings.TrimPrefix(line, "JOURNAL_REPLY ")), &r))
		return r
	}
	logs, _ := os.ReadFile(c.logName)
	c.t.Fatalf("child stopped without a response: %v\n%s", c.output.Err(), logs)
	return behaviorReply{}
}
func (c *behaviorChild) call(cmd behaviorCommand) behaviorReply {
	c.t.Helper()
	b, err := json.Marshal(cmd)
	behaviorCheck(c.t, err)
	_, err = c.input.Write(append(b, '\n'))
	behaviorCheck(c.t, err)
	return c.read()
}
func (c *behaviorChild) ok(cmd behaviorCommand) behaviorReply {
	c.t.Helper()
	r := c.call(cmd)
	if r.Error != "" {
		c.t.Fatalf("%s: %s", cmd.Op, r.Error)
	}
	return r
}
func (c *behaviorChild) kill() {
	c.t.Helper()
	if c.finished {
		return
	}
	err := c.cmd.Process.Kill()
	waitErr := c.cmd.Wait()
	c.finished = true
	if err != nil {
		c.t.Errorf("failed to inject SIGKILL: %v (wait=%v)", err, waitErr)
		return
	}
	ee, ok := waitErr.(*exec.ExitError)
	if !ok {
		c.t.Errorf("expected killed child, got %v", waitErr)
		return
	}
	status, ok := ee.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		c.t.Errorf("unexpected crash status: %v", waitErr)
	}
}

func TestBehaviorE2ECrashDelivery(t *testing.T) {
	for _, gz := range []bool{false, true} {
		for _, seed := range []int64{17, 991} {
			t.Run(fmt.Sprintf("gzip=%v/seed=%d", gz, seed), func(t *testing.T) {
				dir := t.TempDir()
				manifest := map[int64]string{}
				acked := map[int64]bool{}
				sinkName := filepath.Join(t.TempDir(), "downstream.jsonl")
				sink, err := os.Create(sinkName)
				behaviorCheck(t, err)
				defer sink.Close()
				sinkEnc := json.NewEncoder(sink)
				persist := func(r behaviorRecord) { behaviorCheck(t, sinkEnc.Encode(r)); behaviorCheck(t, sink.Sync()) }
				rng := rand.New(rand.NewSource(seed))
				c := behaviorWorker(t, dir, gz)
				for phase := 0; phase < 3; phase++ {
					for _, offset := range rng.Perm(16) {
						id := int64(phase*16 + offset + 1)
						payload := fmt.Sprintf("%d/%d/世界/%s", seed, id, strings.Repeat("a", int(id)%27))
						manifest[id] = payload
						c.ok(behaviorCommand{Op: "append", ID: id, Payload: payload})
						if id%3 == 0 {
							persist(behaviorRecord{id, payload})
							c.ok(behaviorCommand{Op: "ack", ID: id})
							acked[id] = true
						}
					}
					c.kill()
					c = behaviorWorker(t, dir, gz)
					high := c.ok(behaviorCommand{Op: "max"}).High
					if high < int64((phase+1)*16) {
						t.Fatalf("retained identity frontier regressed: %d", high)
					}
					r := c.ok(behaviorCommand{Op: "replay"})
					got := map[int64]string{}
					for _, d := range r.Records {
						if payload, ok := manifest[d.ID]; !ok || payload != d.Payload {
							t.Fatalf("invented/corrupted event: %+v", d)
						}
						got[d.ID] = d.Payload
						if d.ID%4 == 0 {
							persist(d)
							c.ok(behaviorCommand{Op: "ack", ID: d.ID})
							acked[d.ID] = true
						}
					}
					for id := range manifest {
						if id%3 != 0 && !(id%4 == 0 && id <= int64(phase*16)) {
							if _, ok := got[id]; !ok {
								t.Fatalf("missing accepted event %d after crash", id)
							}
						}
					}
				}
				// Final recovery returns every remaining obligation; acknowledge only after
				// a separate application's persisted sink ledger accepts the exact payload.
				c.kill()
				c = behaviorWorker(t, dir, gz)
				r := c.ok(behaviorCommand{Op: "replay"})
				for _, d := range r.Records {
					if manifest[d.ID] != d.Payload {
						t.Fatal("payload mismatch")
					}
					persist(d)
					c.ok(behaviorCommand{Op: "ack", ID: d.ID})
					acked[d.ID] = true
				}
				for id := range manifest {
					if !acked[id] {
						t.Fatalf("unfulfilled downstream obligation %d", id)
					}
				}
				c.kill()
				c = behaviorWorker(t, dir, gz)
				if r := c.ok(behaviorCommand{Op: "replay"}); len(r.Records) != 0 {
					t.Fatalf("durable ACKs were lost: %+v", r.Records)
				}
				c.kill()
				behaviorCheck(t, sink.Close())
				fp, err := os.Open(sinkName)
				behaviorCheck(t, err)
				defer fp.Close()
				scan := bufio.NewScanner(fp)
				received := map[int64]string{}
				for scan.Scan() {
					var d behaviorRecord
					behaviorCheck(t, json.Unmarshal(scan.Bytes(), &d))
					if manifest[d.ID] != d.Payload {
						t.Fatal("sink corruption")
					}
					received[d.ID] = d.Payload
				}
				behaviorCheck(t, scan.Err())
				if !reflect.DeepEqual(received, manifest) {
					t.Fatalf("sink received %d/%d unique events", len(received), len(manifest))
				}
				t.Logf("48 caller events, exact sink reconciliation, 5 SIGKILL/reopen boundaries; seed=%d gzip=%v", seed, gz)
			})
		}
	}
}

func TestBehaviorE2EProcessOwnership(t *testing.T) {
	dir := t.TempDir()
	first := behaviorWorker(t, dir, false)
	_, r := behaviorLaunch(t, dir, false)
	if r.Error == "" {
		t.Fatal("a second process acquired a live journal")
	}
	first.kill()
	recovered := behaviorWorker(t, dir, false)
	recovered.ok(behaviorCommand{Op: "append", ID: 1, Payload: "after-owner-crash"})
	recovered.kill()
}
func TestBehaviorE2EFileLimitFailurePreservesAcceptedPrefix(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			c := behaviorWorker(t, dir, gz)
			c.ok(behaviorCommand{Op: "append", ID: 1, Payload: "accepted-before-failure"})
			c.ok(behaviorCommand{Op: "fileLimit", Limit: 16384})
			entropy := make([]byte, 128<<10)
			rand.New(rand.NewSource(42)).Read(entropy)
			failedPayload := hex.EncodeToString(entropy)
			r := c.call(behaviorCommand{Op: "append", ID: 2, Payload: failedPayload})
			if r.Error == "" {
				t.Fatal("kernel write refusal falsely acknowledged")
			}
			c.kill()
			c = behaviorWorker(t, dir, gz)
			r = c.ok(behaviorCommand{Op: "replay"})
			found := false
			for _, d := range r.Records {
				switch d.ID {
				case 1:
					if d.Payload != "accepted-before-failure" {
						t.Fatal("accepted payload changed")
					}
					found = true
				case 2:
					if d.Payload != failedPayload {
						t.Fatal("failed append replayed a partial payload")
					}
				default:
					t.Fatal("fabricated record")
				}
			}
			if !found {
				t.Fatal("a failed append lost a previously synchronized record")
			}
			c.kill()
		})
	}
}
func TestBehaviorE2ERecoveryWithDescriptorBudget(t *testing.T) {
	dir := t.TempDir()
	// Independently encode 96 valid one-ACK segments. Recovery must close each
	// input as it advances, rather than exhausting a 64-descriptor process limit.
	for i := 1; i <= 96; i++ {
		var word [8]byte
		binary.BigEndian.PutUint64(word[:], uint64(1000+i))
		behaviorCheck(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("20260101_%08d.ids", i)), word[:], 0600))
	}
	c := behaviorWorker(t, dir, false)
	c.ok(behaviorCommand{Op: "fdLimit", Limit: 64})
	r := c.ok(behaviorCommand{Op: "max"})
	if r.High != 1096 {
		t.Fatalf("max=%d want 1096", r.High)
	}
	c.kill()
}
func TestBehaviorE2EEmptyCrashSegments(t *testing.T) {
	for _, gz := range []bool{false, true} {
		t.Run(fmt.Sprint(gz), func(t *testing.T) {
			dir := t.TempDir()
			c := behaviorWorker(t, dir, gz)
			c.kill()
			c = behaviorWorker(t, dir, gz)
			c.ok(behaviorCommand{Op: "max"})
			r := c.ok(behaviorCommand{Op: "replay"})
			if len(r.Records) != 0 {
				t.Fatal("empty crash fabricated events")
			}
			c.kill()
		})
	}
}

// Keep serialization explicitly stable when test diagnostics enumerate records.
func behaviorSortedIDs(m map[int64]string) []int64 {
	out := make([]int64, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

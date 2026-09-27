//go:build linux

// Command e2eperf is an ordinary external consumer of the exported journal API.
// Its independent orchestrator owns the downstream ledger and crash schedule.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	journal "github.com/Laisky/go-journal"
)

type options struct {
	Mode, Dir, Out, Sink, Token, Profile       string
	Count, Payload, Writers, AckPercent, Scans int
	Gzip, Hold                                 bool
	ScanSeconds                                time.Duration
}
type observation struct {
	ID                                       int64  `json:"id"`
	Hash                                     string `json:"hash"`
	Begin, Written, Durable, Received, Acked int64
}
type usage struct {
	CPU     float64 `json:"cpu_seconds"`
	Alloc   uint64  `json:"total_alloc"`
	Mallocs uint64  `json:"mallocs"`
	GC      uint32  `json:"num_gc"`
	RSSKiB  int64   `json:"peak_rss_kib"`
}
type phase struct {
	Name          string `json:"name"`
	Begin, End    int64
	Before, After usage
	Ops           int64   `json:"ops"`
	LatencyNS     []int64 `json:"latency_ns"`
}
type result struct {
	Diagnostic  string `json:"diagnostic,omitempty"`
	diagnostics *diagnostics
	Mode        string        `json:"mode"`
	Count       int           `json:"count"`
	High        int64         `json:"high"`
	Phases      []phase       `json:"phases"`
	Records     []observation `json:"records"`
}

func snapshot() usage {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		panic(err)
	}
	var hwm int64
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmHWM:" && fields[2] == "kB" {
			hwm, err = strconv.ParseInt(fields[1], 10, 64)
			if err != nil || hwm <= 0 {
				panic("invalid process VmHWM")
			}
		}
	}
	if hwm == 0 {
		panic("missing process VmHWM")
	}
	return usage{CPU: float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6,
		Alloc: m.TotalAlloc, Mallocs: m.Mallocs, GC: m.NumGC, RSSKiB: hwm}
}
func measure(r *result, name string, ops int64, fn func() ([]int64, error)) error {
	p := phase{Name: name, Ops: ops, Before: snapshot()}
	start := time.Now()
	p.Begin = start.UnixNano()
	lat, err := r.diagnostics.phase(name, fn)
	// Use monotonic elapsed time, not a subtraction of adjustable wall clocks.
	p.End = p.Begin + time.Since(start).Nanoseconds()
	p.LatencyNS = lat
	p.After = snapshot()
	r.Phases = append(r.Phases, p)
	return err
}
func payload(id int64, n int) string {
	return fmt.Sprintf("id=%012d|世界/café|", id) + strings.Repeat("0123456789abcdef", (n+15)/16)[:n]
}
func hash(s string) string                  { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func initialAck(id int64, percent int) bool { return int(id%100) < percent }
func document(id int64, n int) map[string]interface{} {
	return map[string]interface{}{"id": id, "body": payload(id, n)}
}
func validate(d *journal.Data, n int) error {
	body, ok := d.Data["body"].(string)
	if !ok || body != payload(d.ID, n) || fmt.Sprint(d.Data["id"]) != strconv.FormatInt(d.ID, 10) || len(d.Data) != 2 {
		return fmt.Errorf("changed recovered payload for ID %d", d.ID)
	}
	return nil
}
func save(path string, value interface{}) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	encErr := json.NewEncoder(f).Encode(value)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err = errors.Join(encErr, syncErr, closeErr); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func deliver(client *http.Client, o options, id int64, body string) error {
	b, err := json.Marshal(map[string]interface{}{"id": id, "body": body})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), "POST", o.Sink+"/"+o.Mode, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+o.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1025))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || len(raw) > 1024 {
		return fmt.Errorf("downstream status %d", resp.StatusCode)
	}
	var receipt struct {
		ID      int64  `json:"id"`
		Hash    string `json:"hash"`
		Durable bool   `json:"durable"`
	}
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return err
	}
	if receipt.ID != id || receipt.Hash != hash(body) || !receipt.Durable {
		return errors.New("downstream receipt mismatch")
	}
	return nil
}

func seed(j *journal.Journal, o options, r *result, client *http.Client) error {
	r.Records = make([]observation, o.Count)
	var next atomic.Int64
	var wg sync.WaitGroup
	var first error
	var em sync.Mutex
	epoch := time.Now()
	for w := 0; w < o.Writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id := next.Add(1)
				if id > int64(o.Count) {
					return
				}
				body := payload(id, o.Payload)
				v := observation{ID: id, Hash: hash(body)}
				v.Begin = time.Since(epoch).Nanoseconds()
				region := r.diagnostics.region("WriteData")
				err := j.WriteData(&journal.Data{ID: id, Data: map[string]interface{}{"id": id, "body": body}})
				region.end()
				v.Written = time.Since(epoch).Nanoseconds()
				if err == nil {
					region = r.diagnostics.region("Sync/data")
					err = j.Sync()
					region.end()
				}
				v.Durable = time.Since(epoch).Nanoseconds()
				if err == nil && initialAck(id, o.AckPercent) {
					region = r.diagnostics.region("downstream/fsync-receipt")
					err = deliver(client, o, id, body)
					region.end()
					v.Received = time.Since(epoch).Nanoseconds()
					if err == nil {
						region = r.diagnostics.region("WriteId")
						err = j.WriteId(id)
						region.end()
					}
					if err == nil {
						region = r.diagnostics.region("Sync/ack")
						err = j.Sync()
						region.end()
					}
					v.Acked = time.Since(epoch).Nanoseconds()
				}
				if err != nil {
					em.Lock()
					if first == nil {
						first = err
					}
					em.Unlock()
					return
				}
				r.Records[id-1] = v
			}
		}()
	}
	wg.Wait()
	return first
}

func replay(j *journal.Journal, o options, r *result, client *http.Client) error {
	if !j.LockLegacy() {
		return errors.New("replay lease unavailable")
	}
	defer j.UnLockLegacy()
	epoch := time.Now()
	for {
		begin := time.Since(epoch).Nanoseconds()
		d := new(journal.Data)
		region := r.diagnostics.region("LoadLegacyBuf")
		err := j.LoadLegacyBuf(d)
		region.end()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if d.ID < 1 || d.ID > int64(o.Count) {
			return fmt.Errorf("unexpected ID %d", d.ID)
		}
		if err = validate(d, o.Payload); err != nil {
			return err
		}
		v := observation{ID: d.ID, Hash: hash(d.Data["body"].(string)), Begin: begin, Written: time.Since(epoch).Nanoseconds()}
		if o.Mode == "transfer" {
			// Copy and synchronize BEFORE requesting another record or EOF cleanup.
			region = r.diagnostics.region("WriteData/transfer")
			err = j.WriteData(d)
			region.end()
			if err != nil {
				return err
			}
			region = r.diagnostics.region("Sync/replay")
			err = j.Sync()
			region.end()
			if err != nil {
				return err
			}
			v.Durable = time.Since(epoch).Nanoseconds()
		} else if o.Mode == "deliver" {
			region = r.diagnostics.region("downstream/fsync-receipt")
			err = deliver(client, o, d.ID, d.Data["body"].(string))
			region.end()
			if err != nil {
				return err
			}
			v.Received = time.Since(epoch).Nanoseconds()
			region = r.diagnostics.region("WriteId/replay")
			err = j.WriteId(d.ID)
			region.end()
			if err != nil {
				return err
			}
			region = r.diagnostics.region("Sync/replay")
			err = j.Sync()
			region.end()
			if err != nil {
				return err
			}
			v.Acked = time.Since(epoch).Nanoseconds()
		} else {
			return fmt.Errorf("pending ID %d after completion", d.ID)
		}
		r.Records = append(r.Records, v)
	}
}

func run(o options) (err error) {
	if err = journal.Logger.ChangeLevel("error"); err != nil {
		return err
	}
	if err = os.MkdirAll(o.Out, 0700); err != nil {
		return err
	}
	diag, err := startDiagnostics(o.Out, o.Profile)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, diag.close()) }()
	r := result{Mode: o.Mode, Count: o.Count, Records: []observation{}, Diagnostic: o.Profile, diagnostics: diag}
	var j *journal.Journal
	err = measure(&r, "open", 1, func() ([]int64, error) {
		var e error
		j, e = journal.NewJournal(journal.WithBufDirPath(o.Dir), journal.WithName("e2eperf"),
			journal.WithIsCompress(o.Gzip), journal.WithIsAggresiveGC(false),
			journal.WithFlushInterval(24*time.Hour), journal.WithRotateDuration(24*time.Hour),
			journal.WithRotateCheckInterval(24*time.Hour), journal.WithCommitIDTTL(24*time.Hour), journal.WithBufSizeByte(1<<30))
		if e == nil {
			e = j.Start(context.Background())
		}
		return nil, e
	})
	if err != nil {
		return err
	}
	defer j.Close()
	transport := &http.Transport{Proxy: nil, MaxIdleConns: o.Writers + 4, MaxIdleConnsPerHost: o.Writers + 4}
	client := &http.Client{Timeout: 30 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer transport.CloseIdleConnections()
	if o.Mode != "seed" {
		err = measure(&r, "frontier", 1, func() ([]int64, error) { var e error; r.High, e = j.LoadMaxId(); return nil, e })
		if err != nil {
			return err
		}
		if r.High != int64(o.Count) {
			return fmt.Errorf("frontier %d, want %d", r.High, o.Count)
		}
	}
	switch o.Mode {
	case "seed":
		err = measure(&r, "append_sync_deliver_ack", int64(o.Count), func() ([]int64, error) { return nil, seed(j, o, &r, client) })
	case "transfer", "deliver", "verify":
		err = measure(&r, "replay_"+o.Mode, 0, func() ([]int64, error) { return nil, replay(j, o, &r, client) })
		r.Phases[len(r.Phases)-1].Ops = int64(len(r.Records))
	case "scan":
		err = measure(&r, "scan", 0, func() ([]int64, error) {
			var wg sync.WaitGroup
			var mu sync.Mutex
			var failure error
			var times []int64
			deadline := time.Now().Add(o.ScanSeconds)
			for w := 0; w < o.Writers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := 0; (o.ScanSeconds > 0 && time.Now().Before(deadline)) || (o.ScanSeconds == 0 && n < o.Scans); n++ {
						start := time.Now()
						high, e := j.LoadMaxId()
						elapsed := time.Since(start).Nanoseconds()
						mu.Lock()
						if e != nil || high != int64(o.Count) {
							if failure == nil {
								failure = fmt.Errorf("scan frontier %d: %v", high, e)
							}
							mu.Unlock()
							return
						}
						times = append(times, elapsed)
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			return times, failure
		})
		p := &r.Phases[len(r.Phases)-1]
		p.Ops = int64(len(p.LatencyNS))
	default:
		return errors.New("unknown mode")
	}
	if err != nil {
		return err
	}
	if o.Mode == "seed" || o.Mode == "transfer" {
		err = measure(&r, "seal", 1, func() ([]int64, error) { return nil, j.Rotate(context.Background()) })
		if err != nil {
			return err
		}
	}
	if err = j.Sync(); err != nil {
		return err
	}
	// Close before publishing the checkpoint: SIGKILL must not truncate profiles.
	if err = diag.close(); err != nil {
		return err
	}
	if err = save(filepath.Join(o.Out, "result.json"), r); err != nil {
		return err
	}
	if o.Hold {
		// The supervisor kills this process after reading the durable checkpoint.
		fmt.Println("CHECKPOINT")
		for {
			time.Sleep(time.Hour)
		}
	}
	return nil
}
func main() {
	var o options
	flag.StringVar(&o.Mode, "mode", "", "seed|transfer|deliver|verify|scan")
	flag.StringVar(&o.Dir, "dir", "", "private journal directory")
	flag.StringVar(&o.Out, "out", "", "new phase evidence directory")
	flag.StringVar(&o.Sink, "sink", "", "independent loopback peer")
	flag.StringVar(&o.Token, "token", "", "local peer token")
	flag.StringVar(&o.Profile, "profile", "", "diagnostic-only cpu|trace|contention (all lifecycle modes)")
	flag.IntVar(&o.Count, "count", 2048, "total source records")
	flag.IntVar(&o.Payload, "payload", 16384, "text bytes excluding identity prefix")
	flag.IntVar(&o.Writers, "writers", 4, "concurrent public API callers")
	flag.IntVar(&o.AckPercent, "ack-percent", 50, "deterministic sparse ACK selection")
	flag.IntVar(&o.Scans, "scans", 1, "LoadMaxId repetitions per scan worker")
	flag.DurationVar(&o.ScanSeconds, "scan-duration", 0, "diagnostic duration instead of fixed scan count")
	flag.BoolVar(&o.Gzip, "gzip", false, "gzip journal")
	flag.BoolVar(&o.Hold, "hold", false, "hold durable checkpoint until supervisor SIGKILL")
	flag.Parse()
	if o.Dir == "" || o.Out == "" || o.Count < 1 || o.Count > 1000000 || o.Payload < 0 || o.Payload > 4<<20 || o.Writers < 1 || o.Writers > 128 || o.AckPercent < 0 || o.AckPercent > 100 || o.Scans < 0 || (o.Mode == "scan" && o.Scans == 0) || o.Scans > 10000 || o.ScanSeconds < 0 || o.ScanSeconds > 10*time.Minute {
		fmt.Fprintln(os.Stderr, "invalid options")
		os.Exit(2)
	}
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

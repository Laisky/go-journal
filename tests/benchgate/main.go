//go:build linux

// Command benchgate measures fixed public-API work. It never substitutes these
// warm-file CPU/allocation batches for durable end-to-end latency or capacity.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	journal "github.com/Laisky/go-journal"
)

type result struct {
	Operations int     `json:"operations"`
	CPU        float64 `json:"cpu_ns/op"`
	Wall       float64 `json:"wall_ns/op"`
	Bytes      float64 `json:"B/op"`
	Allocs     float64 `json:"allocs/op"`
}

type fixture struct {
	run   func() error
	close func() error
}

func cpu() (int64, error) {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return 0, err
	}
	return (r.Utime.Sec+r.Stime.Sec)*1e9 + (r.Utime.Usec+r.Stime.Usec)*1000, nil
}

func measure(f fixture, n int) (result, error) {
	// One unmeasured warm-up makes fixture creation/pool initialization explicit.
	if err := f.run(); err != nil {
		return result{}, err
	}
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	startCPU, err := cpu()
	if err != nil {
		return result{}, err
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := f.run(); err != nil {
			return result{}, err
		}
	}
	wall := time.Since(start).Nanoseconds()
	endCPU, err := cpu()
	if err != nil {
		return result{}, err
	}
	runtime.ReadMemStats(&b)
	if endCPU <= startCPU || wall <= 0 {
		return result{}, fmt.Errorf("invalid measurement interval")
	}
	return result{n, float64(endCPU-startCPU) / float64(n), float64(wall) / float64(n),
		float64(b.TotalAlloc-a.TotalAlloc) / float64(n), float64(b.Mallocs-a.Mallocs) / float64(n)}, nil
}

func staging(size int) (fixture, error) {
	fp, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return fixture{}, err
	}
	enc, err := journal.NewDataEncoder(fp, false)
	if err != nil {
		fp.Close()
		return fixture{}, err
	}
	d := &journal.Data{ID: 1, Data: map[string]interface{}{"body": strings.Repeat("x", size)}}
	return fixture{func() error { return enc.Write(d) }, func() error {
		err := enc.Close()
		closed := fp.Close()
		if err != nil {
			return err
		}
		return closed
	}}, nil
}

func ttl(refresh bool) fixture {
	ctx, cancel := context.WithCancel(context.Background())
	s := journal.NewInt64SetWithTTL(ctx, time.Hour)
	for i := 0; i < 8192; i++ {
		s.AddInt64(int64(i))
	}
	return fixture{func() error {
		for i := 0; i < 8192; i++ {
			if refresh {
				s.AddInt64(int64(i))
			}
			if !s.CheckAndRemove(int64(i)) {
				return fmt.Errorf("missing TTL identity %d", i)
			}
		}
		if s.GetLen() != 8192 {
			return fmt.Errorf("changed TTL cardinality")
		}
		return nil
	}, func() error { s.Close(); cancel(); return nil }}
}

type sink struct {
	count int
	bad   bool
}

func (s *sink) Add(v int) { s.AddInt64(int64(v)) }
func (s *sink) AddInt64(v int64) {
	if v != int64(s.count)+97 {
		s.bad = true
	}
	s.count++
}
func (s *sink) GetLen() int             { return s.count }
func (*sink) CheckAndRemove(int64) bool { return false }

func ack(dir string, maximum bool) (fixture, error) {
	const count = 131072
	path := filepath.Join(dir, "ack")
	wire := make([]byte, count*8)
	for i := 0; i < count; i++ {
		value := uint64(i)
		if i == 0 {
			value = 97
		}
		binary.BigEndian.PutUint64(wire[i*8:], value)
	}
	fp, err := os.Create(path)
	if err != nil {
		return fixture{}, err
	}
	if _, err = fp.Write(wire); err == nil {
		err = fp.Sync()
	}
	closed := fp.Close()
	if err != nil {
		return fixture{}, err
	}
	if closed != nil {
		return fixture{}, closed
	}
	return fixture{func() error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		dec, err := journal.NewIdsDecoder(f, false)
		if err != nil {
			return err
		}
		if maximum {
			high, err := dec.LoadMaxId()
			if err != nil {
				return err
			}
			if high != count+96 {
				return fmt.Errorf("wrong ACK frontier %d", high)
			}
		} else {
			s := &sink{}
			if err := dec.ReadAllToInt64Set(s); err != nil {
				return err
			}
			if s.bad || s.count != count {
				return fmt.Errorf("changed ACK import")
			}
		}
		return f.Close()
	}, func() error { return os.Remove(path) }}, nil
}

func directory(dir string) (fixture, error) {
	const count = 256
	for i := 0; i < count/2; i++ {
		for _, suffix := range []string{"buf", "ids"} {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("20990101_%08d.%s", i+1, suffix)), nil, 0600); err != nil {
				return fixture{}, err
			}
		}
	}
	return fixture{func() error {
		s, err := journal.PrepareNewBufFile(dir, nil, true, false, 0)
		if err != nil {
			return err
		}
		if len(s.OldDataFnames) != count/2 || len(s.OldIDsDataFnames) != count/2 {
			return fmt.Errorf("changed directory work")
		}
		for _, f := range []*os.File{s.NewDataFp, s.NewIDsFp} {
			if err := f.Close(); err != nil {
				return err
			}
			if err := os.Remove(f.Name()); err != nil {
				return err
			}
		}
		return nil
	}, func() error { return nil }}, nil
}

func frontier(dir string, onlyACK bool) (fixture, error) {
	j, err := journal.NewJournal(journal.WithBufDirPath(dir), journal.WithIsCompress(false),
		journal.WithIsAggresiveGC(false), journal.WithBufSizeByte(65536),
		journal.WithFlushInterval(time.Hour), journal.WithRotateCheckInterval(time.Hour))
	if err != nil {
		return fixture{}, err
	}
	if err := j.Start(context.Background()); err != nil {
		j.Close()
		return fixture{}, err
	}
	success := false
	defer func() {
		if !success {
			j.Close()
		}
	}()
	const count = 128
	for id := 1; id <= count; id++ {
		if onlyACK {
			err = j.WriteId(int64(id))
		} else {
			err = j.WriteData(&journal.Data{ID: int64(id), Data: map[string]interface{}{"body": strings.Repeat("x", 512)}})
		}
		if err != nil {
			return fixture{}, err
		}
		if id%8 == 0 {
			if err = j.Sync(); err != nil {
				return fixture{}, err
			}
			if err = j.Rotate(context.Background()); err != nil {
				return fixture{}, err
			}
		}
	}
	success = true
	return fixture{func() error {
		high, err := j.LoadMaxId()
		if err != nil {
			return err
		}
		if high != count {
			return fmt.Errorf("changed multi-segment frontier %d", high)
		}
		return nil
	}, func() error { j.Close(); return nil }}, nil
}

func run(n int, selected string, output io.Writer) error {
	if n < 32 || n > 4096 {
		return fmt.Errorf("iterations must be in [32,4096]")
	}
	if err := journal.Logger.ChangeLevel("error"); err != nil {
		return err
	}
	names := []string{"staging-256k", "staging-1m", "staging-overflow", "ttl-hits", "ttl-refresh", "ack-import", "ack-maximum", "directory", "data-segments", "ack-segments"}
	results := map[string]result{}
	for _, name := range names {
		if selected != "" && selected != name {
			continue
		}
		dir, err := os.MkdirTemp("", "journal-benchgate-")
		if err != nil {
			return err
		}
		var f fixture
		switch name {
		case "staging-256k":
			f, err = staging(256 << 10)
		case "staging-1m":
			f, err = staging(1 << 20)
		case "staging-overflow":
			f, err = staging((4 << 20) + 17)
		case "ttl-hits":
			f = ttl(false)
		case "ttl-refresh":
			f = ttl(true)
		case "ack-import":
			f, err = ack(dir, false)
		case "ack-maximum":
			f, err = ack(dir, true)
		case "directory":
			f, err = directory(dir)
		case "data-segments":
			f, err = frontier(dir, false)
		case "ack-segments":
			f, err = frontier(dir, true)
		}
		if err != nil {
			os.RemoveAll(dir)
			return fmt.Errorf("%s setup: %w", name, err)
		}
		r, measured := measure(f, n)
		closed := f.close()
		removed := os.RemoveAll(dir)
		if measured != nil {
			return fmt.Errorf("%s: %w", name, measured)
		}
		if closed != nil {
			return closed
		}
		if removed != nil {
			return removed
		}
		results[name] = r
	}
	if len(results) == 0 {
		return fmt.Errorf("unknown benchmark %q", selected)
	}
	return json.NewEncoder(output).Encode(map[string]interface{}{"schema": 1, "go": runtime.Version(),
		"gomaxprocs": runtime.GOMAXPROCS(0), "results": results})
}

func main() {
	n := flag.Int("iterations", 128, "fixed operations per case")
	selected := flag.String("case", "", "one diagnostic case; empty runs all cases")
	out := flag.String("out", "", "exclusive JSON output; stdout if empty")
	flag.Parse()
	var output io.Writer = os.Stdout
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		output = f
	}
	if err := run(*n, *selected, output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var _ journal.Int64SetItf = (*sink)(nil)

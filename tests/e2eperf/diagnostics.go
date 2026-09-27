//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

// Diagnostics are process-local and opt-in. Never use their durations for an
// acceptance comparison: GC, stack sampling and trace collection perturb work.
// CPU and tracing are separate modes so their overhead is not conflated.
type diagnostics struct {
	dir, kind           string
	cpu, execution      *os.File
	blockRate, mutexRate int
	oldMutex            int
	closed              bool
}

func exclusiveProfile(dir, name string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
}

func writeProfile(dir, name, profile string) error {
	f, err := exclusiveProfile(dir, name)
	if err != nil {
		return err
	}
	return errors.Join(pprof.Lookup(profile).WriteTo(f, 0), f.Close())
}

func startDiagnostics(dir, kind string) (_ *diagnostics, err error) {
	if kind == "" {
		return nil, nil
	}
	if kind != "cpu" && kind != "trace" && kind != "contention" && kind != "diagnostic" {
		return nil, fmt.Errorf("unknown profile mode %q: cpu|trace|contention|diagnostic", kind)
	}
	d := &diagnostics{dir: dir, kind: kind}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.close())
		}
	}()
	runtime.GC()
	if err = writeProfile(dir, "alloc-before.pprof", "allocs"); err != nil {
		return nil, err
	}
	if kind == "cpu" || kind == "diagnostic" {
		if d.cpu, err = exclusiveProfile(dir, "cpu.pprof"); err != nil {
			return nil, err
		}
		if err = pprof.StartCPUProfile(d.cpu); err != nil {
			err = errors.Join(err, d.cpu.Close())
			d.cpu = nil
			return nil, err
		}
	}
	if kind == "trace" {
		if d.execution, err = exclusiveProfile(dir, "trace.out"); err != nil {
			return nil, err
		}
		if err = trace.Start(d.execution); err != nil {
			err = errors.Join(err, d.execution.Close())
			d.execution = nil
			return nil, err
		}
	}
	if kind == "contention" {
		// Standalone worker owns process-wide profiler settings. These rates are
		// intentionally expensive and never enabled in an unprofiled trial.
		d.blockRate, d.mutexRate = 1, 1
		runtime.SetBlockProfileRate(d.blockRate)
		d.oldMutex = runtime.SetMutexProfileFraction(d.mutexRate)
	}
	return d, nil
}

func (d *diagnostics) phase(name string, fn func() ([]int64, error)) (lat []int64, err error) {
	if d == nil {
		return fn()
	}
	ctx, task := trace.NewTask(context.Background(), "journal/"+name)
	defer task.End()
	pprof.Do(ctx, pprof.Labels("phase", name), func(ctx context.Context) {
		trace.WithRegion(ctx, name, func() { lat, err = fn() })
	})
	return lat, err
}

type diagnosticRegion struct{ region *trace.Region }

func (d *diagnostics) region(name string) diagnosticRegion {
	if d == nil || d.kind != "trace" {
		return diagnosticRegion{}
	}
	return diagnosticRegion{trace.StartRegion(context.Background(), name)}
}
func (r diagnosticRegion) end() {
	if r.region != nil {
		r.region.End()
	}
}

func (d *diagnostics) close() (err error) {
	if d == nil || d.closed {
		return nil
	}
	d.closed = true
	if d.cpu != nil {
		pprof.StopCPUProfile()
		err = errors.Join(err, d.cpu.Close())
	}
	if d.execution != nil {
		trace.Stop()
		err = errors.Join(err, d.execution.Close())
	}
	if d.blockRate != 0 {
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(d.oldMutex)
		err = errors.Join(err, writeProfile(d.dir, "block.pprof", "block"), writeProfile(d.dir, "mutex.pprof", "mutex"))
	}
	runtime.GC()
	err = errors.Join(err, writeProfile(d.dir, "alloc.pprof", "allocs"), writeProfile(d.dir, "heap.pprof", "heap"))
	f, e := exclusiveProfile(d.dir, "diagnostics.json")
	if e != nil {
		return errors.Join(err, e)
	}
	meta := map[string]interface{}{"diagnostic_only": true, "mode": d.kind, "go_version": runtime.Version(),
		"mem_profile_rate": runtime.MemProfileRate, "block_profile_rate": d.blockRate, "mutex_profile_fraction": d.mutexRate,
		"allocation_scope": "subtract alloc-before.pprof from alloc.pprof; heap.pprof is post-GC live heap"}
	return errors.Join(err, json.NewEncoder(f).Encode(meta), f.Close())
}

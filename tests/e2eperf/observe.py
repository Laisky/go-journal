#!/usr/bin/env python3
"""Observe a whole campaign without adding code or allocations to the Go worker.

Raw monotonic samples explain possible host interference; missing counters are
explicit errors, not zero load. Observation does not certify dedicated hardware.
"""
import argparse
import json
import math
import os
from pathlib import Path
import threading
import time

from compare import run_supervisor
from supervised_exec import install_signal_handlers


COUNTERS = {
    'cpu': '/proc/stat', 'load': '/proc/loadavg', 'memory': '/proc/meminfo',
    'disk': '/proc/diskstats', 'host_cpu_pressure': '/proc/pressure/cpu',
    'host_io_pressure': '/proc/pressure/io', 'host_memory_pressure': '/proc/pressure/memory',
    'cgroup_cpu': '/sys/fs/cgroup/cpu.stat', 'cgroup_io': '/sys/fs/cgroup/io.stat',
    'cgroup_io_pressure': '/sys/fs/cgroup/io.pressure',
    'cgroup_cpu_pressure': '/sys/fs/cgroup/cpu.pressure',
    'cgroup_memory': '/sys/fs/cgroup/memory.current',
}


def read_counter(path):
    try:
        return {'path': str(path), 'text': Path(path).read_text()}
    except OSError as exc:
        return {'path': str(path), 'error': str(exc), 'errno': exc.errno}


def snapshot():
    row = {'monotonic_ns': time.monotonic_ns(), 'wall_ns': time.time_ns()}
    for name, path in COUNTERS.items():
        value = read_counter(path)
        if 'text' in value and name == 'cpu':
            value['text'] = '\n'.join(line for line in value['text'].splitlines()
                                      if line.startswith(('cpu ', 'ctxt ', 'procs_running ', 'procs_blocked ')))
        if 'text' in value and name == 'memory':
            value['text'] = '\n'.join(line for line in value['text'].splitlines()
                                      if line.startswith(('MemAvailable:', 'Dirty:', 'Writeback:', 'SwapFree:')))
        row[name] = value
    return row


def observe(command, out, timeout, interval=1.0):
    if not command or not math.isfinite(timeout) or not 0 < timeout <= 14400 or not math.isfinite(interval) or not .05 <= interval <= 60:
        raise ValueError('invalid observer command/deadline/interval')
    out.mkdir(parents=True, exist_ok=False)
    metadata = {'command': command, 'interval_seconds': interval,
                'affinity': sorted(os.sched_getaffinity(0)),
                'gomaxprocs': os.environ.get('GOMAXPROCS'),
                'cgroup_membership': read_counter('/proc/self/cgroup'),
                'mountinfo': read_counter('/proc/self/mountinfo'),
                'cpu_max': read_counter('/sys/fs/cgroup/cpu.max'),
                'scope': 'host and mounted cgroup root, not per-worker or proof of exclusive resources'}
    (out / 'observer.json').write_text(json.dumps(metadata, indent=2) + '\n')
    stop, failures = threading.Event(), []
    with (out / 'host.jsonl').open('x') as rows, (out / 'command.log').open('xb') as log:
        def record():
            rows.write(json.dumps(snapshot(), allow_nan=False) + '\n')
            rows.flush()  # No extra fsync workload is introduced by this sampler.

        def sample():
            try:
                record()
                while not stop.wait(interval):
                    record()
            except Exception as exc:
                failures.append(repr(exc))

        thread = threading.Thread(target=sample)
        thread.start()
        code = None
        try:
            code = run_supervisor(command, log, timeout)
        finally:
            stop.set()
            thread.join()
            record()
            (out / 'exit.json').write_text(json.dumps({'returncode': code, 'observer_errors': failures}) + '\n')
        if failures:
            raise RuntimeError(f'observer failed; command status {code}; inspect exit.json')
        return code


def main():
    install_signal_handlers()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--timeout', type=float, default=1800)
    p.add_argument('--interval', type=float, default=1)
    p.add_argument('command', nargs=argparse.REMAINDER)
    args = p.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    raise SystemExit(observe(command, args.out.resolve(), args.timeout, args.interval))


if __name__ == '__main__':
    main()

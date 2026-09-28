"""Event-driven worker observation, separate from journal performance.

The worker protocol is unchanged: save result.json, print an exact CHECKPOINT
line, then hold for SIGKILL. Linux pidfds report exit without a sampling tick;
stdout wakes checkpoint handling. The explicit fallback still polls exits.
"""
from __future__ import annotations

import errno
import math
import os
from pathlib import Path
import selectors
import subprocess
import time
from typing import BinaryIO, Callable

METHOD = 'events-v2'
READ_BYTES = 64 << 10
MAX_LOG_BYTES = 16 << 20


class CheckpointLine:
    """Constant-memory exact-line recognition across arbitrary read boundaries."""
    marker = b'\nCHECKPOINT\n'

    def __init__(self) -> None:
        self.tail = b'\n'  # The first output line also has a virtual delimiter.
        self.seen = False

    def feed(self, chunk: bytes) -> bool:
        data = self.tail + chunk
        self.seen = self.seen or self.marker in data
        self.tail = data[-(len(self.marker) - 1):]
        return self.seen


def pidfd_for(pid: int) -> tuple[int | None, str | None]:
    """Fall back only for unavailable/forbidden pidfds, not resource exhaustion."""
    if not hasattr(os, 'pidfd_open'):
        return None, 'pidfd_open unavailable'
    try:
        return os.pidfd_open(pid), None
    except OSError as exc:
        if exc.errno in (errno.ENOSYS, errno.EINVAL, errno.EPERM, errno.EACCES, errno.ESRCH):
            return None, f'pidfd_open errno={exc.errno}'
        raise


def write_all(stream: BinaryIO, chunk: bytes) -> None:
    remaining = memoryview(chunk)
    while remaining:
        written = stream.write(remaining)
        if written is None or written <= 0:
            raise OSError('short worker log write')
        remaining = remaining[written:]


def observe(process: subprocess.Popen, log: BinaryIO, *, checkpoint: Path | None,
            timeout: float, sampler: Callable[[int], dict], samples: list[dict],
            metadata: dict, started_ns: int, sample_interval: float = .02) -> int:
    """Observe, reap and retain evidence; callers persist metadata even on error.

    Owns stdout and pidfd, but the caller owns the process-kill finally block.
    A constant read budget per loop preserves deadline and sampling fairness.
    Missing checkpoints, truncated marker lines, excessive logs and timeouts fail
    closed. A checkpoint line must follow the result file; it cannot be repaired
    later by a graceful close. Descendants retaining stdout cannot block exit.
    """
    if (not math.isfinite(timeout) or timeout <= 0 or
            not math.isfinite(sample_interval) or sample_interval <= 0):
        raise ValueError('positive finite timeout and sample interval required')
    if process.stdout is None:
        raise ValueError('worker stdout must be a pipe')
    metadata.update(completed=False, method=METHOD, backend='uninitialized', fallback_reason=None,
                    started_ns=started_ns, first_output_ns=None,
                    checkpoint_ns=None, kill_sent_ns=None, exit_observed_ns=None,
                    sample_interval_ns=int(sample_interval * 1e9), samples=0,
                    stdout_bytes=0, stdout_eof=False, killed_at_checkpoint=False)
    deadline = time.monotonic() + timeout
    next_sample = time.monotonic()
    marker = CheckpointLine()
    pidfd = None
    pipe_fd = process.stdout.fileno()
    pipe_open = True
    try:
        os.set_blocking(pipe_fd, False)
        with selectors.DefaultSelector() as ready:
            ready.register(pipe_fd, selectors.EVENT_READ, 'stdout')
            pidfd, reason = pidfd_for(process.pid)
            metadata.update(backend='pidfd' if pidfd is not None else 'pipe-poll', fallback_reason=reason)
            if pidfd is not None:
                ready.register(pidfd, selectors.EVENT_READ, 'exit')

            def read_output() -> bool:
                nonlocal pipe_open
                if not pipe_open:
                    return False
                try:
                    chunk = os.read(pipe_fd, READ_BYTES)
                except BlockingIOError:
                    return False
                if not chunk:
                    ready.unregister(pipe_fd)
                    pipe_open = False
                    metadata['stdout_eof'] = True
                    return False
                if metadata['first_output_ns'] is None:
                    metadata['first_output_ns'] = time.monotonic_ns()
                metadata['stdout_bytes'] += len(chunk)
                write_all(log, chunk)
                if metadata['stdout_bytes'] > MAX_LOG_BYTES:
                    raise ValueError('worker log exceeds 16 MiB bound')
                marker.feed(chunk)
                return True

            while True:
                # Sample independently of stdout frequency, without catch-up bursts.
                current = time.monotonic()
                if current >= next_sample:
                    try:
                        samples.append(sampler(process.pid))
                        metadata['samples'] = len(samples)
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                    next_sample = current + sample_interval
                remaining = deadline - current
                if remaining <= 0:
                    raise TimeoutError('worker deadline')
                for key, _ in ready.select(min(remaining, max(0, next_sample-current))):
                    if key.data == 'stdout':
                        read_output()
                if checkpoint is not None and marker.seen and not metadata['killed_at_checkpoint']:
                    metadata['checkpoint_ns'] = time.monotonic_ns()
                    if not checkpoint.is_file():
                        raise ValueError('checkpoint line precedes result file')
                    if process.poll() is not None:
                        raise ValueError('checkpoint worker exited without waiting for SIGKILL')
                    process.kill()
                    metadata['kill_sent_ns'] = time.monotonic_ns()
                    metadata['killed_at_checkpoint'] = True
                code = process.poll()
                if code is not None:
                    metadata['exit_observed_ns'] = time.monotonic_ns()
                    # Drain only available bytes, never wait for inherited writers.
                    while read_output():
                        pass
                    if checkpoint is not None and not metadata['killed_at_checkpoint']:
                        raise ValueError('held worker exited without supervised checkpoint')
                    break
    finally:
        if pidfd is not None:
            os.close(pidfd)
        process.stdout.close()
    # Publish success only after output/pidfd cleanup. An exception during
    # draining or close must never be reconstructed offline as a valid trial.
    metadata['completed'] = True
    return code

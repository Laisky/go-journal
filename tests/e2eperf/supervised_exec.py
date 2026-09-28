#!/usr/bin/env python3
"""Linux exec guard: notify a nested supervisor even if its owner is SIGKILLed.

Configure prctl in a fresh single-threaded helper, never in preexec_fn. This
harness launches non-setuid programs from the owning main thread. Children that
are supervisors must handle SIGTERM and terminate their own process groups.
"""
import ctypes
import os
import signal
import sys


def install_signal_handlers():
    def stop(signum, _frame):
        raise SystemExit(128 + signum)
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, stop)


def main():
    if len(sys.argv) < 3:
        raise SystemExit('expected parent PID and command')
    parent = int(sys.argv[1])
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise OSError(ctypes.get_errno(), 'cannot arm supervisor parent-death signal')
    # Parent may have died between fork and prctl; do not execute orphan work.
    if os.getppid() != parent:
        raise SystemExit(125)
    os.execvp(sys.argv[2], sys.argv[2:])


if __name__ == '__main__':
    main()

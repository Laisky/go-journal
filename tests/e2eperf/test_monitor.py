import errno
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from monitor import CheckpointLine, observe, pidfd_for, write_all


class MarkerTest(unittest.TestCase):
    def test_every_split_and_bytewise_delivery(self):
        wire = b'ordinary log\nCHECKPOINT\ntrailing\n'
        for split in range(len(wire) + 1):
            marker = CheckpointLine()
            marker.feed(wire[:split])
            self.assertTrue(marker.feed(wire[split:]))
        marker = CheckpointLine()
        for byte in b'CHECKPOINT\n':
            marker.feed(bytes([byte]))
        self.assertTrue(marker.seen)

    def test_not_a_complete_exact_line(self):
        for wire in (b'CHECKPOINT', b'prefixCHECKPOINT\n', b'CHECKPOINT suffix\n', b'CHECKPOINT\r\n'):
            marker = CheckpointLine()
            self.assertFalse(marker.feed(wire))
        marker = CheckpointLine()
        for _ in range(32):
            marker.feed(b'x' * (64 << 10))
            self.assertLessEqual(len(marker.tail), len(marker.marker)-1)
        self.assertTrue(marker.feed(b'\nCHECKPOINT\n'))

    def test_short_log_writes(self):
        class Partial(io.BytesIO):
            def write(self, data):
                return super().write(data[:3])
        output = Partial()
        write_all(output, b'0123456789')
        self.assertEqual(output.getvalue(), b'0123456789')
        class Broken:
            def write(self, data):
                return 0
        with self.assertRaises(OSError):
            write_all(Broken(), b'x')


class MonitorTest(unittest.TestCase):
    def run_worker(self, script, *, checkpoint=None, timeout=5, interval=.02, sample=None):
        log = io.BytesIO()
        self.samples, self.metadata = [], {}
        started = time.monotonic_ns()
        process = subprocess.Popen([sys.executable, '-S', '-c', script], stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, bufsize=0)
        self.pid = process.pid
        try:
            code = observe(process, log, checkpoint=checkpoint, timeout=timeout,
                           sampler=sample or (lambda pid: {'pid': pid}), samples=self.samples,
                           metadata=self.metadata, started_ns=started, sample_interval=interval)
            return code, log.getvalue()
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            process.stdout.close()

    def test_quiet_exit_does_not_wait_for_resource_tick(self):
        if not hasattr(os, 'pidfd_open'):
            self.skipTest('kernel pidfd support required for notification-latency assertion')
        code, output = self.run_worker('raise SystemExit(7)', interval=2)
        self.assertEqual(code, 7)
        self.assertEqual(output, b'')
        if self.metadata['backend'] == 'pidfd':
            self.assertLess(self.metadata['exit_observed_ns'] - self.metadata['started_ns'], 1_500_000_000)
        self.assertIsNone(self.metadata['checkpoint_ns'])

    def test_fragmented_checkpoint_and_real_sigkill(self):
        with tempfile.TemporaryDirectory() as temp:
            cp = Path(temp)/'result.json'
            script = (f'import os,time;open({str(cp)!r},"w").write("{{}}");'
                      'os.write(1,b"CHECK");time.sleep(.03);os.write(1,b"POINT\\n");time.sleep(10)')
            code, output = self.run_worker(script, checkpoint=cp, interval=2)
            self.assertEqual(code, -9)
            self.assertEqual(output, b'CHECKPOINT\n')
            self.assertTrue(self.metadata['killed_at_checkpoint'])
            self.assertLess(self.metadata['kill_sent_ns'] - self.metadata['checkpoint_ns'], 500_000_000)

    def test_marker_cannot_precede_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError, 'precedes result'):
                self.run_worker('import os,time;os.write(1,b"CHECKPOINT\\n");time.sleep(10)',
                                checkpoint=Path(temp)/'absent')
        self.assertFalse(Path(f'/proc/{self.pid}').exists())

    def test_non_checkpoint_exit_is_not_successful_crash(self):
        with tempfile.TemporaryDirectory() as temp:
            cp = Path(temp)/'result.json'
            cp.write_text('{}')
            for script in ('raise SystemExit(0)', 'import os,signal;os.kill(os.getpid(),signal.SIGKILL)'):
                with self.assertRaisesRegex(ValueError, 'without supervised checkpoint'):
                    self.run_worker(script, checkpoint=cp)

    def test_truncated_marker_and_timeout_retains_output(self):
        with tempfile.TemporaryDirectory() as temp:
            cp = Path(temp)/'result.json'; cp.write_text('{}')
            with self.assertRaises(TimeoutError):
                self.run_worker('import os,time;os.write(1,b"CHECKPOINT");time.sleep(10)', checkpoint=cp, timeout=.2)
        self.assertFalse(self.metadata['killed_at_checkpoint'])
        self.assertFalse(Path(f'/proc/{self.pid}').exists())

    def test_early_stdout_eof_is_not_process_exit(self):
        with self.assertRaises(TimeoutError):
            self.run_worker('import os,time;os.close(1);os.close(2);time.sleep(10)', timeout=.2)
        self.assertTrue(self.metadata['stdout_eof'])

    def test_large_stdout_and_stderr_are_fully_drained(self):
        code, output = self.run_worker('import os;os.write(1,b"x"*262144);os.write(2,b"stderr\\n")')
        self.assertEqual(code, 0)
        self.assertEqual(output, b'x'*262144 + b'stderr\n')
        self.assertEqual(self.metadata['stdout_bytes'], len(output))

    def test_pidfd_fallback_is_explicit(self):
        with patch('monitor.pidfd_for', return_value=(None, 'test fallback')):
            code, _ = self.run_worker('raise SystemExit(0)')
        self.assertEqual(code, 0)
        self.assertEqual(self.metadata['backend'], 'pipe-poll')
        self.assertEqual(self.metadata['fallback_reason'], 'test fallback')
        with patch('monitor.os.pidfd_open', side_effect=OSError(errno.EMFILE, 'too many FDs')):
            with self.assertRaises(OSError):
                pidfd_for(os.getpid())

    def test_noisy_output_does_not_starve_deadline(self):
        with patch('monitor.MAX_LOG_BYTES', 1 << 30):
            with self.assertRaises(TimeoutError):
                self.run_worker('import os,time\nwhile True: os.write(1,b"x"*8192);time.sleep(.001)', timeout=.2)
        self.assertGreater(self.metadata['samples'], 1)

    def test_log_bound_fails_closed(self):
        with patch('monitor.MAX_LOG_BYTES', 100):
            with self.assertRaisesRegex(ValueError, 'log exceeds'):
                self.run_worker('import os,time;os.write(1,b"x"*101);time.sleep(10)')

    def test_sample_errors_are_not_hidden(self):
        def broken(pid):
            raise PermissionError('sampling denied')
        with self.assertRaises(PermissionError):
            self.run_worker('import time;time.sleep(10)', sample=broken)
        self.assertFalse(Path(f'/proc/{self.pid}').exists())

    def test_fds_are_released_after_exit_and_failure(self):
        initial = len(list(Path('/proc/self/fd').iterdir()))
        self.run_worker('pass')
        with self.assertRaises(TimeoutError):
            self.run_worker('import time;time.sleep(10)', timeout=.1)
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), initial)

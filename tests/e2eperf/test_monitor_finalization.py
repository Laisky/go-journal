"""A successfully exited worker is not enough when observer cleanup fails."""
import io
import subprocess
import sys
import time
import unittest

from monitor import observe


class FinalizationTest(unittest.TestCase):
    def test_close_error_cannot_publish_observer_success(self):
        process = subprocess.Popen([sys.executable, '-S', '-c', 'pass'], stdout=subprocess.PIPE)
        original = process.stdout

        class BrokenClose:
            def fileno(self):
                return original.fileno()

            def close(self):
                original.close()
                raise OSError('close failed')

        process.stdout = BrokenClose()
        metadata = {}
        try:
            with self.assertRaisesRegex(OSError, 'close failed'):
                observe(process, io.BytesIO(), checkpoint=None, timeout=5, sampler=lambda pid: {},
                        samples=[], metadata=metadata, started_ns=time.monotonic_ns())
            self.assertIsNotNone(metadata['exit_observed_ns'])
            self.assertFalse(metadata['completed'])
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            original.close()

    def test_success_is_explicit(self):
        process = subprocess.Popen([sys.executable, '-S', '-c', 'pass'], stdout=subprocess.PIPE)
        metadata = {}
        try:
            code = observe(process, io.BytesIO(), checkpoint=None, timeout=5, sampler=lambda pid: {},
                           samples=[], metadata=metadata, started_ns=time.monotonic_ns())
            self.assertEqual(code, 0)
            self.assertIs(metadata['completed'], True)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()

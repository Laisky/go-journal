from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock
from compare import run_supervisor, trial_deadline


def process_terminated(stat):
    """A task can vanish before open (ENOENT) or while reading procfs (ESRCH)."""
    try:
        state = stat.read_text().rsplit(')', 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return True
    return state == 'Z'


class SupervisorTest(unittest.TestCase):
    def test_deadline(self):
        self.assertEqual(trial_deadline({}), 930)
        self.assertEqual(trial_deadline({'timeout': 3600}), 18030)
        for value in (0, -1, 3601, float('inf'), float('nan'), True):
            with self.assertRaises(ValueError):
                trial_deadline({'timeout': value})

    def test_exit_status(self):
        with tempfile.TemporaryFile() as log:
            self.assertEqual(run_supervisor([sys.executable, '-c', 'raise SystemExit(7)'], log, 10), 7)
            self.assertEqual(run_supervisor([sys.executable, '-c', 'print("ok")'], log, 10), 0)

    def test_timeout_terminates_worker_group(self):
        with tempfile.TemporaryDirectory() as temp, tempfile.TemporaryFile() as log:
            marker = Path(temp)/'pid'
            script = ('import subprocess,sys,time; '
                      'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                      f'open({str(marker)!r},"w").write(str(p.pid)); time.sleep(60)')
            self.assertEqual(run_supervisor([sys.executable, '-c', script], log, 3), 124)
            self.assertTrue(marker.exists(), 'worker never started; test did not exercise cleanup')
            stat = Path('/proc')/marker.read_text()/'stat'
            # A killed orphan may await init reaping, but must not be running.
            for _ in range(100):
                if process_terminated(stat):
                    break
                time.sleep(.01)
            else:
                self.fail('worker survived controller timeout')

    def test_proc_exit_during_open_or_read_is_terminated(self):
        # Reproduce the native observer job's read-time ESRCH deterministically.
        for error in (FileNotFoundError(2, 'No such file'), ProcessLookupError(3, 'No such process')):
            with self.subTest(error=type(error)), mock.patch.object(Path, 'read_text', side_effect=error):
                self.assertTrue(process_terminated(Path('/unused/proc/stat')))

    def test_proc_permission_and_io_errors_are_not_success(self):
        for error in (PermissionError(13, 'Permission denied'), OSError(5, 'I/O error')):
            with self.subTest(error=type(error)), mock.patch.object(Path, 'read_text', side_effect=error):
                with self.assertRaises(type(error)):
                    process_terminated(Path('/unused/proc/stat'))

    def test_live_proc_states_do_not_pass_cleanup(self):
        for state in ('R', 'S', 'D', 'T', 'Z'):
            with self.subTest(state=state), mock.patch.object(Path, 'read_text', return_value=f'42 (worker name) {state} 1 2'):
                self.assertEqual(process_terminated(Path('/unused/proc/stat')), state == 'Z')

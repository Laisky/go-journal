from pathlib import Path
import sys
import tempfile
import time
import unittest
from compare import run_supervisor, trial_deadline


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
                try:
                    state = stat.read_text().rsplit(')', 1)[1].split()[0]
                except FileNotFoundError:
                    break
                if state == 'Z':
                    break
                time.sleep(.01)
            else:
                self.fail('worker survived controller timeout')

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class ParentDeathTest(unittest.TestCase):
    def test_killed_owner_stops_nested_session(self):
        here = str(Path(__file__).resolve().parent)
        for sig in (signal.SIGTERM, signal.SIGKILL):
            with self.subTest(signal=sig), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                marker = root/'pids'
                leaf = ('import os,json,time; from pathlib import Path; '
                        f'Path({str(marker)!r}).write_text(json.dumps([os.getppid(), os.getpid()])); '
                        'time.sleep(60)')
                middle = ('import sys; '
                          f'sys.path.insert(0,{here!r}); '
                          'from compare import run_supervisor; '
                          'from supervised_exec import install_signal_handlers; install_signal_handlers(); '
                          f'run_supervisor([sys.executable,"-c",{leaf!r}],sys.stdout,30)')
                owner = ('import sys; '
                         f'sys.path.insert(0,{here!r}); '
                         'from compare import run_supervisor; '
                         'from supervised_exec import install_signal_handlers; install_signal_handlers(); '
                         f'run_supervisor([sys.executable,"-c",{middle!r}],sys.stdout,30)')
                p = subprocess.Popen([sys.executable, '-c', owner], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                pids = []
                try:
                    until = time.monotonic()+10
                    while not marker.exists() and time.monotonic() < until:
                        time.sleep(.02)
                    self.assertTrue(marker.exists(), 'leaf never started')
                    # Atomicity is not provided by exists; wait for complete JSON.
                    while time.monotonic() < until:
                        try:
                            pids = json.loads(marker.read_text())
                            break
                        except json.JSONDecodeError:
                            time.sleep(.01)
                    self.assertEqual(len(pids), 2)
                    p.send_signal(sig)
                    p.wait(timeout=10)
                    for pid in pids:
                        until = time.monotonic()+5
                        while time.monotonic() < until:
                            try:
                                state = (Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()[0]
                            except FileNotFoundError:
                                break
                            if state == 'Z':
                                break
                            time.sleep(.02)
                        else:
                            self.fail(f'nested PID {pid} survived owner signal {sig}')
                finally:
                    if p.poll() is None:
                        p.kill()
                        p.wait()
                    for pid in pids:
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

    def test_already_orphaned_helper_does_not_execute_command(self):
        with tempfile.TemporaryDirectory() as temp:
            marker=Path(temp)/'should-not-exist'
            command=[sys.executable, str(Path(__file__).with_name('supervised_exec.py')), '0', sys.executable, '-c', f'open({str(marker)!r},"w").write("bad")']
            p=subprocess.run(command, check=False, timeout=10)
            self.assertEqual(p.returncode,125)
            self.assertFalse(marker.exists())

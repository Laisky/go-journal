#!/usr/bin/env python3
"""Freeze one driver/case set, run alternating pairs, retain every observation."""
import argparse
import hashlib
import json
import math
import os
import signal
from pathlib import Path
import shutil
import statistics
import subprocess
import sys

from report import analyze


def trial_deadline(case):
    seconds = case.get('timeout', 180)
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or not 0 < seconds <= 3600:
        raise ValueError('invalid worker deadline')
    return 5 * seconds + 30  # At most five independently supervised stages.


def run_supervisor(cmd, log, timeout):
    process = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def terminate():
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()

    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        terminate()  # Kill the controller AND its worker; never leak a load.
        return 124
    except BaseException:
        terminate()
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--candidate', type=Path, required=True)
    p.add_argument('--cases', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--pairs', type=int, default=5)
    args = p.parse_args()
    if not 1 <= args.pairs <= 10:
        raise ValueError('pairs must be between 1 and 10')
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    driver = Path(__file__).with_name('run.py')
    cases = json.loads(args.cases.read_text())
    if not cases or len({c['name'] for c in cases}) != len(cases):
        raise ValueError('empty or duplicate cases')
    if any(c.get('diagnostics') or c.get('profile_seconds') for c in cases):
        raise ValueError('diagnostic runs cannot enter paired timing comparisons')
    (root / 'cases.json').write_text(json.dumps(cases, indent=2) + '\n')
    report = {'expected_pairs': args.pairs, 'case_names': [c['name'] for c in cases],
              'driver_sha256': hashlib.sha256(driver.read_bytes()).hexdigest(), 'trials': [], 'medians': {}}

    def persist():
        (root / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')

    for case in cases:
        if not case['name'].replace('-', '').isalnum():
            raise ValueError('case name must be alphanumeric with hyphens')
        for pair in range(args.pairs):
            order = ('baseline', 'candidate') if pair % 2 == 0 else ('candidate', 'baseline')
            for side in order:
                target = root / f"{case['name']}-{pair}-{side}"
                cmd = [sys.executable, str(driver), '--binary', str(getattr(args, side).resolve()), '--out', str(target)]
                for key, value in case.items():
                    if key == 'name':
                        continue
                    if type(value) is bool:
                        if value:
                            cmd.append('--' + key.replace('_', '-'))
                    else:
                        cmd += ['--' + key.replace('_', '-'), str(value)]
                with open(root / (target.name + '.log'), 'x') as log:
                    code = run_supervisor(cmd, log, trial_deadline(case))
                trial = {'case': case['name'], 'pair': pair, 'side': side, 'returncode': code}
                report['trials'].append(trial)
                persist()
                if code:
                    raise RuntimeError(f'{target.name} failed; all artifacts retained, no retry')
                subprocess.run([sys.executable, str(driver), '--audit-only', str(target)], stdout=subprocess.DEVNULL, check=True)
                trial['summary'] = json.loads((target / 'summary.json').read_text())
                if trial['summary'].get('diagnostic_only') is not False:
                    raise ValueError('profiled trial cannot enter timing comparison')
                persist()
                # Only synthetic WAL files, after complete independent acceptance.
                shutil.rmtree(target / 'wal')
        values = {}
        for side in ('baseline', 'candidate'):
            trials = [t['summary'] for t in report['trials'] if t['case'] == case['name'] and t['side'] == side]
            values[side] = {
                'lifecycle_records_s': statistics.median(t['lifecycle_records_s'] for t in trials),
                'seed_sync_p99_ms': statistics.median(t['seed_sync_p99_ms'] for t in trials),
            }
            for phase in trials[0]['phases']:
                values[side][phase] = {key: statistics.median(t['phases'][phase][key] for t in trials)
                                      for key in ('seconds', 'cpu_seconds', 'allocated_bytes', 'peak_rss_mib')}
        report['medians'][case['name']] = values
        persist()
    report['assessment'] = analyze(report)
    persist()
    print(json.dumps(report['medians'], indent=2, allow_nan=False))


if __name__ == '__main__':
    main()

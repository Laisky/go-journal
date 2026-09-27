#!/usr/bin/env python3
"""Freeze one driver/case set, run alternating pairs, retain every observation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--candidate', type=Path, required=True)
    p.add_argument('--cases', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--pairs', type=int, default=3)
    args = p.parse_args()
    if not 1 <= args.pairs <= 10:
        raise ValueError('pairs must be between 1 and 10')
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    driver = Path(__file__).with_name('run.py')
    cases = json.loads(args.cases.read_text())
    (root / 'cases.json').write_text(json.dumps(cases, indent=2) + '\n')
    report = {'driver_sha256': hashlib.sha256(driver.read_bytes()).hexdigest(), 'trials': [], 'medians': {}}
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
                    result = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=900)
                trial = {'case': case['name'], 'pair': pair, 'side': side, 'returncode': result.returncode}
                report['trials'].append(trial)
                (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
                if result.returncode:
                    raise RuntimeError(f'{target.name} failed; all artifacts retained, no retry')
                subprocess.run([sys.executable, str(driver), '--audit-only', str(target)], stdout=subprocess.DEVNULL, check=True)
                trial['summary'] = json.loads((target / 'summary.json').read_text())
                # Only synthetic WAL files, after complete independent acceptance.
                import shutil
                shutil.rmtree(target / 'wal')
        values = {}
        for side in ('baseline', 'candidate'):
            trials = [t['summary'] for t in report['trials'] if t['case'] == case['name'] and t['side'] == side]
            values[side] = {
                'lifecycle_records_s': statistics.median(t['lifecycle_records_s'] for t in trials),
                'seed_sync_p99_ms': statistics.median(t['seed_sync_p99_ms'] for t in trials),
            }
            for phase in ('scan/scan', 'transfer/frontier', 'transfer/replay_transfer'):
                values[side][phase] = {key: statistics.median(t['phases'][phase][key] for t in trials)
                                      for key in ('seconds', 'cpu_seconds', 'allocated_bytes', 'peak_rss_mib')}
        report['medians'][case['name']] = values
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report['medians'], indent=2))


if __name__ == '__main__':
    main()

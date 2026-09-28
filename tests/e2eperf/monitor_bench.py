#!/usr/bin/env python3
"""Measure controller notification overhead, NOT journal throughput.

Both methods run identical synthetic checkpoint producers. The poll method is
retained solely as an explicit harness negative/control implementation. All
paired samples remain in the report; no exclusions or automatic retries.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from monitor import observe
from report import paired_effect
from run import file_digest, proc_sample, save
from supervised_exec import install_signal_handlers


WORKER = '''import json,os,sys,time
path,delay = sys.argv[1],float(sys.argv[2])
time.sleep(delay)
with open(path,"x") as f:
    json.dump({"ready_ns":time.monotonic_ns()},f)
os.write(1,b"CHECKPOINT\\n")
while True: time.sleep(60)
'''


def trial(out, side, delay):
    out.mkdir()
    checkpoint = out/'result.json'
    samples, metadata = [], {}
    command = [sys.executable, '-S', '-c', WORKER, str(checkpoint), str(delay)]
    guard = [sys.executable, '-S', str(Path(__file__).with_name('supervised_exec.py')),
             str(os.getpid()), *command]
    save(out/'command.json', command)
    with open(out/'worker.log', 'xb', buffering=0) as log:
        start = time.monotonic_ns()
        process = subprocess.Popen(guard, stdout=subprocess.PIPE if side == 'events' else log,
                                   stderr=subprocess.STDOUT, bufsize=0)
        try:
            if side == 'events':
                code = observe(process, log, checkpoint=checkpoint, timeout=10, sampler=proc_sample,
                               samples=samples, metadata=metadata, started_ns=start)
                exit_ns = metadata['exit_observed_ns']
            else:
                deadline = time.monotonic() + 10
                while process.poll() is None:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('poll control deadline')
                    try:
                        samples.append(proc_sample(process.pid))
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                    if checkpoint.exists() and b'CHECKPOINT\n' in (out/'worker.log').read_bytes():
                        process.kill()
                        break
                    time.sleep(.02)
                code = process.wait(timeout=5)
                exit_ns = time.monotonic_ns()
            if code != -9:
                raise ValueError(f'checkpoint fixture exited {code}')
            ready_ns = json.loads(checkpoint.read_text())['ready_ns']
            value = {'side': side, 'duration_ns': exit_ns-start,
                     'completion_observation_ns': exit_ns-ready_ns, 'returncode': code,
                     'backend': metadata.get('backend', 'poll-v1'), 'delay_seconds': delay}
            save(out/'observation.json', value)
            save(out/'resources.json', samples)
            return value
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
            if process.stdout is not None:
                process.stdout.close()


def main():
    install_signal_handlers()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--pairs', type=int, default=20)
    args = parser.parse_args()
    if not 5 <= args.pairs <= 100:
        raise ValueError('use 5..100 counterbalanced pairs')
    root = args.out.resolve(); root.mkdir(parents=True, exist_ok=False)
    report = {'scope': 'synthetic controller overhead; not journal performance', 'pairs': args.pairs,
              'python': sys.version, 'trials': [], 'source_sha256': {
                  name: file_digest(Path(__file__).with_name(name))
                  for name in ('monitor_bench.py', 'monitor.py', 'run.py', 'report.py', 'supervised_exec.py')}}
    try:
        for pair in range(args.pairs):
            for side in (('poll', 'events') if pair % 2 == 0 else ('events', 'poll')):
                value = trial(root/f'{pair}-{side}', side, (pair % 4)*.004)
                value['pair'] = pair
                report['trials'].append(value)
    finally:
        save(root/'raw-report.json', report)
    for metric in ('duration_ns', 'completion_observation_ns'):
        values = {side: [r[metric] for r in report['trials'] if r['side'] == side] for side in ('poll','events')}
        report[metric] = paired_effect(values['poll'], values['events'])
    save(root/'report.json', report)
    print(json.dumps({k: report[k] for k in ('scope','duration_ns','completion_observation_ns')}, indent=2))


if __name__ == '__main__':
    main()

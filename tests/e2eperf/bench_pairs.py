#!/usr/bin/env python3
"""Fixed-work public-API benchmark pairs. Keep raw process output and failures."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys

from compare import run_supervisor
from report import paired_effect
from supervised_exec import install_signal_handlers

SUITES = {
    'ack': ('BenchmarkPublicACKFrontier', ['plain-8k','plain-128k','plain-128k-segments','gzip-control']),
    'directory': ('BenchmarkPublicDirectorySnapshot', ['16','256','4096']),
}


def checksum(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse(raw, suite, iterations):
    prefix, cases = SUITES[suite]
    results = {}
    for line in raw.splitlines():
        if not line.startswith(prefix+'/'):
            continue
        parts = line.split()
        name = re.sub(r'-\d+$','',parts[0].split('/',1)[1])
        if name not in cases or name in results or int(parts[1]) != iterations:
            raise ValueError('different/duplicate benchmark work')
        fields = {}
        for i in range(2,len(parts),2):
            value = float(parts[i])
            if not math.isfinite(value) or value < 0:
                raise ValueError('invalid metric')
            fields[parts[i+1]] = value
        if any(key not in fields for key in ('ns/op','B/op','allocs/op')) or fields['ns/op'] == 0:
            raise ValueError('missing/invalid benchmark counters')
        results[name] = {k:fields[k] for k in ('ns/op','B/op','allocs/op')}
    if set(results) != set(cases) or 'PASS' not in raw.splitlines() or 'FAIL' in raw.splitlines():
        raise ValueError('incomplete/failed benchmark suite')
    return results


def assess(trials, suite, pairs):
    prefix, cases = SUITES[suite]
    if len(trials)!=pairs*2:
        raise ValueError('incomplete pairs')
    indexed={}
    for trial in trials:
        key=(trial['pair'],trial['side'])
        if key in indexed or not 0<=key[0]<pairs or key[1] not in ('baseline','candidate') or trial['returncode']!=0:
            raise ValueError('failed or duplicate trial')
        indexed[key]=trial['results']
    return {name:{metric:paired_effect(
        [indexed[(i,'baseline')][name][metric] for i in range(pairs)],
        [indexed[(i,'candidate')][name][metric] for i in range(pairs)])
        for metric in ('ns/op','B/op','allocs/op')} for name in cases}


def verify(root):
    report=json.loads((root/'report.json').read_text())
    for trial in report['trials']:
        recomputed=parse((root/trial['log']).read_text(),report['suite'],report['iterations'])
        if recomputed!=trial['results'] or checksum(root/trial['log'])!=trial['log_sha256']:
            raise ValueError('raw benchmark evidence changed')
    result=assess(report['trials'],report['suite'],report['pairs'])
    if result!=report['assessment']:
        raise ValueError('assessment changed')
    return report


def main():
    install_signal_handlers()
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline',type=Path)
    p.add_argument('--candidate',type=Path)
    p.add_argument('--out',type=Path)
    p.add_argument('--suite',choices=SUITES,default='ack')
    p.add_argument('--pairs',type=int,default=5)
    p.add_argument('--iterations',type=int,default=64)
    p.add_argument('--verify',type=Path)
    a=p.parse_args()
    if a.verify:
        report=verify(a.verify); print(json.dumps(report['assessment'],indent=2));return
    if not a.baseline or not a.candidate or not a.out or not 1<=a.pairs<=10 or not 1<=a.iterations<=1024:
        raise ValueError('invalid benchmark arguments')
    a.out.mkdir(parents=True,exist_ok=False)
    report={'suite':a.suite,'pairs':a.pairs,'iterations':a.iterations,'trials':[],
            'binaries':{side:checksum(getattr(a,side)) for side in ('baseline','candidate')}}
    def save():
        (a.out/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    save()
    for pair in range(a.pairs):
        for side in (('baseline','candidate') if pair%2==0 else ('candidate','baseline')):
            log=a.out/f'{pair}-{side}.log'
            cmd=[str(getattr(a,side).resolve()),'-test.run=^$',f'-test.bench=^{SUITES[a.suite][0]}$',
                 f'-test.benchtime={a.iterations}x','-test.benchmem','-test.timeout=180s']
            with log.open('x') as output:
                code=run_supervisor(cmd,output,240)
            trial={'pair':pair,'side':side,'command':cmd,'returncode':code,'log':log.name,
                   'log_sha256':checksum(log)}
            report['trials'].append(trial);save()
            if code!=0:
                raise RuntimeError('benchmark failed; raw output retained, no automatic retry')
            trial['results']=parse(log.read_text(),a.suite,a.iterations);save()
    report['assessment']=assess(report['trials'],a.suite,a.pairs);save()
    print(json.dumps(report['assessment'],indent=2,allow_nan=False))


if __name__=='__main__':
    main()

#!/usr/bin/env python3
"""Fail-closed allocation/CPU regression gate, not a durable-latency certificate.

One block runs reference, an identical-reference control, and candidate in all
six counterbalanced orders. Every raw result is retained. No automatic retries.
"""
from __future__ import annotations
import argparse
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys

METRICS = ('cpu_ns/op', 'wall_ns/op', 'B/op', 'allocs/op')
SIDES = ('reference', 'control', 'candidate')


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def load(path):
    def unique(items):
        result = {}
        for key, value in items:
            require(key not in result, f'duplicate JSON key: {key}')
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique)


def save(path, value):
    # Reports are replaced atomically; raw samples are create-only by the worker.
    path = Path(path)
    tmp = path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value, indent=2, allow_nan=False)+'\n')
    os.replace(tmp, path)


def validate_policy(p):
    require(p['schema'] == 1 and type(p['iterations']) is int and 32 <= p['iterations'] <= 4096, 'invalid iteration policy')
    require(type(p['rounds']) is int and 9 <= p['rounds'] <= 20, 'insufficient/excessive rounds')
    require(p['gomaxprocs'] == 4, 'unsupported GOMAXPROCS policy')
    require(len(p['reference']) == 40 and all(c in '0123456789abcdef' for c in p['reference']), 'invalid pinned reference')
    for key in ('cpu_ratio_limit', 'relative_bytes_ratio', 'relative_bytes_slack', 'relative_allocs_ratio', 'relative_allocs_slack'):
        require(type(p[key]) in (float, int) and math.isfinite(p[key]) and p[key] > 0, 'invalid threshold')
    require(len(p['control_interval']) == 2 and 0 < p['control_interval'][0] < 1 < p['control_interval'][1], 'invalid A/A interval')
    require(isinstance(p['cases'], dict) and 0 < len(p['cases']) <= 32, 'invalid case manifest')
    for name, rule in p['cases'].items():
        require(name.replace('-', '').isalnum(), 'unsafe case name')
        require(type(rule['cpu_gate']) is bool, 'invalid CPU gate')
        for key in ('max_bytes', 'max_allocs'):
            require(type(rule[key]) is int and rule[key] >= 0, 'invalid allocation budget')


def interval(values):
    """Deterministic 99% paired bootstrap interval of the median ratio."""
    rng = random.Random(20260928)
    sampled = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(5000))
    return [sampled[24], sampled[4974]]


def assessment(report):
    p = report['policy']
    validate_policy(p)
    require(report.get('schema') == 1, 'unknown report format')
    require(set(report['binary_sha256']) == set(SIDES), 'missing binary identity')
    require(report['binary_sha256']['reference'] == report['binary_sha256']['control'], 'A/A used different binaries')
    for value in report['binary_sha256'].values():
        require(len(value) == 64 and all(c in '0123456789abcdef' for c in value), 'bad executable digest')
    require(len(report['trials']) == p['rounds']*3, 'incomplete campaign')
    rows, versions = {}, set()
    orders = list(itertools.permutations(SIDES))
    for position, t in enumerate(report['trials']):
        block = position//3
        side = orders[block % len(orders)][position % 3]
        require(type(t['round']) is int and t['round'] == block and t['side'] == side, 'changed counterbalanced order')
        require(type(t['returncode']) is int and t['returncode'] == 0, 'failed benchmark process')
        raw = t['result']
        require(raw['schema'] == 1 and type(raw['gomaxprocs']) is int and raw['gomaxprocs'] == p['gomaxprocs'], 'changed runtime configuration')
        require(isinstance(raw['go'], str) and raw['go'].startswith('go1.'), 'missing compiler identity')
        versions.add(raw['go'])
        require(set(raw['results']) == set(p['cases']), 'missing/extra benchmark cases')
        for name, values in raw['results'].items():
            require(set(values) == {'operations', *METRICS}, 'different metric set')
            require(type(values['operations']) is int and values['operations'] == p['iterations'], 'different measured work')
            for key in METRICS:
                v = values[key]
                require(type(v) in (int, float) and math.isfinite(v) and v >= 0, 'invalid metric')
            require(values['cpu_ns/op'] > 0 and values['wall_ns/op'] > 0, 'empty timing interval')
        rows[(block, side)] = raw['results']
    require(len(versions) == 1, 'different Go versions')
    issues, cases = [], {}
    for name, rule in p['cases'].items():
        samples = {side: [rows[(i, side)][name] for i in range(p['rounds'])] for side in SIDES}
        medians = {side: {m: statistics.median(v[m] for v in sample) for m in METRICS} for side, sample in samples.items()}
        for metric, ceiling in (('B/op', rule['max_bytes']), ('allocs/op', rule['max_allocs'])):
            if max(v[metric] for v in samples['candidate']) > ceiling:
                issues.append({'case': name, 'kind': 'absolute-allocation', 'metric': metric, 'limit': ceiling})
            ratio, slack = ((p['relative_bytes_ratio'], p['relative_bytes_slack']) if metric == 'B/op' else
                            (p['relative_allocs_ratio'], p['relative_allocs_slack']))
            before = statistics.median(v[metric] for side in ('reference', 'control') for v in samples[side])
            limit = before*ratio+slack
            if medians['candidate'][metric] > limit:
                issues.append({'case': name, 'kind': 'relative-allocation', 'metric': metric, 'limit': limit})
        control = [samples['control'][i]['cpu_ns/op']/samples['reference'][i]['cpu_ns/op'] for i in range(p['rounds'])]
        ratios = [samples['candidate'][i]['cpu_ns/op']/statistics.mean(samples[s][i]['cpu_ns/op'] for s in ('reference', 'control')) for i in range(p['rounds'])]
        ci, aa = interval(ratios), interval(control)
        if rule['cpu_gate']:
            if aa[0] < p['control_interval'][0] or aa[1] > p['control_interval'][1]:
                issues.append({'case': name, 'kind': 'unstable-control', 'interval99': aa})
            if ci[0] > p['cpu_ratio_limit']:
                issues.append({'case': name, 'kind': 'cpu-regression', 'interval99': ci})
            elif ci[1] > p['cpu_ratio_limit']:
                issues.append({'case': name, 'kind': 'inconclusive-cpu', 'interval99': ci})
        cases[name] = {'medians': medians, 'cpu_gate': rule['cpu_gate'], 'cpu_ratio_median': statistics.median(ratios),
                       'cpu_ratio_interval99': ci, 'control_interval99': aa}
    return {'passed': not issues, 'issues': issues, 'cases': cases,
            'scope': 'fixed-work CPU/allocation gate; wall time is diagnostic, not a durable-latency/SLO qualification'}


def verify(root):
    root = Path(root)
    report = load(root/'report.json')
    require(report['policy_sha256'] == digest(root/'policy.json'), 'policy digest mismatch')
    require(report['policy'] == load(root/'policy.json'), 'policy changed')
    for t in report['trials']:
        require(t['file'] == f"{t['round']}-{t['side']}.json", 'unexpected raw sample path')
        path = root/t['file']
        require(digest(path) == t['sha256'] and load(path) == t['result'], 'raw evidence changed')
        require(digest(root/t['log']) == t['log_sha256'], 'process log changed')
    current = assessment(report)
    require(current == report['assessment'], 'stored assessment differs')
    return current


def summary(report):
    a = report['assessment']
    lines = ['## Benchmark regression gate', '', '**'+('PASS' if a['passed'] else 'FAIL')+'**', '',
             '| Case | Candidate B/op | CPU ratio | CPU gate |', '|---|---:|---:|---|']
    for name, result in a['cases'].items():
        lines.append(f"| {name} | {result['medians']['candidate']['B/op']:.1f} | {result['cpu_ratio_median']:.3f} | {result['cpu_gate']} |")
    lines += ['', 'Wall-clock/fsync p99 is diagnostic only; this result is not a production SLO certificate.']
    for issue in a['issues']:
        lines.append(f"\n- {issue['case']}: **{issue['kind']}**")
    return '\n'.join(lines)+'\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--policy', type=Path, default=Path(__file__).with_name('policy.json'))
    parser.add_argument('--case', default='', help='one case only, for executable negative controls')
    parser.add_argument('--verify', type=Path)
    args = parser.parse_args()
    if args.verify:
        a = verify(args.verify)
        print(json.dumps(a, indent=2))
        return 0 if a['passed'] else 1
    require(args.reference and args.candidate and args.out, 'reference, candidate and new output path required')
    policy = load(args.policy)
    validate_policy(policy)
    if args.case:
        require(args.case in policy['cases'], 'unknown selected case')
        policy['cases'] = {args.case: policy['cases'][args.case]}
    args.out.mkdir(parents=True, exist_ok=False)
    save(args.out/'policy.json', policy)
    binaries = {'reference': args.reference.resolve(), 'control': args.reference.resolve(), 'candidate': args.candidate.resolve()}
    report = {'schema': 1, 'policy': policy, 'policy_sha256': digest(args.out/'policy.json'),
              'binary_sha256': {s: digest(p) for s, p in binaries.items()}, 'trials': []}
    save(args.out/'report.json', report)
    orders = list(itertools.permutations(SIDES))
    for i in range(policy['rounds']):
        for side in orders[i % len(orders)]:
            raw = args.out/f'{i}-{side}.json'
            log = args.out/f'{i}-{side}.log'
            cmd = [str(binaries[side]), '--iterations', str(policy['iterations']), '--out', str(raw.resolve())]
            if args.case:
                cmd += ['--case', args.case]
            with log.open('xb') as stream:
                try:
                    code = subprocess.run(cmd, stdout=stream, stderr=subprocess.STDOUT,
                        timeout=120, env=dict(os.environ, GOMAXPROCS=str(policy['gomaxprocs']))).returncode
                except subprocess.TimeoutExpired:
                    code = 124
            t = {'round': i, 'side': side, 'command': cmd, 'returncode': code, 'file': raw.name,
                 'log': log.name, 'log_sha256': digest(log)}
            report['trials'].append(t)
            save(args.out/'report.json', report)
            require(code == 0 and raw.is_file(), 'worker failed; all observations retained, no retry')
            t.update(result=load(raw), sha256=digest(raw))
            save(args.out/'report.json', report)
    report['assessment'] = assessment(report)
    save(args.out/'report.json', report)
    verify(args.out)
    text = summary(report)
    (args.out/'summary.md').write_text(text)
    print(text)
    if 'GITHUB_STEP_SUMMARY' in os.environ:
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
            f.write(text)
    return 0 if report['assessment']['passed'] else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f'Benchmark gate: invalid or incomplete evidence: {exc}', file=sys.stderr)
        raise SystemExit(2)

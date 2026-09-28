#!/usr/bin/env python3
"""Paired exploratory effect estimates; never silently accept incomplete evidence.

Ratios are candidate/baseline, so lower is better for every reported metric.
The 95% paired bootstrap interval is descriptive (no multiplicity correction),
not a capacity certificate or an automatic merge decision.
"""
import argparse
import json
import math
from pathlib import Path
import random
import statistics


def paired_effect(before, after):
    if len(before) != len(after) or not before:
        raise ValueError('incomplete pairs')
    for v in before + after:
        if type(v) not in (int, float) or not math.isfinite(v) or v < 0:
            raise ValueError('invalid metric')
    out = {'baseline': before, 'candidate': after,
           'baseline_median': statistics.median(before), 'candidate_median': statistics.median(after)}
    if any(v == 0 for v in before):
        return dict(out, status='zero-baseline', median_ratio=None, interval95=None)
    ratios = [b / a for a, b in zip(before, after)]
    rng = random.Random(27092026)
    samples = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(4000))
    low, high = samples[99], samples[3899]
    status = 'inconclusive'
    if len(ratios) < 5:
        status = 'insufficient-pairs'
    elif high < .95:
        status = 'improved'
    elif low > 1.05:
        status = 'regressed'
    out.update(median_ratio=statistics.median(ratios), paired_ratios=ratios,
               interval95=[low, high], status=status)
    return out


def metrics(summary):
    if summary.get('passed') is not True or summary.get('diagnostic_only') is not False:
        raise ValueError('only explicitly unprofiled, audited trials may be compared')
    out = {key: summary[key] for key in ('lifecycle_seconds', 'seed_sync_p99_ms')}
    for name, phase in summary['phases'].items():
        for key in ('seconds', 'cpu_seconds', 'allocated_bytes', 'peak_rss_mib'):
            out[f'{name}/{key}'] = phase[key]
    return out


def analyze(report):
    count = report['expected_pairs']
    if type(count) is not int or not 1 <= count <= 10:
        raise ValueError('invalid expected pair count')
    cases = report['case_names']
    if not cases or len(cases) != len(set(cases)):
        raise ValueError('empty/duplicate cases')
    groups = {}
    for trial in report['trials']:
        key = (trial['case'], trial['pair'], trial['side'])
        if key in groups or trial['case'] not in cases or type(trial['pair']) is not int or not 0 <= trial['pair'] < count or trial['side'] not in ('baseline', 'candidate') or trial['returncode'] != 0:
            raise ValueError('duplicate, unexpected or failed trial')
        groups[key] = trial['summary']
    if len(groups) != len(cases) * count * 2:
        raise ValueError('incomplete campaign')
    output = {}
    for case in cases:
        raw = [[groups[(case, i, side)] for i in range(count)] for side in ('baseline', 'candidate')]
        if len({s.get('measurement_method', 'poll-v1') for side in raw for s in side}) != 1:
            raise ValueError('different observation methods; not a library performance comparison')
        backends = [tuple(s.get('observer_backends', ['poll-v1'])) for side in raw for s in side]
        if any(len(b) != 1 for b in backends) or len(set(backends)) != 1:
            raise ValueError('mixed observation backends; timing is not comparable')
        if len({s['count'] for side in raw for s in side}) != 1:
            raise ValueError('different record counts')
        work = [{name: p['ops'] for name, p in s['phases'].items()} for side in raw for s in side]
        if any(w != work[0] for w in work):
            raise ValueError('different phase work')
        values = [[metrics(s) for s in side] for side in raw]
        keys = set(values[0][0])
        if any(set(v) != keys for side in values for v in side):
            raise ValueError('different metric sets')
        output[case] = {key: paired_effect([v[key] for v in values[0]], [v[key] for v in values[1]]) for key in sorted(keys)}
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('report', type=Path)
    args = p.parse_args()
    print(json.dumps(analyze(json.loads(args.report.read_text())), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()

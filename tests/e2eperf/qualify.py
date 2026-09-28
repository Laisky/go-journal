#!/usr/bin/env python3
"""Conservative timing qualification, separate from correctness and byte counts.

Freeze these limits before running a campaign. Never retry or remove a failing
control to obtain qualification. Passing is not a production capacity guarantee.
"""
import argparse
import json
from pathlib import Path

from report import analyze


POLICY = {'min_pairs': 5, 'ratio_interval': [.9, 1.1], 'max_sample_span': 1.25,
          'metrics': ['lifecycle_seconds', 'seed_sync_p99_ms']}


def qualify(controls):
    if set(controls) != {'before', 'after'}:
        raise ValueError('both before and after controls are required')
    results = {}
    for position, report in controls.items():
        assessment = analyze(report)
        if assessment != report.get('assessment'):
            raise ValueError('control assessment differs from raw pairs')
        issues, observed = [], {}
        if report['expected_pairs'] < POLICY['min_pairs']:
            issues.append('fewer than five pairs')
        for case, metrics in assessment.items():
            observed[case] = {}
            for key in POLICY['metrics']:
                value = metrics[key]
                interval = value['interval95']
                spans = []
                for side in ('baseline', 'candidate'):
                    samples = value[side]
                    spans.append(max(samples)/min(samples) if min(samples) > 0 else None)
                observed[case][key] = {'interval95': interval, 'sample_spans': spans}
                if interval is None or interval[0] < .9 or interval[1] > 1.1:
                    issues.append(f'{case}/{key}: paired interval outside [0.9, 1.1]')
                if any(span is None or span > POLICY['max_sample_span'] for span in spans):
                    issues.append(f'{case}/{key}: sample max/min exceeds 1.25')
        results[position] = {'passed': not issues, 'issues': issues, 'observed': observed}
    return {'timing_qualified': all(r['passed'] for r in results.values()),
            'policy': POLICY, 'controls': results,
            'scope': 'only these control workloads and this campaign; not equivalence or dedicated-host certification'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--before', type=Path, required=True)
    p.add_argument('--after', type=Path, required=True)
    args = p.parse_args()
    controls = {key: json.loads(getattr(args, key).read_text()) for key in ('before', 'after')}
    # An unqualified measurement is retained, not retried or converted to a
    # correctness failure. Callers must read timing_qualified before claiming speed.
    print(json.dumps(qualify(controls), indent=2, allow_nan=False))


if __name__ == '__main__':
    main()

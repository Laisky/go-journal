#!/usr/bin/env python3
"""Freeze a bounded concurrency/payload/ACK/codec matrix, then run paired E2E loads.

These are fixed-work, closed-loop loads with real Sync and crash/recovery. They
are not an open-loop offered-rate test or a production sustainable-capacity claim.
"""
import argparse
import itertools
import json
from pathlib import Path
import subprocess
import sys


def integers(text, low, high):
    values = [int(v) for v in text.split(',')]
    if not values or len(set(values)) != len(values) or any(not low <= v <= high for v in values):
        raise ValueError(f'expected unique integers in [{low}, {high}]')
    return values


def cases(count, payloads, writers, acks, codecs, scans):
    if not 1 <= count <= 1000000 or not 0 <= scans <= 10000:
        raise ValueError('record/scan bound')
    if not codecs or len(set(codecs)) != len(codecs) or any(c not in ('plain', 'gzip') for c in codecs):
        raise ValueError('codecs must be unique plain,gzip values')
    if len(payloads)*len(writers)*len(acks)*len(codecs) > 36:
        raise ValueError('matrix exceeds 36 cases; split the campaign explicitly')
    output = []
    for payload, writer, ack, codec in itertools.product(payloads, writers, acks, codecs):
        if not 0 <= payload <= 4 << 20 or not 1 <= writer <= 128 or not 0 <= ack <= 100:
            raise ValueError('workload bounds')
        if count*(payload+256) > 2 << 30:
            raise ValueError('per-trial synthetic disk-work bound (2 GiB)')
        output.append({'name': f'{codec}-p{payload}-w{writer}-ack{ack}',
                       'count': count, 'payload': payload, 'writers': writer,
                       'ack_percent': ack, 'gzip': codec == 'gzip', 'scans': scans,
                       'timeout': 180})
    if not output or len({c['name'] for c in output}) != len(output):
        raise ValueError('empty or duplicate matrix')
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--candidate', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--count', type=int, default=4096)
    p.add_argument('--payloads', default='1024')
    p.add_argument('--writers', default='1,4,16')
    p.add_argument('--ack-percents', default='50')
    p.add_argument('--codecs', default='plain,gzip')
    p.add_argument('--scans', type=int, default=4)
    p.add_argument('--pairs', type=int, default=5)
    p.add_argument('--generate-only', action='store_true')
    a = p.parse_args()
    if not 1 <= a.pairs <= 10:
        raise ValueError('pair count must be in [1, 10]')
    matrix = cases(a.count, integers(a.payloads, 0, 4 << 20), integers(a.writers, 1, 128),
                   integers(a.ack_percents, 0, 100), a.codecs.split(','), a.scans)
    a.out.mkdir(parents=True, exist_ok=False)
    frozen = a.out.resolve()/'cases.json'
    frozen.write_text(json.dumps(matrix, indent=2)+'\n')
    if not a.generate_only:
        subprocess.run([sys.executable, str(Path(__file__).with_name('compare.py')),
                        '--baseline', str(a.baseline.resolve()), '--candidate', str(a.candidate.resolve()),
                        '--cases', str(frozen), '--out', str(a.out.resolve()/'trials'),
                        '--pairs', str(a.pairs)], check=True)
    print(f'{len(matrix)} cases; {len(matrix)*a.pairs*2} full audited lifecycle trials')


if __name__ == '__main__':
    main()

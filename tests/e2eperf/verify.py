#!/usr/bin/env python3
"""Verify a downloaded evidence manifest and recompute every paired trial.

Run from trusted repository source, not by executing scripts from an artifact.
Any verification output must be saved outside the immutable evidence directory.
"""
import argparse
import hashlib
import json
import statistics
from pathlib import Path

from report import analyze
from run import audit, read, require


def checked_path(root, name):
    relative = Path(name)
    require(not relative.is_absolute() and '..' not in relative.parts, 'unsafe evidence path')
    path = root / relative
    require(path.resolve().is_relative_to(root), 'evidence path escapes root')
    require(not any((root / Path(*relative.parts[:n])).is_symlink() for n in range(1, len(relative.parts) + 1)), 'symlinked evidence path')
    require(path.is_file(), f'missing/nonregular evidence: {name}')
    return path


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def manifest(root):
    listed = {}
    for line in (root / 'SHA256SUMS').read_text().splitlines():
        expected, name = line.split('  ', 1)
        name = name.removeprefix('./')
        require(name not in listed and name != 'SHA256SUMS', 'duplicate/self-referential manifest entry')
        require(len(expected) == 64 and all(c in '0123456789abcdef' for c in expected), 'invalid digest')
        require(sha256(checked_path(root, name)) == expected, f'checksum mismatch: {name}')
        listed[name] = expected
    actual = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p != root / 'SHA256SUMS'}
    require(actual == set(listed), 'manifest omits evidence files')
    require(bool(listed), 'empty evidence manifest')
    return listed


def verify_campaign(root, campaign, binaries, hashes):
    folder = root / campaign
    require(folder.resolve().is_relative_to(root), 'campaign escapes evidence root')
    report = read(folder / 'report.json')
    require(report.get('driver_sha256') == sha256(Path(__file__).with_name('run.py')), 'auditor revision differs from frozen driver')
    cases = read(folder / 'cases.json')
    require(report['case_names'] == [case['name'] for case in cases], 'case manifest mismatch')
    expected_cases = {case['name']: case for case in cases}
    accepted, deliveries, duplicates = 0, 0, 0
    options_by_case, environments = {}, []
    for trial in report['trials']:
        require(trial['returncode'] == 0, 'failed trial cannot be verified as accepted')
        name, pair, side = trial['case'], trial['pair'], trial['side']
        require(name in expected_cases and side in ('baseline', 'candidate'), 'unexpected trial')
        target = folder / f'{name}-{pair}-{side}'
        require(target.resolve().is_relative_to(folder.resolve()), 'unsafe trial path')
        options = read(target / 'options.json')
        require(not options.get('diagnostics') and not options.get('profile_seconds'), 'profiled timing trial')
        normalized = {k: v for k, v in options.items() if k != 'token'}
        for key, value in expected_cases[name].items():
            if key != 'name':
                require(normalized.get(key) == value, f'changed frozen option: {name}/{key}')
        require(normalized == options_by_case.setdefault(name, normalized), 'different workload options across pairs')
        environment = read(target / 'environment.json')
        binary = binaries[0 if side == 'baseline' else 1]
        require(environment['binary_sha256'] == hashes[binary], f'wrong binary: {campaign}/{name}/{side}')
        environments.append({key: environment[key] for key in ('effective_cpus', 'cpu_max', 'gomaxprocs', 'platform')})
        recomputed = audit(target)
        require(recomputed == read(target / 'summary.json') == trial['summary'], 'audit or report summary mismatch')
        accepted += 1
        deliveries += recomputed['count']
        duplicates += recomputed['duplicates']
    require(all(value == environments[0] for value in environments), 'CPU/platform settings changed within campaign')
    require(analyze(report) == report['assessment'], 'paired assessment mismatch')
    for case in expected_cases:
        for side in ('baseline', 'candidate'):
            summaries = [trial['summary'] for trial in report['trials'] if trial['case'] == case and trial['side'] == side]
            medians = {key: statistics.median(s[key] for s in summaries) for key in ('lifecycle_records_s', 'seed_sync_p99_ms')}
            for phase in summaries[0]['phases']:
                medians[phase] = {key: statistics.median(s['phases'][phase][key] for s in summaries)
                                  for key in ('seconds', 'cpu_seconds', 'allocated_bytes', 'peak_rss_mib')}
            require(medians == report['medians'][case][side], 'stored medians mismatch')
    return {'trials': accepted, 'source_deliveries': deliveries, 'duplicates': duplicates,
            'cases': len(cases), 'pairs': report['expected_pairs']}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--campaign', action='append', help='relative-folder=baseline-binary,candidate-binary')
    args = p.parse_args()
    root = args.root.resolve()
    hashes = manifest(root)
    specifications = args.campaign or [
        'sync-paired=worker-baseline,worker-candidate',
        'scan-guardrails=worker-baseline,worker-candidate',
        'aa=worker-candidate,worker-candidate']
    results = {}
    for spec in specifications:
        name, raw = spec.split('=', 1)
        binaries = raw.split(',')
        require(len(binaries) == 2 and name not in results, 'invalid/duplicate campaign')
        results[name] = verify_campaign(root, name, binaries, hashes)
    print(json.dumps({'manifest_files': len(hashes), 'campaigns': results,
                      'total_trials': sum(r['trials'] for r in results.values()),
                      'total_source_deliveries': sum(r['source_deliveries'] for r in results.values()),
                      'source': (root / 'source.txt').read_text().splitlines(),
                      'verified': True}, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()

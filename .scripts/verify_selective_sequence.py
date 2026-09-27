#!/usr/bin/env python3
"""Require behavioral detection of the missing-successor-guard regression."""
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile


def run(source, output, pattern):
    with output.open('w') as log:
        completed = subprocess.run(
            ['go', 'test', '-mod=readonly', '-count=1', '-json', '-run', pattern, '.'],
            cwd=source, stdout=log, stderr=subprocess.STDOUT, timeout=120,
        )
    rows = []
    for line in output.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return completed.returncode, rows


def public_leaves(rows, action):
    return {row.get('Test', '') for row in rows
            if row.get('Action') == action
            and row.get('Test', '').startswith('TestRegressionSelectiveReplaySequenceCompatibility/')
            and row.get('Test', '').endswith(('/missing-data', '/missing-id'))}


def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    out = pathlib.Path(sys.argv[1]).resolve()
    out.mkdir(parents=True, exist_ok=True)
    pattern = '^TestRegressionSelectiveReplaySequenceCompatibility$'
    code, rows = run(root, out/'sequence-correct.jsonl', pattern)
    expected = public_leaves(rows, 'pass')
    assert code == 0 and len(expected) == 8, 'correct-source public controls failed or absent'
    assert not any(row.get('Action') in ('fail', 'skip') for row in rows), 'nonpassing control'
    code, rows = run(root, out/'sequence-independent.jsonl', '^TestSelectiveSequencesPreserveFallbackState$')
    assert code == 0 and any(row.get('Action') == 'pass' and row.get('Test') ==
                             'TestSelectiveSequencesPreserveFallbackState' for row in rows), 'missing differential control'
    with tempfile.TemporaryDirectory(prefix='journal-sequence-control-') as temp:
        target = pathlib.Path(temp)/'source'
        shutil.copytree(root, target, ignore=shutil.ignore_patterns('.git', '__pycache__'))
        path = target/'selective.go'
        text = path.read_text()
        needle = ' && hasIndependentSuccessor(b[size:])'
        assert text.count(needle) == 1, 'mutation target changed'
        path.write_text(text.replace(needle, '', 1))
        code, rows = run(target, out/'sequence-unsafe.jsonl', pattern)
    assert code == 1 and public_leaves(rows, 'fail') == expected, 'unsafe implementation escaped named assertions'
    output = ''.join(row.get('Output', '') for row in rows)
    assert 'selective replay changed sequence semantics:' in output, 'missing payload assertion'
    assert 'missing ID changed ACK suppression:' in output, 'missing ID assertion'
    assert not any(row.get('Action') == 'skip' for row in rows), 'skips are not detection'
    (out/'sequence-controls.json').write_text(json.dumps({
        'correct_public_cases': 8, 'unsafe_assertion_failures': 8,
        'independent_sequence_control': 'pass', 'passed': True,
    }, indent=2)+'\n')
    print('Sequence controls passed: 8 correct, 8 intended failures, independent differential control passed')


if __name__ == '__main__':
    main()

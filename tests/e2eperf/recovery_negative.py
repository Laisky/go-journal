#!/usr/bin/env python3
"""Real recovery mutants must fail their intended behavioral assertion.

Use Go's file overlay in a private directory: never mutate checked-out code and
never accept compilation errors, timeouts or missing tests as a negative control.
"""
import argparse
import json
from pathlib import Path
import tempfile

from compare import run_supervisor
from supervised_exec import install_signal_handlers


CASES = (
    ('ack-invalid-delta', 'ack_words.go', '\t\tif id < 0 {', '\t\tif false {',
     'TestACKBufferedMaximumRetainsInvalidSuffix', 'invalid ACK delta accepted'),
    ('directory-skip-validation', 'fs.go', 'if _, err := os.Stat(absFname); err != nil {', 'if false {',
     'TestDirectorySnapshotStillRejectsDanglingEntriesBeforeCreatingFiles', 'dangling directory entry accepted'),
)


def run_case(root, out, name, test, assertion, source=None):
    with tempfile.TemporaryDirectory(prefix='journal-recovery-mutant-') as temp:
        command = ['go', 'test', '-mod=readonly', '-count=1', '-json', '-run', '^'+test+'$']
        if source is not None:
            path, text = source
            replacement = Path(temp)/path.name
            replacement.write_text(text)
            overlay = Path(temp)/'overlay.json'
            overlay.write_text(json.dumps({'Replace': {str(path): str(replacement)}}))
            command.append('-overlay='+str(overlay))
        command.append(str(root))
        log = out/(name+'.jsonl')
        with log.open('x') as output:
            code = run_supervisor(command, output, 120)
        raw = log.read_text()
        events = []
        for line in raw.splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        expected = 'fail' if source else 'pass'
        if code != (1 if source else 0) or not any(e.get('Test') == test and e.get('Action') == expected for e in events):
            raise ValueError(name+': compilation, timeout or missing test cannot satisfy this control')
        if source and assertion not in raw:
            raise ValueError(name+': mutant did not fail the intended assertion')
        return {'variant': name, 'test': test, 'returncode': code, 'expected_assertion': expected}


def main():
    install_signal_handlers()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[2]
    outcomes = []
    for name, filename, old, new, test, assertion in CASES:
        path = root/filename
        text = path.read_text()
        if text.count(old) != 1:
            raise ValueError(name+': mutation anchor changed')
        outcomes.append(run_case(root, a.out, name+'-positive', test, assertion))
        outcomes.append(run_case(root, a.out, name, test, assertion, (path, text.replace(old, new, 1))))
    print(json.dumps(outcomes, indent=2))


if __name__ == '__main__':
    main()

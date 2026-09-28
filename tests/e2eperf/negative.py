#!/usr/bin/env python3
"""Actual executable mutants must fail the independent lifecycle assertion."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    here = Path(__file__).resolve().parent
    source = (here / 'main.go').read_text()
    variants = {
        'omit-append': ('err := j.WriteData(&journal.Data{ID: id, Data: map[string]interface{}{"id": id, "body": body}})',
                        'var err error; if id != 50 { err = j.WriteData(&journal.Data{ID: id, Data: map[string]interface{}{"id": id, "body": body}}) }',
                        'transfer missing/extra records'),
        'omit-ack': ('err = j.WriteId(id)', 'err = nil // mutant: omit ACK', 'transfer missing/extra records'),
        'omit-transfer': ('err = j.WriteData(d)', 'err = nil // mutant: omit transfer',
                          'frontier 49, want 64'),
    }
    outcomes = []
    for name, (old, new, expected) in variants.items():
        if source.count(old) != 1:
            raise ValueError(f'{name}: ambiguous mutation anchor')
        with tempfile.TemporaryDirectory(prefix='journal-worker-') as temp:
            path = Path(temp) / 'main.go'
            path.write_text(source.replace(old, new, 1))
            support = []
            for extra in here.glob('*.go'):
                if extra.name != 'main.go' and not extra.name.endswith('_test.go'):
                    copy = Path(temp) / extra.name
                    copy.write_bytes(extra.read_bytes())
                    support.append(str(copy))
            binary = Path(temp) / 'worker'
            subprocess.run(['go', 'build', '-mod=readonly', '-o', str(binary), str(path), *support], check=True)
            target = root / name
            cmd = [sys.executable, str(here / 'run.py'), '--binary', str(binary), '--out', str(target),
                   '--count', '64', '--payload', '64', '--writers', '4', '--scans', '1']
            with open(root / (name + '.log'), 'x') as log:
                result = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, timeout=120)
            text = (root / (name + '.log')).read_text()
            if name == 'omit-transfer':
                text += (target / 'deliver/worker.log').read_text()
            if result.returncode != 1 or expected not in text:
                raise ValueError(f'{name}: did not fail its intended assertion; inspect {name}.log')
            outcomes.append({'variant': name, 'returncode': result.returncode, 'assertion': expected})
    (root / 'negative.json').write_text(json.dumps(outcomes, indent=2) + '\n')
    print(json.dumps(outcomes))


if __name__ == '__main__':
    main()

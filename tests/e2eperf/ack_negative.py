#!/usr/bin/env python3
"""Fail a real decoder-base reset mutant; unrelated errors cannot pass the gate."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    repo = Path(__file__).resolve().parents[2]
    original = (repo/'scan_ids.go').read_text()
    old = '\tdec.baseID = -1\n'
    if original.count(old) != 1:
        raise ValueError('base-reset mutation anchor changed')
    outcomes = []
    for name, source in (('positive', original), ('keep-base', original.replace(old, '', 1))):
        with tempfile.TemporaryDirectory(prefix='journal-ack-control-') as temp:
            files = []
            for path in sorted(repo.glob('*.go')):
                if not path.name.endswith('_test.go') or path.name == 'scan_ids_test.go':
                    target = Path(temp)/path.name
                    target.write_text(source if path.name == 'scan_ids.go' else path.read_text())
                    files.append(str(target))
            log = a.out/(name+'.jsonl')
            with log.open('x') as output:
                result = subprocess.run(['go', 'test', '-mod=readonly', '-count=1', '-json', '-run',
                                         '^TestACKScanReuseResetsBaseUnreadBytesAndErrors$', *files],
                                        stdout=output, stderr=subprocess.STDOUT, timeout=120)
            events = []
            for line in log.read_text().splitlines():
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            action = 'pass' if name == 'positive' else 'fail'
            if result.returncode != (0 if name == 'positive' else 1) or not any(
                    e.get('Test') == 'TestACKScanReuseResetsBaseUnreadBytesAndErrors' and e.get('Action') == action
                    for e in events):
                raise ValueError(f'{name}: compilation/timeout/unexpected outcome is not an assertion')
            if name != 'positive' and 'ACK reader retained file state' not in log.read_text():
                raise ValueError('mutant did not fail the intended base-reset assertion')
            outcomes.append({'variant': name, 'returncode': result.returncode, 'assertion': action})
    print(json.dumps(outcomes))


if __name__ == '__main__':
    main()

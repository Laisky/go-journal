#!/usr/bin/env python3
"""A no-op Rotate with a forged success counter must fail the workload test."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    here = Path(__file__).resolve().parent
    source = (here/'main.go').read_text()
    old = 'err = j.Rotate(context.Background())'
    if source.count(old) != 1:
        raise ValueError('rotation mutation anchor changed')
    outcomes = []
    for name, text in (('positive', source), ('omit-rotation', source.replace(old, 'err = nil // mutant: no physical rotation', 1))):
        with tempfile.TemporaryDirectory(prefix='journal-segment-control-') as temp:
            folder = Path(temp)
            files = []
            for file in sorted(here.glob('*.go')):
                if not file.name.endswith('_test.go') or file.name == 'segments_test.go':
                    target = folder/file.name
                    target.write_text(text if file.name == 'main.go' else file.read_text())
                    files.append(str(target))
            with (args.out/(name+'.jsonl')).open('x') as log:
                result = subprocess.run(['go', 'test', '-mod=readonly', '-count=1', '-json', '-run', '^TestSeedRotationProducesDistinctSegments$', *files], stdout=log, stderr=subprocess.STDOUT, timeout=120)
            lines = (args.out/(name+'.jsonl')).read_text().splitlines()
            events = []
            for line in lines:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            expected = 'pass' if name == 'positive' else 'fail'
            leaves = [e for e in events if e.get('Action') == expected and e.get('Test', '').startswith('TestSeedRotationProducesDistinctSegments/')]
            if len(leaves) != 2 or result.returncode != (0 if name == 'positive' else 1):
                raise ValueError(f'{name}: not the intended two behavior outcomes')
            if name != 'positive' and 'actual nonempty segment count: got 1, want 5' not in '\n'.join(lines):
                raise ValueError('mutant failed for a different reason')
            outcomes.append({'variant': name, 'returncode': result.returncode, 'leaf_outcomes': len(leaves)})
    print(json.dumps(outcomes))


if __name__ == '__main__':
    main()

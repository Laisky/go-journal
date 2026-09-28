#!/usr/bin/env python3
"""Prove that bypassing private staging fails the public rejection assertion."""
import argparse
import json
from pathlib import Path
import tempfile

from compare import run_supervisor
from supervised_exec import install_signal_handlers


def main():
    install_signal_handlers()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=False)
    repo = Path(__file__).resolve().parents[2]
    original = (repo/'serialize.go').read_text()
    anchor = 'msgp.Encode(&enc.record, msg)'
    if original.count(anchor) != 1:
        raise ValueError('transactional staging mutation anchor changed')
    results = []
    for name, source in (('positive', original), ('bypass-stage', original.replace(anchor, 'msgp.Encode(enc.writer, msg)', 1))):
        with tempfile.TemporaryDirectory(prefix='journal-staging-control-') as temp:
            files = []
            for path in sorted(repo.glob('*.go')):
                if not path.name.endswith('_test.go') or path.name in ('record_stage_test.go', 'writer_buffer_test.go'):
                    target = Path(temp)/path.name
                    target.write_text(source if path.name == 'serialize.go' else path.read_text())
                    files.append(str(target))
            log = a.out/(name+'.jsonl')
            with log.open('x') as output:
                code = run_supervisor(['go','test','-mod=readonly','-count=1','-json','-run',
                                       '^TestRecordStageLargeCustomRejectionThenRetry$', *files], output, 120)
            events = []
            for line in log.read_text().splitlines():
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
            expected = 'pass' if name == 'positive' else 'fail'
            if code != (0 if name == 'positive' else 1) or not any(
                    e.get('Test') == 'TestRecordStageLargeCustomRejectionThenRetry' and e.get('Action') == expected
                    for e in events):
                raise ValueError(f'{name}: compiler errors, missing tests and timeouts cannot pass')
            if name != 'positive' and 'rejection reached live writer' not in log.read_text():
                raise ValueError('mutant did not fail the intended live-file assertion')
            results.append({'variant': name, 'returncode': code, 'assertion': expected})
    print(json.dumps(results))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Reproduce the one-file Sync experiment or prove coordination negative controls.

--apply is for an isolated historical worktree, never automatic production adoption.
Mutants must compile, run the named test, and fail its intended assertion.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def apply(source):
    path = source / 'journal.go'
    text = path.read_text()
    field = '\tlastRotateAt  time.Time\n'
    entry = 'func (j *Journal) Sync() error {\n\tj.Lock()\n\tdefer j.Unlock()\n'
    if not (source / 'sync_group.go').is_file() or text.count(field) != 1 or text.count(entry) != 1:
        raise ValueError('integration anchor changed; do not silently reinterpret the experiment')
    text = text.replace(field, field + '\tsyncGroup     syncBarrierGroup\n', 1)
    text = text.replace(entry, '''func (j *Journal) Sync() error {
	select {
	case <-j.stopChan:
		return os.ErrClosed
	default:
	}
	return j.syncGroup.run(&j.RWMutex, j.syncBarrierLocked)
}

// syncBarrierLocked runs with exclusive writer ownership. Overlapping Sync
// callers can share this result only until ownership is released.
func (j *Journal) syncBarrierLocked() error {
''', 1)
    path.write_text(text)


def negative(source, out):
    out.mkdir(parents=True, exist_ok=False)
    original = (source / 'sync_group.go').read_text()
    tests = (source / 'sync_group_test.go').read_text()
    completion = 'g.complete(err)' if 'g.complete(err)' in original else 'g.complete(f, err)'
    reset = '\tg.active = nil\n'
    if '\tg.running = false\n' in original:
        reset += '\tg.running = false\n'
    variants = {
        'publish-after-unlock': (
            '\t\t' + completion + '\n\t\tlock.Unlock()\n',
            '\t\tlock.Unlock()\n\t\t' + completion + '\n',
            'TestSyncGroupPublishesBeforeWriterCanProceed',
            'old barrier remains joinable after releasing writer exclusion'),
        'hide-barrier-error': (
            '\tf.err = err\n', '\tf.err = nil\n',
            'TestSyncGroupSharesOnlyAnOverlappingBarrier', 'follower lost barrier error'),
        'cache-completed-barrier': (
            reset, '\t// mutant: keep completed barrier\n',
            'TestSyncGroupSharesOnlyAnOverlappingBarrier', 'sequential Sync reused a cached barrier'),
    }
    outcomes = []
    for name, spec in [('correct', None), *variants.items()]:
        text, pattern, expected = original, 'TestSyncGroup', ''
        if spec is not None:
            old, new, pattern, expected = spec
            if original.count(old) != 1:
                raise ValueError(f'{name}: ambiguous mutation anchor')
            text = original.replace(old, new, 1)
        with tempfile.TemporaryDirectory(prefix='journal-sync-mutant-') as temp:
            root = Path(temp)
            implementation, testfile = root / 'sync_group.go', root / 'sync_group_test.go'
            implementation.write_text(text)
            testfile.write_text(tests)
            command = ['go', 'test', '-count=1', '-json', '-run', '^' + pattern,
                       str(implementation), str(testfile)]
            result = subprocess.run(command, capture_output=True, text=True, timeout=60)
            (out / (name + '.jsonl')).write_text(result.stdout)
            (out / (name + '.stderr')).write_text(result.stderr)
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            ran = any(e.get('Action') == 'run' and e.get('Test', '').startswith(pattern) for e in events)
            failed = any(e.get('Action') == 'fail' and e.get('Test', '').startswith(pattern) for e in events)
            if not ran or (spec is None and (result.returncode != 0 or failed)) or (spec is not None and (result.returncode != 1 or not failed or expected not in result.stdout)):
                raise ValueError(f'{name}: did not satisfy its intended assertion; inspect retained output')
            outcomes.append({'variant': name, 'returncode': result.returncode,
                             'test': pattern, 'expected_assertion': expected})
    (out / 'summary.json').write_text(json.dumps(outcomes, indent=2) + '\n')
    print(json.dumps(outcomes))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    modes = p.add_mutually_exclusive_group(required=True)
    modes.add_argument('--apply', action='store_true')
    modes.add_argument('--negative', type=Path)
    a = p.parse_args()
    if a.apply:
        apply(a.source.resolve())
    else:
        negative(a.source.resolve(), a.negative.resolve())


if __name__ == '__main__':
    main()

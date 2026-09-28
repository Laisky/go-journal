import copy
import json
from pathlib import Path
import tempfile
import unittest

from monitor import METHOD
from run import audit
from test_oracle import fixture


class MonitorAuditTest(unittest.TestCase):
    def make_trial(self, root, method=True, scan=False):
        options, results, processes, rows, errors = fixture()
        options.update(scans=int(scan), writers=1)
        if method:
            options['measurement_method'] = METHOD
        if scan:
            phases = copy.deepcopy(results['verify']['phases'])
            phases[-1].update(name='scan', ops=1, latency_ns=[1], End=3)
            results['scan'] = {'mode': 'scan', 'count': 4, 'high': 4, 'records': [], 'phases': phases}
            processes['scan'] = {'returncode': 0, 'held': False, 'duration_ns': 100}
        (root/'options.json').write_text(json.dumps(options))
        for stage, result in results.items():
            folder = root/stage
            folder.mkdir()
            proc = processes[stage]
            held = stage in ('seed', 'transfer')
            if method:
                proc['observer'] = {'method': METHOD, 'backend': 'pidfd', 'completed': True,
                    'started_ns': 1000, 'exit_observed_ns': 1090, 'first_output_ns': 1005 if held else None,
                    'checkpoint_ns': 1010 if held else None, 'kill_sent_ns': 1020 if held else None,
                    'killed_at_checkpoint': held, 'fallback_reason': None, 'sample_interval_ns': 20_000_000,
                    'samples': 0, 'stdout_bytes': 11 if held else 0, 'stdout_eof': True}
            (folder/'process.json').write_text(json.dumps(proc))
            (folder/'result.json').write_text(json.dumps(result))
        (root/'downstream.jsonl').write_text('\n'.join(json.dumps(row) for row in rows))
        (root/'peer-errors.json').write_text(json.dumps(errors))

    def test_decomposition_and_legacy_compatibility(self):
        for method in (True, False):
            for scan in (True, False):
                with tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    self.make_trial(root, method, scan)
                    summary = audit(root)
                    if method:
                        self.assertAlmostEqual(summary['worker_phase_seconds']+summary['outside_phase_seconds'],
                                               summary['lifecycle_seconds'])
                        self.assertEqual(summary['observer_backends'], ['pidfd'])
                    else:
                        self.assertNotIn('measurement_method', summary)
                        self.assertNotIn('outside_phase_seconds', summary)

    def test_all_process_metadata_including_scanner_is_checked(self):
        mutations = [lambda p: p.pop('observer'),
            lambda p: p.update(returncode=False), lambda p: p.update(held=0),
            lambda p: p.update(duration_ns=True), lambda p: p.update(duration_ns=-1),
            lambda p: p['observer'].update(method='poll-v1'),
            lambda p: p['observer'].update(backend='invented'),
            lambda p: p['observer'].update(backend='pipe-poll'),
            lambda p: p['observer'].update(completed=False),
            lambda p: p['observer'].pop('completed'),
            lambda p: p['observer'].update(exit_observed_ns=999),
            lambda p: p['observer'].update(exit_observed_ns=1200),
            lambda p: p['observer'].update(sample_interval_ns=10_000_000),
            lambda p: p['observer'].update(samples=True),
            lambda p: p['observer'].update(stdout_bytes=-1),
            lambda p: p['observer'].update(stdout_eof=1),
            lambda p: p['observer'].update(first_output_ns=1200)]
        for stage in ('seed', 'scan', 'deliver'):
            for index, mutate in enumerate(mutations):
                with self.subTest(stage=stage, mutation=index), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    self.make_trial(root, scan=True)
                    path = root/stage/'process.json'
                    proc = json.loads(path.read_text())
                    mutate(proc)
                    path.write_text(json.dumps(proc))
                    with self.assertRaises(ValueError):
                        audit(root)

    def test_checkpoint_order_and_no_kill_for_nonheld_process(self):
        for stage in ('seed', 'transfer', 'scan', 'deliver', 'verify'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.make_trial(root, scan=True)
                path = root/stage/'process.json'
                proc = json.loads(path.read_text())
                proc['observer']['kill_sent_ns'] = 1001
                path.write_text(json.dumps(proc))
                with self.assertRaises(ValueError):
                    audit(root)

    def test_event_metadata_cannot_silently_downgrade_to_legacy(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.make_trial(root, scan=True)
            path = root/'options.json'
            options = json.loads(path.read_text())
            options.pop('measurement_method')
            path.write_text(json.dumps(options))
            with self.assertRaisesRegex(ValueError, 'downgrade'):
                audit(root)

    def test_backend_must_match_across_scanner_and_other_stages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.make_trial(root, scan=True)
            path = root/'scan/process.json'
            proc = json.loads(path.read_text())
            proc['observer'].update(backend='pipe-poll', fallback_reason='unavailable')
            path.write_text(json.dumps(proc))
            with self.assertRaisesRegex(ValueError, 'mixed observer backends'):
                audit(root)

    def test_individual_worker_duration_cannot_be_hidden_in_other_stages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.make_trial(root)
            path = root/'seed/result.json'
            result = json.loads(path.read_text())
            result['phases'][0]['End'] = 150
            path.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, 'exceeds worker observation'):
                audit(root)

    def test_scan_time_is_not_counted_as_delivered_lifecycle(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.make_trial(root, scan=True)
            before = audit(root)
            path = root/'scan/process.json'
            proc = json.loads(path.read_text())
            proc['duration_ns'] *= 100
            path.write_text(json.dumps(proc))
            after = audit(root)
            for key in ('lifecycle_seconds', 'worker_phase_seconds', 'outside_phase_seconds'):
                self.assertEqual(before[key], after[key])

    def test_missing_or_false_checkpoint_cannot_pass(self):
        for update in ({'killed_at_checkpoint': False}, {'checkpoint_ns': None}):
            with self.subTest(update=update), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.make_trial(root)
                path = root/'seed/process.json'
                proc = json.loads(path.read_text())
                proc['observer'].update(update)
                path.write_text(json.dumps(proc))
                with self.assertRaises(ValueError):
                    audit(root)

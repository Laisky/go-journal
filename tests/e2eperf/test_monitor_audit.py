import copy
import json
from pathlib import Path
import tempfile
import unittest

from monitor import METHOD
from run import audit
from test_oracle import fixture


class MonitorAuditTest(unittest.TestCase):
    def make_trial(self, root, method=True, scans=False):
        options, results, processes, rows, errors = fixture()
        options['scans'] = 1 if scans else 0
        options['writers'] = 1
        if scans:
            results['scan'] = copy.deepcopy(results['verify'])
            results['scan']['mode'] = 'scan'
            results['scan']['phases'][-1].update(name='scan', ops=1, latency_ns=[1])
            processes['scan'] = dict(processes['verify'])
        if method:
            options['measurement_method'] = METHOD
        (root/'options.json').write_text(json.dumps(options))
        for stage in results:
            folder=root/stage; folder.mkdir()
            proc=processes[stage]
            proc['observer']={'method':METHOD,'backend':'pidfd', 'started_ns':1000,
                'exit_observed_ns':1090, 'checkpoint_ns':1010, 'kill_sent_ns':1020,
                'killed_at_checkpoint':stage in ('seed','transfer')}
            (folder/'process.json').write_text(json.dumps(proc))
            (folder/'result.json').write_text(json.dumps(results[stage]))
        (root/'downstream.jsonl').write_text('\n'.join(json.dumps(row) for row in rows))
        (root/'peer-errors.json').write_text(json.dumps(errors))

    def test_decomposition_and_legacy_compatibility(self):
        for method in (True, False):
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp); self.make_trial(root,method)
                summary=audit(root)
                if method:
                    self.assertAlmostEqual(summary['worker_phase_seconds']+summary['outside_phase_seconds'],summary['lifecycle_seconds'])
                    self.assertEqual(summary['observer_backends'], ['pidfd'])
                else:
                    self.assertNotIn('measurement_method',summary)
                    self.assertNotIn('outside_phase_seconds',summary)

    def test_invalid_event_evidence_rejected(self):
        mutations=[lambda p:p.pop('observer'),
                   lambda p:p['observer'].update(method='poll-v1'),
                   lambda p:p['observer'].update(backend='invented'),
                   lambda p:p['observer'].update(exit_observed_ns=999),
                   lambda p:p['observer'].update(exit_observed_ns=1200),
                   lambda p:p['observer'].update(killed_at_checkpoint=False),
                   lambda p:p['observer'].update(checkpoint_ns=None),
                   lambda p:p['observer'].update(kill_sent_ns=1001)]
        for mutate in mutations:
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp); self.make_trial(root)
                path=root/'seed/process.json'; proc=json.loads(path.read_text())
                mutate(proc); path.write_text(json.dumps(proc))
                with self.assertRaises(ValueError): audit(root)

    def test_individual_worker_duration_cannot_be_hidden_in_other_stages(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); self.make_trial(root)
            path=root/'seed/result.json'; result=json.loads(path.read_text())
            result['phases'][0]['End']=150
            path.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError,'exceeds worker observation'):
                audit(root)

    def test_scan_process_metadata_is_audited(self):
        mutations = [lambda p:p.pop('observer'),
                     lambda p:p['observer'].update(method='poll-v1'),
                     lambda p:p['observer'].update(backend='invented'),
                     lambda p:p['observer'].update(exit_observed_ns=999),
                     lambda p:p.update(held=True),
                     lambda p:p.update(returncode=False),
                     lambda p:p.update(duration_ns=True)]
        for mutate in mutations:
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp); self.make_trial(root, scans=True)
                self.assertTrue(audit(root)['passed'])
                path=root/'scan/process.json'; proc=json.loads(path.read_text())
                mutate(proc); path.write_text(json.dumps(proc))
                with self.assertRaises(ValueError): audit(root)

    def test_mixed_backends_within_one_trial_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); self.make_trial(root, scans=True)
            path=root/'scan/process.json'; proc=json.loads(path.read_text())
            proc['observer'].update(backend='pipe-poll')
            path.write_text(json.dumps(proc))
            with self.assertRaisesRegex(ValueError, 'mixed observer backends within trial'):
                audit(root)

    def test_scan_time_is_not_counted_as_delivered_lifecycle(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); self.make_trial(root, scans=True)
            a=audit(root)
            path=root/'scan/process.json'; proc=json.loads(path.read_text())
            proc['duration_ns'] *= 100
            path.write_text(json.dumps(proc))
            b=audit(root)
            self.assertEqual(a['lifecycle_seconds'], b['lifecycle_seconds'])
            self.assertEqual(a['worker_phase_seconds'], b['worker_phase_seconds'])

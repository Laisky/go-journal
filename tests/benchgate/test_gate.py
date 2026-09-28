import copy
import itertools
import json
from pathlib import Path
import tempfile
import unittest

from gate import SIDES, assessment, digest, load, save, validate_policy, verify

POLICY = load(Path(__file__).with_name('policy.json'))


def campaign():
    p = copy.deepcopy(POLICY)
    trials = []
    orders = list(itertools.permutations(SIDES))
    for i in range(p['rounds']):
        for side in orders[i % len(orders)]:
            trials.append({'round': i, 'side': side, 'returncode': 0,
                'file': f'{i}-{side}.json', 'log': f'{i}-{side}.log',
                'result': {'schema': 1, 'go': 'go1.27.1', 'gomaxprocs': 4,
                           'results': {name: {'operations': p['iterations'], 'cpu_ns/op': 100000,
                               'wall_ns/op': 150000, 'B/op': 0, 'allocs/op': 0} for name in p['cases']}}})
    return {'schema': 1, 'policy': p, 'trials': trials, 'binary_sha256': {s: 'a'*64 for s in SIDES}}


def mutate_candidate(report, name, metric, value):
    for t in report['trials']:
        if t['side'] == 'candidate':
            t['result']['results'][name][metric] = value


class GateTests(unittest.TestCase):
    def test_identical_work_passes(self):
        self.assertTrue(assessment(campaign())['passed'])

    def test_absolute_allocation_failure(self):
        r = campaign()
        mutate_candidate(r, 'staging-1m', 'B/op', 1 << 20)
        self.assertIn('absolute-allocation', {v['kind'] for v in assessment(r)['issues']})

    def test_relative_slack_does_not_allow_gradual_memory_regression(self):
        r = campaign()
        mutate_candidate(r, 'staging-1m', 'B/op', 2048)
        self.assertEqual({v['kind'] for v in assessment(r)['issues']}, {'relative-allocation'})

    def test_allocation_count_is_independently_gated(self):
        r = campaign()
        mutate_candidate(r, 'ttl-hits', 'allocs/op', 9)
        self.assertFalse(assessment(r)['passed'])

    def test_cpu_regression_fails(self):
        r = campaign()
        mutate_candidate(r, 'ttl-hits', 'cpu_ns/op', 150000)
        self.assertIn('cpu-regression', {v['kind'] for v in assessment(r)['issues']})

    def test_uncertain_cpu_is_not_a_pass(self):
        r = campaign()
        for t in r['trials']:
            if t['side'] == 'candidate' and t['round'] % 2:
                t['result']['results']['ttl-hits']['cpu_ns/op'] = 170000
        self.assertIn('inconclusive-cpu', {v['kind'] for v in assessment(r)['issues']})

    def test_unstable_same_binary_control_fails(self):
        r = campaign()
        for t in r['trials']:
            if t['side'] == 'control':
                t['result']['results']['ttl-hits']['cpu_ns/op'] = 140000
        self.assertIn('unstable-control', {v['kind'] for v in assessment(r)['issues']})

    def test_wall_clock_is_diagnostic_not_a_slo_claim(self):
        r = campaign()
        mutate_candidate(r, 'ttl-hits', 'wall_ns/op', 1000000000)
        self.assertTrue(assessment(r)['passed'])

    def test_malformed_and_incomplete_evidence_fails(self):
        changes = [lambda r: r['trials'].pop(),
            lambda r: r['trials'].append(r['trials'][0]),
            lambda r: r['trials'][0].update(returncode=1),
            lambda r: r['trials'][0].update(side='candidate'),
            lambda r: r['binary_sha256'].update(control='b'*64),
            lambda r: r['trials'][0]['result'].update(go='go1.28.0'),
            lambda r: r['trials'][0]['result'].update(gomaxprocs=1),
            lambda r: r['trials'][0]['result']['results'].pop('ttl-hits'),
            lambda r: r['trials'][0]['result']['results']['ttl-hits'].update(operations=1),
            lambda r: r['trials'][0]['result']['results']['ttl-hits'].update(operations=True),
            lambda r: r['trials'][0]['result']['results']['ttl-hits'].update(**{'B/op': float('nan')}),
            lambda r: r['trials'][0]['result']['results']['ttl-hits'].update(**{'cpu_ns/op': 0})]
        for change in changes:
            with self.subTest(change=change):
                r = campaign(); change(r)
                with self.assertRaises((ValueError, KeyError)):
                    assessment(r)

    def test_policy_bounds(self):
        for rounds in (1, 8, 21, True):
            p = copy.deepcopy(POLICY); p['rounds'] = rounds
            with self.assertRaises(ValueError): validate_policy(p)

    def test_duplicate_json_keys_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'bad.json'; path.write_text('{"passed":false,"passed":true}')
            with self.assertRaises(ValueError): load(path)

    def test_raw_samples_and_recomputed_assessment_are_verified(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); r = campaign()
            save(root/'policy.json', r['policy']); r['policy_sha256'] = digest(root/'policy.json')
            for t in r['trials']:
                save(root/t['file'], t['result']); t['sha256'] = digest(root/t['file'])
                (root/t['log']).write_text('test fixture\n'); t['log_sha256'] = digest(root/t['log'])
            r['assessment'] = assessment(r); save(root/'report.json', r)
            self.assertTrue(verify(root)['passed'])
            (root/r['trials'][0]['file']).write_text('{}')
            with self.assertRaises(ValueError): verify(root)


if __name__ == '__main__':
    unittest.main()

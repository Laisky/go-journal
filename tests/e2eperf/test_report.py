import copy
import unittest
from report import analyze, paired_effect


def campaign():
    summary = {'passed': True, 'diagnostic_only': False, 'count': 32,
               'lifecycle_seconds': 2., 'seed_sync_p99_ms': 1.,
               'phases': {'scan/scan': {'seconds': 1., 'cpu_seconds': .2,
                         'allocated_bytes': 1024, 'peak_rss_mib': 16, 'ops': 8}}}
    return {'expected_pairs': 5, 'case_names': ['plain'], 'trials': [
        {'case': 'plain', 'pair': pair, 'side': side, 'returncode': 0, 'summary': copy.deepcopy(summary)}
        for pair in range(5) for side in ('baseline', 'candidate')]}


class ReportTest(unittest.TestCase):
    def test_effect_and_noise(self):
        self.assertEqual(paired_effect([1]*5, [.7]*5)['status'], 'improved')
        self.assertEqual(paired_effect([1]*5, [1.3]*5)['status'], 'regressed')
        self.assertEqual(paired_effect([1]*5, [.5, 2, .6, 1.9, 1])['status'], 'inconclusive')
        self.assertEqual(paired_effect([1]*3, [.1]*3)['status'], 'insufficient-pairs')
        self.assertEqual(paired_effect([0]*5, [1]*5)['status'], 'zero-baseline')
        self.assertEqual(analyze(campaign())['plain']['lifecycle_seconds']['status'], 'inconclusive')

    def test_mixed_observation_methods_cannot_claim_library_improvement(self):
        report = campaign()
        report['trials'][0]['summary'].update(measurement_method='events-v1', observer_backends=['pidfd'])
        with self.assertRaisesRegex(ValueError, 'different observation methods'):
            analyze(report)

    def test_mixed_backends_are_not_comparable(self):
        for backends in (['pidfd'], ['pidfd', 'pipe-poll']):
            report = campaign()
            report['trials'][0]['summary']['observer_backends'] = backends
            with self.assertRaisesRegex(ValueError, 'mixed observation backends'):
                analyze(report)

    def test_invalid_numbers(self):
        for value in (-1, float('nan'), float('inf'), True):
            with self.assertRaises(ValueError):
                paired_effect([value], [1])

    def test_invalid_campaigns(self):
        mutations = [
            lambda r: r['trials'].pop(),
            lambda r: r['trials'].append(copy.deepcopy(r['trials'][0])),
            lambda r: r['trials'][0].update(returncode=1),
            lambda r: r['trials'][0]['summary'].update(diagnostic_only=True),
            lambda r: r['trials'][0]['summary'].pop('diagnostic_only'),
            lambda r: r['trials'][0]['summary'].update(passed=False),
            lambda r: r['trials'][0]['summary'].update(count=31),
            lambda r: r['trials'][0]['summary']['phases']['scan/scan'].update(ops=7),
            lambda r: r['case_names'].append('missing'),
        ]
        for mutate in mutations:
            value = campaign()
            mutate(value)
            with self.assertRaises(ValueError):
                analyze(value)

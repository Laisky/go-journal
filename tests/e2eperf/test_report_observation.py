import unittest

from report import analyze
from test_report import campaign


class ObservationComparisonTest(unittest.TestCase):
    def test_mixed_methods_cannot_claim_library_improvement(self):
        report = campaign()
        report['trials'][0]['summary'].update(measurement_method='events-v2', observer_backends=['pidfd'])
        with self.assertRaisesRegex(ValueError, 'different observation methods'):
            analyze(report)

    def test_mixed_backends_are_not_comparable(self):
        for backends in (['pidfd'], ['pidfd', 'pipe-poll']):
            report = campaign()
            report['trials'][0]['summary']['observer_backends'] = backends
            with self.assertRaisesRegex(ValueError, 'mixed observation backends'):
                analyze(report)

    def test_missing_or_unknown_method_metadata_is_rejected(self):
        for method, backend in (('future', ['pidfd']), ('events-v2', None), ('events-v2', ['invented'])):
            report = campaign()
            for trial in report['trials']:
                trial['summary'].update(measurement_method=method, observer_backends=backend)
            with self.assertRaises(ValueError):
                analyze(report)

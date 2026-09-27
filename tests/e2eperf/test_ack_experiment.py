import unittest
from pathlib import Path
from ack_experiment import integrate


class ACKExperimentTest(unittest.TestCase):
    def test_exact_integration_or_adopted_source(self):
        source = Path(__file__).resolve().parents[2].joinpath('legacy.go').read_text()
        if 'var ackBuffers scanIDBuffers' in source:
            self.assertEqual(source.count('var ackBuffers scanIDBuffers'), 3)
            with self.assertRaises((ValueError, AssertionError)):
                integrate(source)
            return
        changed = integrate(source)
        self.assertEqual(changed.count('var ackBuffers scanIDBuffers'), 3)
        self.assertEqual(changed.count('}, &ackBuffers); err != nil'), 3)
        self.assertIn('readIDsFileWithBuffers(name, consume, nil)', changed)
        # A second application or a shifted source must fail, never silently skip.
        with self.assertRaises((ValueError, AssertionError)):
            integrate(changed)
        with self.assertRaises((ValueError, AssertionError)):
            integrate(source.replace('readIDsFile(name, ', 'changed(name, ', 1))

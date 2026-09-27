import copy
import unittest
from test_oracle import fixture, perf


class SegmentOracleTest(unittest.TestCase):
    def test_exact_rotation_work(self):
        data = fixture()
        data[0]['rotate_every'] = 2
        data[1]['seed']['rotations'] = 2
        self.assertEqual(perf.audit_data(*data), 0)
        for bad in (0, 1, 3, True, -1):
            changed = copy.deepcopy(data)
            changed[1]['seed']['rotations'] = bad
            with self.assertRaisesRegex(ValueError, 'rotation workload'):
                perf.audit_data(*changed)
        changed = copy.deepcopy(data)
        changed[1]['transfer']['rotations'] = 1
        with self.assertRaisesRegex(ValueError, 'rotation workload'):
            perf.audit_data(*changed)

    def test_invalid_rotation_parameters(self):
        for bad in (-1, 5, True, 1.0):
            data = fixture()
            data[0]['rotate_every'] = bad
            with self.assertRaisesRegex(ValueError, 'rotation workload'):
                perf.audit_data(*data)

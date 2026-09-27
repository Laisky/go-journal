import unittest
from stress import cases, integers


class StressTest(unittest.TestCase):
    def test_matrix(self):
        result = cases(32, [0, 1024], [1, 16], [0, 50, 100], ['plain', 'gzip'], 0)
        self.assertEqual(len(result), 24)
        self.assertEqual(len({c['name'] for c in result}), 24)
        self.assertTrue(all(c['scans'] == 0 for c in result))

    def test_bounds(self):
        for text in ('', '1,1', '0', '129'):
            with self.assertRaises(ValueError):
                integers(text, 1, 128)
        for args in ((0, [1], [1], [50], ['plain'], 1),
                     (1000000, [4096], [1], [50], ['plain'], 1),
                     (1, [1], [1], [50], ['unknown'], 1),
                     (1, [1], [1], [50], ['plain'], -1),
                     (1, [1, 2, 3], [1, 2, 3], [0, 25, 50], ['plain', 'gzip'], 1)):
            with self.assertRaises(ValueError):
                cases(*args)

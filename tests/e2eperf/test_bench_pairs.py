import unittest
from bench_pairs import parse, assess


class PublicBenchmarkEvidenceTest(unittest.TestCase):
    def valid(self):
        return '\n'.join(f'BenchmarkPublicDirectorySnapshot/{n}-4 64 100 ns/op 30 B/op 3 allocs/op' for n in (16,256,4096))+'\nPASS\n'

    def test_complete_fixed_work(self):
        self.assertEqual(len(parse(self.valid(),'directory',64)),3)

    def test_invalid_evidence(self):
        raw=self.valid()
        for bad in (raw.replace('PASS','FAIL'),raw.replace('64 100','63 100'),
                    raw.replace('100 ns/op','nan ns/op'),raw+'BenchmarkPublicDirectorySnapshot/16-4 64 100 ns/op 30 B/op 3 allocs/op\n',
                    raw.replace('BenchmarkPublicDirectorySnapshot/16-4','WrongName')):
            with self.assertRaises(ValueError):parse(bad,'directory',64)
        with self.assertRaises(ValueError):assess([],'directory',5)

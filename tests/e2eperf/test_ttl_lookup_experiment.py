import unittest
from ttl_lookup_experiment import candidate
from bench_pairs import parse

class TTLLookupExperimentTest(unittest.TestCase):
    def test_clock_scope_and_double_apply(self):
        source='before\n\tvar (\n\t\tt  = time.Now().UnixNano()\n\t\tvi interface{}\n\t)\nif hit {return true}\n\tif s.og != nil {after'
        changed=candidate(source)
        self.assertEqual(changed.count('time.Now()'),1)
        self.assertLess(changed.index('if hit'),changed.index('time.Now()'))
        self.assertTrue(changed.endswith('after'))
        with self.assertRaises(ValueError):candidate(changed)

    def test_benchmark_work_identity(self):
        raw='\n'.join('BenchmarkPublicACKMembership/'+n+'-4 64 100 ns/op 0 B/op 0 allocs/op' for n in ('current-hits','no-old-misses','parallel-hits'))+'\nPASS\n'
        self.assertEqual(len(parse(raw,'membership',64)),3)
        with self.assertRaises(ValueError):parse(raw.replace('64 100','63 100'),'membership',64)

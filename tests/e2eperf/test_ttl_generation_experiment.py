import unittest
from ttl_generation_experiment import REPLACEMENTS, transform
from bench_pairs import parse

class GenerationExperimentTest(unittest.TestCase):
    def test_only_declared_changes_and_no_double_application(self):
        original='\n'.join(old for old,new in REPLACEMENTS)+'\nunchanged-body'
        changed=transform(original)
        self.assertEqual(changed,'\n'.join(new for old,new in REPLACEMENTS)+'\nunchanged-body')
        with self.assertRaises(ValueError): transform(changed)
        with self.assertRaises(ValueError): transform(original+REPLACEMENTS[0][0])

    def test_load_work_is_fixed(self):
        names=['refresh-serial','refresh-parallel8','refresh-parallel32','hot-key32']
        raw='\n'.join('BenchmarkPublicTTLGeneration/'+name+'-4 64 100 ns/op 0 B/op 0 allocs/op' for name in names)+'\nPASS\n'
        self.assertEqual(len(parse(raw,'generation',64)),4)
        with self.assertRaises(ValueError): parse(raw.replace('64 100','63 100'),'generation',64)

import unittest
from ack_writer_experiment import candidate, missing_flush
from bench_pairs import parse


class ACKWriterExperimentTest(unittest.TestCase):
    def test_scope_and_reapply(self):
        source = ('before\nfunc NewIdsEncoder(\nutils.WithCompressBufSizeByte(BufSize),\n'
                  'bufio.NewWriterSize(enc.gzWriter, BufSize)\nbufio.NewWriterSize(fp, BufSize)\n'
                  '// NewIdsDecoder\nafter')
        changed = candidate(source)
        self.assertEqual(changed.count('len(enc.word)'), 2)
        self.assertIn('utils.WithCompressBufSizeByte(BufSize)', changed)
        self.assertTrue(changed.startswith('before\n'))
        self.assertTrue(changed.endswith('// NewIdsDecoder\nafter'))
        with self.assertRaises(ValueError):
            candidate(changed)

    def test_scalar_flush_mutation_does_not_change_data_encoder(self):
        block = '\tif err = enc.writer.Flush(); err != nil {\n\t\treturn errors.Wrap(err, "flush journal record")\n\t}\n'
        source = block+'func (enc *IdsEncoder) Write('+block+'// Flush flush buf to fp'+block
        self.assertEqual(missing_flush(source).count(block), 2)
        with self.assertRaises(ValueError):
            missing_flush(missing_flush(source))

    def test_benchmark_exact_work(self):
        raw = '\n'.join('BenchmarkACKWriterResources/'+n+'-4 64 100 ns/op 10 B/op 2 allocs/op'
                        for n in ('construct-plain', 'construct-gzip', 'construct-pair', 'write-plain'))+'\nPASS\n'
        self.assertEqual(len(parse(raw, 'ack-writer', 64)), 4)
        with self.assertRaises(ValueError):
            parse(raw.replace('64 100', '63 100'), 'ack-writer', 64)

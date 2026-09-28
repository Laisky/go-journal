from pathlib import Path
import unittest

from staging_experiment import transform


class StagingTransformTest(unittest.TestCase):
    def test_refuses_ambiguous_or_already_adopted_source(self):
        for source in ('', 'msgp.Encode(&enc.record, msg)'):
            with self.assertRaises(ValueError):
                transform(source)

    def test_current_source_or_adopted_source_is_explicit(self):
        source = (Path(__file__).resolve().parents[2]/'serialize.go').read_text()
        if 'record   bytes.Buffer' in source:
            changed = transform(source)
            self.assertIn('record   recordStage', changed)
            self.assertEqual(changed.count('msgp.NewWriterSize('), 2)
            self.assertEqual(changed.count('bufio.NewWriterSize('), 2)
            self.assertIn('bufio.NewWriterSize(fp, BufSize)', changed)
            self.assertIn('utils.WithCompressBufSizeByte(BufSize)', changed)
            self.assertIn('msgp.Encode(&enc.record, msg)', changed)
            with self.assertRaises(ValueError):
                transform(changed)
        else:
            self.assertIn('record   recordStage', source)
            with self.assertRaises(ValueError):
                transform(source)

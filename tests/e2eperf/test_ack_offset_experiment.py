from pathlib import Path
import unittest
from ack_offset_experiment import OLD, NEW, transform


class ACKOffsetExperimentTest(unittest.TestCase):
    def test_only_one_exact_body_changes(self):
        self.assertEqual(transform('before\n'+OLD+'\nafter'), 'before\n'+NEW+'\nafter')
        for text in ('', NEW, OLD+OLD):
            with self.assertRaises(ValueError):
                transform(text)

    def test_real_source_or_adopted_body(self):
        text = (Path(__file__).resolve().parents[2]/'serialize.go').read_text()
        if NEW in text:
            self.assertNotIn(OLD, text)
        else:
            self.assertEqual(transform(text).replace(NEW, OLD, 1), text)

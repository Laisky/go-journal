import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from observe import observe, read_counter
from qualify import qualify
from report import analyze
from test_report import campaign
from writer_experiment import resize


class WriterCampaignTest(unittest.TestCase):
    def test_only_live_writer_sizes_change(self):
        text = '\n'.join([
            'enc.writer = msgp.NewWriterSize(enc.gzWriter, BufSize)',
            'enc.writer = msgp.NewWriterSize(fp, BufSize)',
            'enc.writer = bufio.NewWriterSize(enc.gzWriter, BufSize)',
            'enc.writer = bufio.NewWriterSize(fp, BufSize)',
            'decoder.reader = msgp.NewReaderSize(fp, BufSize)',
            'utils.WithCompressBufSizeByte(BufSize)',
        ])
        for size in (256, 4096, 65536, 4194304):
            result = resize(text, size)
            self.assertEqual(result.count(f', {size})'), 4)
            self.assertIn('decoder.reader = msgp.NewReaderSize(fp, BufSize)', result)
            self.assertIn('utils.WithCompressBufSizeByte(BufSize)', result)
        for invalid in (True, 0, 18, 8192):
            with self.assertRaises(ValueError):
                resize(text, invalid)
        with self.assertRaises(ValueError):
            resize(text + '\n' + text, 4096)
        with self.assertRaises(ValueError):
            resize('missing constructors', 4096)

    def test_qualification_requires_two_stable_controls(self):
        r = campaign()
        r['assessment'] = analyze(r)
        self.assertTrue(qualify({'before': r, 'after': r})['timing_qualified'])
        noisy = copy.deepcopy(r)
        noisy['trials'][0]['summary']['lifecycle_seconds'] *= 4
        noisy['assessment'] = analyze(noisy)
        self.assertFalse(qualify({'before': r, 'after': noisy})['timing_qualified'])
        with self.assertRaises(ValueError):
            qualify({'before': r})
        changed = copy.deepcopy(r)
        changed['assessment']['plain']['lifecycle_seconds']['median_ratio'] = .5
        with self.assertRaises(ValueError):
            qualify({'before': r, 'after': changed})

    def test_observer_retains_nonzero_and_timeout_status(self):
        with tempfile.TemporaryDirectory() as tmp, patch('observe.snapshot', return_value={'monotonic_ns': 1}):
            root = Path(tmp)
            self.assertEqual(observe([sys.executable, '-c', 'raise SystemExit(7)'], root/'error', 10, .05), 7)
            self.assertEqual(json.loads((root/'error/exit.json').read_text())['returncode'], 7)
            self.assertEqual(observe([sys.executable, '-c', 'import time; time.sleep(60)'], root/'timeout', .1, .05), 124)
            self.assertGreaterEqual(len((root/'timeout/host.jsonl').read_text().splitlines()), 2)
            with self.assertRaises(FileExistsError):
                observe([sys.executable, '-c', 'pass'], root/'error', 10, .05)
        self.assertIn('error', read_counter('/nonexistent/journal-e2eperf-counter'))
        self.assertNotIn('text', read_counter('/nonexistent/journal-e2eperf-counter'))

    def test_observer_error_is_not_a_zero_load_sample(self):
        with tempfile.TemporaryDirectory() as tmp, patch('observe.snapshot', side_effect=[RuntimeError('probe failed'), {'monotonic_ns': 2}]):
            with self.assertRaisesRegex(RuntimeError, 'observer failed'):
                observe([sys.executable, '-c', 'pass'], Path(tmp)/'probe', 10, .05)
            result = json.loads((Path(tmp)/'probe/exit.json').read_text())
            self.assertEqual(result['returncode'], 0)
            self.assertTrue(result['observer_errors'])

"""Controls for historical fixture normalization; no performance budgets changed."""
from pathlib import Path
import tempfile
import unittest
from prepare_harness import V1, V2, adapt, copy_harness


class TestHistoricalHarness(unittest.TestCase):
    def fixture(self):
        return 'package main\nimport journal "' + V2 + '"\n// timing, logger and assertion stay exact\n'

    def test_native_copy_is_identical(self):
        self.assertEqual(adapt(self.fixture(), V2), (self.fixture(), True))

    def test_legacy_change_is_only_import(self):
        text, changed = adapt(self.fixture(), V1)
        self.assertTrue(changed)
        self.assertIn('"' + V1 + '"', text)
        self.assertEqual(text.replace('"' + V1 + '"', '"' + V2 + '"'), self.fixture())

    def test_no_import_file_is_identical(self):
        self.assertEqual(adapt('package main\n', V1), ('package main\n', False))

    def test_unknown_duplicate_and_legacy_imports_rejected(self):
        for text in (self.fixture() + 'import "' + V2 + '"', self.fixture().replace(V2, V1), self.fixture().replace(V2, V1 + '/v3')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                adapt(text, V1)
        with self.assertRaises(ValueError):
            adapt(self.fixture(), 'unknown')

    def test_complete_copy_and_no_partial_write_on_bad_fixture(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name); source = root/'source'; target = root/'target'
            folder = source/'tests/benchgate'; folder.mkdir(parents=True)
            (folder/'main.go').write_text(self.fixture())
            (folder/'other.go').write_text('package main\n')
            row = copy_harness(source, target, 'benchgate', V1)
            self.assertEqual(row['journal_imports'], 1)
            self.assertEqual((target/'tests/benchgate/main.go').read_text(), self.fixture().replace(V2, V1))
            (folder/'main.go').write_text(self.fixture().replace(V2, V1))
            before = (target/'tests/benchgate/main.go').read_text()
            with self.assertRaises(ValueError):
                copy_harness(source, target, 'benchgate', V1)
            self.assertEqual((target/'tests/benchgate/main.go').read_text(), before)

    def test_missing_empty_and_same_checkout_rejected(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name)
            for kind in ('benchgate','e2eperf','unknown'):
                with self.subTest(kind=kind), self.assertRaises(ValueError):
                    copy_harness(root/'source', root/'target', kind, V1)
            with self.assertRaises(ValueError):
                copy_harness(root, root, 'benchgate', V1)


if __name__ == '__main__':
    unittest.main()

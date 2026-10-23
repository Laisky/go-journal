"""Negative controls for the gate's fail-closed event accounting."""
import unittest
import sys
import tempfile
from pathlib import Path
from fast_ci import module_root, run, validate


class TestReceipts(unittest.TestCase):
    def events(self):
        return [{"Package": "p", "Test": "TestRequired", "Action": action} for action in ("run", "pass")] + [{"Package": "p", "Action": "pass"}]

    def test_success(self):
        validate(self.events(), ["TestRequired"], 0)

    def test_empty_or_missing_tests(self):
        for events, expected in (([], ["TestRequired"]), (self.events(), []), (self.events(), ["TestMissing"])):
            with self.assertRaises(RuntimeError):
                validate(events, expected, 0)

    def test_failed_process_even_with_pass_events(self):
        with self.assertRaises(RuntimeError):
            validate(self.events(), ["TestRequired"], 1)

    def test_skipped_or_failed_subtest(self):
        for action in ("skip", "fail", "build-fail"):
            with self.assertRaises(RuntimeError):
                validate(self.events() + [{"Package": "p", "Test": "TestRequired/child", "Action": action}], ["TestRequired"], 0)

    def test_missing_package_completion(self):
        with self.assertRaises(RuntimeError):
            validate(self.events()[:-1], ["TestRequired"], 0)

    def test_duplicate_pass(self):
        with self.assertRaises(RuntimeError):
            validate(self.events() + self.events()[:2], ["TestRequired"], 0)

    def test_native_failure_is_retained(self):
        records = []
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            with self.assertRaises(RuntimeError):
                run([sys.executable, "-c", "import sys; sys.exit(7)"], evidence, "failure", records)
            self.assertEqual(records[0]["exit_code"], 7)
            self.assertTrue((evidence / "failure.stderr").is_file())

    def test_timeout_does_not_report_success(self):
        records = []
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError):
                run([sys.executable, "-c", "import time; time.sleep(10)"], Path(directory), "timeout", records, timeout=0.1)
            self.assertIsNone(records[0]["exit_code"])
            self.assertIn("timeout", records[0]["error"])


class TestModuleRoots(unittest.TestCase):
    def test_nested_module_is_explicit(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / "go.mod").write_text("module main\n")
            nested = root / "tests" / "consumer"
            nested.mkdir(parents=True)
            (nested / "go.mod").write_text("module consumer\n")
            self.assertEqual(module_root(root, {}), root.resolve())
            self.assertEqual(module_root(root, {"module_dir": "tests/consumer"}), nested.resolve())

    def test_missing_and_escaping_modules_fail(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            for value in ("missing", "../outside", str(root.resolve()), ""):
                with self.subTest(value=value), self.assertRaises(RuntimeError):
                    module_root(root, {"module_dir": value})

    def test_command_uses_recorded_module_directory(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            records = []
            output = run([sys.executable, "-c", "import os; print(os.getcwd())"], root, "cwd", records, cwd=root)
            self.assertEqual(Path(output.strip()), root.resolve())
            self.assertEqual(records[0]["cwd"], str(root))


if __name__ == "__main__":
    unittest.main()

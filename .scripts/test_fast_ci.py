"""Negative controls for the gate's fail-closed event accounting."""
import unittest
import re
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



def literal_branch_event(workflow, event, branch, *, ref_type="branch"):
    """Model this gate's literal branch filters, not general Actions YAML."""
    block = re.search(r"(?m)^  " + re.escape(event) + r":\n((?:^    [^\n]*\n)+)", workflow)
    if block is None:
        raise ValueError("missing event branch filter")
    match = re.search(r"(?m)^    branches: \[([^\]\n]+)\]$", block.group(1))
    if match is None:
        raise ValueError("expected explicit literal branch list")
    branches = [value.strip().strip("'\"") for value in match.group(1).split(",")]
    if not branches or any(not re.fullmatch(r"[A-Za-z0-9_./-]+", value) for value in branches):
        raise ValueError("unsupported or empty branch pattern")
    return ref_type == "branch" and branch in branches


class TestWorkflowBranches(unittest.TestCase):
    def test_existing_workflow_covers_the_module_major_line(self):
        root = Path(__file__).resolve().parents[1]
        workflow = (root / ".github/workflows/go.yml").read_text()
        module = (root / "go.mod").read_text().splitlines()[0]
        major = re.search(r"/v([2-9][0-9]*)$", module)
        self.assertIsNotNone(major, "this gate belongs to the explicit major-version candidate")
        for event in ("push", "pull_request"):
            for branch in ("master", "v" + major.group(1)):
                with self.subTest(event=event, target_branch=branch):
                    self.assertTrue(literal_branch_event(workflow, event, branch))

    def test_legacy_and_unrelated_ref_controls(self):
        legacy = "on:\n  push:\n    branches: [master]\n  pull_request:\n    branches: [master]\n"
        candidate = legacy.replace("[master]", "[master, v2]")
        for event in ("push", "pull_request"):
            self.assertTrue(literal_branch_event(legacy, event, "master"))
            self.assertFalse(literal_branch_event(legacy, event, "v2"))
            self.assertTrue(literal_branch_event(candidate, event, "v2"))
            for branch in ("feature/v2", "v20", "v2/topic"):
                with self.subTest(event=event, unrelated_branch=branch):
                    self.assertFalse(literal_branch_event(candidate, event, branch))
        self.assertFalse(literal_branch_event(candidate, "push", "v2.0.0", ref_type="tag"))
        self.assertFalse(literal_branch_event(candidate, "push", "v2", ref_type="tag"))

    def test_missing_or_pattern_filters_fail_closed(self):
        for workflow in (
            "on:\n",
            "on:\n  push:\n    branches-ignore: [master]\n",
            "on:\n  push:\n    branches: [v*]\n",
        ):
            with self.subTest(workflow=workflow), self.assertRaises(ValueError):
                literal_branch_event(workflow, "push", "v2")


if __name__ == "__main__":
    unittest.main()

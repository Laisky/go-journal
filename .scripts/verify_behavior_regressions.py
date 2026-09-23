#!/usr/bin/env python3
"""Require public-contract failures on the baseline and passing independent controls.

No compilation error, test skip, or timeout is accepted as a reproduction.
The optional --modfile records an explicitly different consumer dependency graph.
Without it, both versions use the repository's own unmodified module graph.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

BASELINE = "979ec19dde737bb4fec9ada2e39a99267c028706"
FAILURES = [
    "TestBehaviorCanceledStartCanRetry",
    "TestBehaviorInvalidOptionsRejected",
    "TestBehaviorLifecycleMisuseReturnsError",
    "TestBehaviorAcknowledgementCorruptionFailsClosedAndRetries",
    "TestBehaviorCleanupFailureIsNotEOF",
    "TestBehaviorFailedRotationKeepsWriterUsable",
    "TestBehaviorDirectoryHasSingleOwner",
    "TestBehaviorFileNamesStayDiscoverable",
    "TestBehaviorUnrelatedFilesAreNotRecoveryInput",
    "TestBehaviorEncoderClosedOperations",
    "TestBehaviorCompressionCanChangeAcrossRestart",
    "TestBehaviorFractionalTTLDoesNotExpireEarly",
    "TestBehaviorBitmapPreservesUint32Boundary",
    "TestBehaviorBitmapDecodeRejectsUnrepresentableIDs",
    "TestBehaviorMalformedIDStreamIsRejected",
    "TestBehaviorReplayAfterCloseReturnsError",
    "TestBehaviorE2EProcessOwnership",
    "TestBehaviorE2ERecoveryWithDescriptorBudget",
]
CONTROLS = ["TestBehaviorRestartContract", "TestBehaviorE2EEmptyCrashSegments",
            "TestBehaviorDeliveryOracleRejectsFalseSuccess"]


def run(command, cwd, timeout=120):
    return subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout,
                          env={**os.environ, "GOWORK": "off"})


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--before-ref", default=BASELINE)
    ap.add_argument("--before-dir", type=Path,
                    help="Use an independently verified baseline snapshot instead of git worktree")
    ap.add_argument("--modfile", type=Path)
    ap.add_argument("--artifacts", type=Path, required=True)
    args = ap.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.artifacts.resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = {"baseline_ref": args.before_ref, "cases": [], "success": False,
              "dependency_graph": "standalone" if args.modfile is None else "external-modfile",
              "toolchain": run(["go", "version"], root).stdout.strip()}
    if args.modfile:
        report["modfile_sha256"] = hashlib.sha256(args.modfile.read_bytes()).hexdigest()
        shutil.copy2(args.modfile, output / "validation.mod")
    report["test_source_sha256"] = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.glob("behavior*_test.go"))}
    added_worktree = False
    try:
        with tempfile.TemporaryDirectory(prefix="journal-red-green-") as temp:
            temp = Path(temp)
            before = temp / "before"
            if args.before_dir:
                shutil.copytree(args.before_dir.resolve(), before,
                                ignore=shutil.ignore_patterns(".git", "*.test", "coverage*"))
            else:
                cp = run(["git", "worktree", "add", "--detach", str(before), args.before_ref], root)
                (output / "worktree.log").write_text(cp.stdout)
                if cp.returncode:
                    raise RuntimeError("cannot prepare exact baseline; see worktree.log")
                added_worktree = True
            try:
                for source in root.glob("behavior*_test.go"):
                    shutil.copy2(source, before / source.name)
                binaries = {}
                for label, source in [("before", before), ("after", root)]:
                    binary = temp / (label + ".test")
                    command = ["go", "test", "-c", "-mod=readonly", "-o", str(binary)]
                    if args.modfile:
                        command.append("-modfile=" + str(args.modfile.resolve()))
                    command.append(".")
                    cp = run(command, source, timeout=300)
                    (output / (label + "-build.log")).write_text(cp.stdout)
                    if cp.returncode:
                        raise RuntimeError(label + " compilation failed, not a reproduced defect")
                    report[label + "_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
                    binaries[label] = binary
                # A current failure is not allowed to count as a successful red control.
                for label in ["after", "before"]:
                    for name in CONTROLS + FAILURES:
                        cp = run([str(binaries[label]), "-test.v", "-test.run=^" + name + "$",
                                  "-test.timeout=25s"], root, timeout=30)
                        text = cp.stdout
                        (output / (label + "-" + name + ".log")).write_text(text)
                        expected_failure = label == "before" and name in FAILURES
                        fixture_problem = "--- SKIP:" in text or "test timed out" in text
                        passed = cp.returncode == 0 and "--- PASS: " + name + " " in text
                        assertion = "--- FAIL: " + name + " " in text
                        # The old rotation publishes nil encoders; the already-started
                        # flush worker can panic while the public failure test unwinds.
                        known_rotation_panic = (name == "TestBehaviorFailedRotationKeepsWriterUsable"
                            and "panic:" in text and "(*IdsEncoder).Flush" in text)
                        reproduced = cp.returncode != 0 and (assertion or known_rotation_panic)
                        ok = not fixture_problem and (reproduced if expected_failure else passed)
                        report["cases"].append({"version": label, "test": name,
                            "expected_failure": expected_failure, "exit_code": cp.returncode,
                            "observed_pass": passed, "observed_failure": reproduced, "verified": ok})
                        print(label, name, "verified" if ok else "UNEXPECTED", flush=True)
                        if not ok:
                            raise RuntimeError("unexpected result for " + label + " " + name)
                report["success"] = True
            finally:
                if added_worktree:
                    run(["git", "worktree", "remove", "--force", str(before)], root)
    finally:
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"Verified {len(FAILURES)} named baseline failures and {len(CONTROLS)} controls; "
          "current implementation passes all cases.")

if __name__ == "__main__":
    main()

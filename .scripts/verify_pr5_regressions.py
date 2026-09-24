#!/usr/bin/env python3
"""Compare identical public tests against the master that merged PR #6.

Compilation errors, crashes, skips, and timeouts cannot count as reproductions.
Optional paired benchmarks measure cost, not a promise of higher throughput.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

BASELINE = "2ad43212f66b3c04c64992940b7924288f1729f3"
CASES = {
    "TestUserInvalidPayloadDoesNotPoisonLaterAcceptedData": True,
    "TestUserInvalidCallsReturnErrors/before-start/flush": True,
    "TestUserE2ERejectedPayloadCrashRecovery": True,
    "TestUserE2EPartialAppendDoesNotAcceptLaterRecords/false": True,
    "TestUserOpenBufFilePreservesExistingData": False,
    "TestUserCleanupFailureRemainsVisible": False,
    "TestUserCustomEncoderRemainsSupported": False,
    "TestUserAcknowledgementDamageFailsClosedAndCanRetry": False,
    "TestUserFlushConcurrentWithRotationAndClose": False,
}


def run(command, cwd, timeout=180):
    return subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout,
                          env={**os.environ, "GOWORK": "off"})


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--artifacts", required=True, type=Path)
    ap.add_argument("--before-dir", type=Path,
                    help="Verified local baseline snapshot; hosted CI uses the exact Git commit")
    ap.add_argument("--benchmark-pairs", type=int, default=0)
    args = ap.parse_args()
    if not 0 <= args.benchmark_pairs <= 20:
        ap.error("--benchmark-pairs must be between 0 and 20")
    root = Path(__file__).resolve().parents[1]
    output = args.artifacts.resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = {"baseline": BASELINE, "dependency_graph": "standalone", "success": False,
              "toolchain": run(["go", "version"], root).stdout.strip(), "cases": [], "benchmarks": []}
    (output / "environment.txt").write_text(run(["go", "env"], root).stdout)
    added = False
    try:
        with tempfile.TemporaryDirectory(prefix="journal-pr5-") as temp:
            temp = Path(temp)
            before = temp / "before"
            if args.before_dir:
                shutil.copytree(args.before_dir.resolve(), before,
                                ignore=shutil.ignore_patterns(".git", "*.test", "evidence"))
            else:
                cp = run(["git", "worktree", "add", "--detach", str(before), BASELINE], root)
                if cp.returncode:
                    raise RuntimeError("cannot prepare baseline: " + cp.stdout)
                added = True
            try:
                tests = sorted(root.glob("user*_test.go")) + [root / "behavior_process_test.go"]
                report["test_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in tests}
                for source in tests:
                    shutil.copy2(source, before / source.name)
                binaries = {}
                for label, source in [("before", before), ("after", root)]:
                    binary = temp / (label + ".test")
                    cp = run(["go", "test", "-c", "-mod=readonly", "-o", str(binary), "."], source)
                    (output / (label + "-build.txt")).write_text(cp.stdout)
                    if cp.returncode:
                        raise RuntimeError(label + " compilation failed, not a reproduced defect")
                    report[label + "_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
                    binaries[label] = binary
                for label in ["after", "before"]:
                    for name, old_fails in CASES.items():
                        pattern = "/".join("^" + part + "$" for part in name.split("/"))
                        cp = run([str(binaries[label]), "-test.v", "-test.run=" + pattern,
                                  "-test.timeout=60s"], root, timeout=70)
                        (output / (label + "-" + name.replace("/", "_") + ".txt")).write_text(cp.stdout)
                        failed = label == "before" and old_fails
                        marker = ("--- FAIL: " if failed else "--- PASS: ") + name + " "
                        bad = any(s in cp.stdout for s in ["--- SKIP:", "panic:", "test timed out", "DATA RACE"])
                        ok = marker in cp.stdout and not bad and (cp.returncode == 1 if failed else cp.returncode == 0)
                        report["cases"].append({"version": label, "test": name, "expected_failure": failed,
                                                "exit_code": cp.returncode, "verified": ok})
                        if not ok:
                            raise RuntimeError(label + " unexpected result: " + name)
                for pair in range(args.benchmark_pairs):
                    order = ["before", "after"] if pair % 2 == 0 else ["after", "before"]
                    for label in order:
                        cp = run([str(binaries[label]), "-test.run=^$", "-test.bench=^BenchmarkUserJournalAppend$",
                                  "-test.benchtime=20x", "-test.benchmem", "-test.count=1", "-test.timeout=120s"], root)
                        filename = f"bench-{pair:02d}-{label}.txt"
                        (output / filename).write_text(cp.stdout)
                        if cp.returncode or cp.stdout.count("BenchmarkUserJournalAppend/") != 8:
                            raise RuntimeError("incomplete benchmark: " + filename)
                        report["benchmarks"].append({"pair": pair, "version": label, "file": filename})
                report["success"] = True
            finally:
                if added:
                    run(["git", "worktree", "remove", "--force", str(before)], root)
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"success": True, "verified_cases": len(report["cases"]),
                      "benchmark_runs": len(report["benchmarks"])}))


if __name__ == "__main__":
    main()

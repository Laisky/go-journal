"""Copy public benchmark fixtures to a real v1/v2 checkout without repinning it."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess

V1 = "github.com/Laisky/go-journal"
V2 = V1 + "/v2"


def adapt(text, module):
    if module not in (V1, V2):
        raise ValueError("unsupported target module")
    imports = re.findall(r'"github\.com/Laisky/go-journal[^"\n]*"', text)
    if imports not in ([], ['"' + V2 + '"']):
        raise ValueError("expected zero or one native v2 journal import")
    return text.replace('"' + V2 + '"', '"' + module + '"'), bool(imports)


def copy_harness(source, target, harness, module):
    source, target = source.resolve(), target.resolve()
    if source == target or harness not in ("benchgate", "e2eperf"):
        raise ValueError("copy into a separate comparison checkout")
    destination = target / "tests" / harness
    if not destination.resolve().is_relative_to(target):
        raise ValueError("fixture destination escapes comparison checkout")
    files, imports = {}, 0
    for path in sorted((source / "tests" / harness).glob("*.go")):
        text = path.read_text()
        adapted, changed = adapt(text, module)
        if adapted.replace('"' + V1 + '"', '"' + V2 + '"') != text:
            raise ValueError("fixture changed outside the import literal")
        files[path.name] = adapted
        imports += changed
    if not files or not imports:
        raise ValueError("missing journal fixture imports")
    destination.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        (destination / name).write_text(text)
    return {"harness": harness, "files": sorted(files), "journal_imports": imports}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument("--harness", action="append", choices=("benchgate", "e2eperf"), required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    env = dict(os.environ, GOWORK="off")
    module = json.loads(subprocess.check_output(
        ["go", "list", "-mod=readonly", "-m", "-json"], cwd=args.target, env=env, text=True))
    if (module.get("Path") not in (V1, V2) or not module.get("Main")
            or module.get("Replace") is not None
            or Path(module["Dir"]).resolve() != args.target.resolve()):
        parser.error("require the actual journal v1/v2 main module without replacement")
    receipt = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.target, text=True).strip(),
               "module": module["Path"], "source_commit": subprocess.check_output(
                   ["git", "rev-parse", "HEAD"], cwd=args.source, text=True).strip(),
               "adaptation": "journal import literal only", "harnesses": []}
    for harness in args.harness:
        receipt["harnesses"].append(copy_harness(args.source, args.target, harness, module["Path"]))
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()

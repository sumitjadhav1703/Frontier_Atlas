"""Run one cumulative assessment gate from the active project interpreter."""
import argparse
import hashlib
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
BLOCKS = tuple(f"B{i}" for i in range(10))
OUTPUT = ROOT / "artifacts" / "gates"
INPUT_DIRECTORIES = ("src", "config", "migrations", "tests", "scripts", "evidence")
INPUT_PATTERNS = (
    "tools/gate.py", "pyproject.toml", "*.lock", "Pipfile", "requirements*.txt",
    "requirements*.in", "setup.cfg", "setup.py", "pytest.ini", "tox.ini", "conftest.py",
    "alembic.ini", "*compose*.yaml", "*compose*.yml", "Dockerfile*", "Makefile",
    ".python-version", ".env.example", "README.md", "architecture.pdf",
)
REVIEW_FIELDS = (
    "Requirement and inspected files:",
    "Normal-entry success case:",
    "Failure or regression case:",
    "Observed evidence and commands:",
    "Unresolved limits:",
    "Reviewer and execution mode:",
)


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(256 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def implementation_snapshot():
    """Cover repository build inputs; runtime stores, secrets and caches stay outside."""
    files = {p for pattern in INPUT_PATTERNS for p in ROOT.glob(pattern) if p.is_file()}
    for directory in INPUT_DIRECTORIES:
        base = ROOT / directory
        if not base.exists():
            continue
        for parent, folders, names in os.walk(base, onerror=raise_walk_error):
            folders[:] = [d for d in folders if not d.startswith(".") and d != "__pycache__"]
            folder = Path(parent)
            for name in names:
                if name.startswith(".") or name.endswith((".pyc", ".pyo", ".pem", ".key")):
                    continue
                if name in {"credentials.json", "service-account.json", "service_account.json"}:
                    continue
                files.add(folder / name)
    digest = hashlib.sha256()
    for path in sorted(files):
        relative = path.relative_to(ROOT).as_posix()
        digest.update(os.fsencode(relative) + b"\0")
        digest.update(bytes.fromhex(fingerprint(path)))
    return digest.hexdigest()


def raise_walk_error(error):
    raise error


def suite(block):
    return ROOT / "tests" / "gates" / f"test_{block.lower()}.py"


def review(block):
    return ROOT / "notes" / "reviews" / f"{block}.md"


def receipt(block):
    return OUTPUT / f"{block}.json"


def gate_status():
    """Receipts are local historical evidence. Link them to their predecessor."""
    previous = None
    latest_snapshot = None
    pending = None
    for block in BLOCKS:
        path = receipt(block)
        try:
            record = json.loads(path.read_text())
            report = OUTPUT / f"{block}.xml"
            current = (
                record["block"] == block
                and record["previous"] == previous
                and record["suite_sha256"] == fingerprint(suite(block))
                and record["review_sha256"] == fingerprint(review(block))
                and record["report_sha256"] == fingerprint(report)
                and record["format_version"] == 2
                and isinstance(record["inputs_sha256"], str)
            )
        except (OSError, KeyError, ValueError, TypeError):
            current = False
        if not current:
            pending = block
            break
        latest_snapshot = record["inputs_sha256"]
        previous = fingerprint(path)
    stale = latest_snapshot is not None and latest_snapshot != implementation_snapshot()
    if stale and pending is None:
        pending = BLOCKS[-1]
    return pending, stale


def validate_review(block):
    path = review(block)
    if not path.is_file():
        raise ValueError(f"Missing {path.relative_to(ROOT)}. Use notes/review-template.md.")
    lines = [line.strip() for line in path.read_text().splitlines()]
    verdicts = [line for line in lines if line.startswith("Verdict:")]
    if verdicts != ["Verdict: PASS"]:
        raise ValueError(f"{block} needs exactly one passing review verdict.")
    for field in REVIEW_FIELDS:
        if not any(line.startswith(field) and line[len(field):].strip() for line in lines):
            raise ValueError(f"{block} review needs evidence on the line starting {field}")


def verify_report(report, required):
    cases = ET.parse(report).findall(".//testcase")
    counts = {block: 0 for block in required}
    suite_files = {suite(block).relative_to(ROOT).as_posix(): block for block in required}
    for case in cases:
        if any(case.find(tag) is not None for tag in ("error", "failure", "skipped")):
            raise ValueError(f"Non-passing or skipped case: {case.get('name')}")
        block = suite_files.get(case.get("file"))
        if block is not None:
            counts[block] += 1
    missing = [block for block, count in counts.items() if count == 0]
    if missing:
        raise ValueError("No executed gate cases for " + ", ".join(missing))
    return counts, len(cases)


def run(block):
    index = BLOCKS.index(block)
    pending, _ = gate_status()
    if pending is not None and index > BLOCKS.index(pending):
        raise ValueError(f"{pending} must pass before {block}.")
    # A failed recheck must reopen this gate and invalidate later receipts.
    if receipt(block).exists():
        receipt(block).unlink()
    required = BLOCKS[:index + 1]
    for item in required:
        if not suite(item).is_file():
            raise ValueError(f"Missing {suite(item).relative_to(ROOT)}.")
        validate_review(item)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    snapshot = implementation_snapshot()
    review_hash = fingerprint(review(block))
    with tempfile.TemporaryDirectory(dir=OUTPUT) as work:
        report = Path(work) / "pytest.xml"
        command = [sys.executable, "-m", "pytest", "tests", "-q", "--strict-markers",
                   "-o", "addopts=", "-o", "junit_family=xunit1", f"--junitxml={report}"]
        for future in BLOCKS[index + 1:]:
            command.append(f"--ignore={suite(future).relative_to(ROOT)}")
        # Interactive selections such as "-k smoke" must not narrow a gate run.
        environment = os.environ.copy()
        environment.pop("PYTEST_ADDOPTS", None)
        result = subprocess.run(command, cwd=ROOT, env=environment, check=False)
        if result.returncode:
            raise ValueError(f"pytest exited {result.returncode}. Gate stays open.")
        counts, total = verify_report(report, required)
        if implementation_snapshot() != snapshot or fingerprint(review(block)) != review_hash:
            raise ValueError("Gate inputs changed during pytest. Review and run again.")
        saved_report = OUTPUT / f"{block}.xml"
        report.replace(saved_report)
        data = {
            "format_version": 2,
            "block": block,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "previous": fingerprint(receipt(BLOCKS[index - 1])) if index else None,
            "suite_sha256": fingerprint(suite(block)),
            "review_sha256": review_hash,
            "inputs_sha256": snapshot,
            "report_sha256": fingerprint(saved_report),
            "gate_cases": counts,
            "total_cases": total,
        }
        staged = Path(work) / "receipt.json"
        staged.write_text(json.dumps(data, indent=2) + "\n")
        staged.replace(receipt(block))
    print(f"{block} PASS. {total} cumulative cases. Evidence: artifacts/gates/{block}.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", *BLOCKS))
    args = parser.parse_args()
    try:
        if args.action == "status":
            pending, stale = gate_status()
            if stale:
                print("Implementation inputs changed since the latest cumulative check.")
            print(f"Next open block: {pending}" if pending else "All ten gates have receipts.")
            return 0
        run(args.action)
    except (ValueError, OSError, ET.ParseError) as error:
        print(f"OPEN: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

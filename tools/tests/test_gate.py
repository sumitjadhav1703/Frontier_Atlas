"""Gate-runner regression tests. Run with an interpreter that has pytest."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parents[2]
PASSING_CASE = "def test_example():\n    assert len({'one', 'one'}) == 1\n"
FILTERED_CASES = ("def test_smoke():\n    assert True\n"
                  "def test_required_regression():\n    assert False\n")


class GateRunnerTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "tools").mkdir()
        shutil.copy2(PROJECT / "tools/gate.py", self.root / "tools/gate.py")
        # Real packages allow distinct test modules with the same filename.
        for package in ["tests", "tests/gates", "tests/other"]:
            self.write(f"{package}/__init__.py", "")
        self.write("src/signal_ingest/policy.py", "MINIMUM_ROWS = 1000\n")

    def write(self, relative_path, contents):
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")

    def prepare_block(self, number, case=PASSING_CASE):
        self.write(f"tests/gates/test_b{number}.py", case)
        template = (PROJECT / "notes/review-template.md").read_text()
        lines = template.replace("Verdict: REOPEN", "Verdict: PASS").splitlines()
        review = "\n".join(line + " disposable test evidence" if line.endswith(":")
                           else line for line in lines)
        self.write(f"notes/reviews/B{number}.md", review)

    def gate(self, action, succeeds=True, pytest_addopts=""):
        env = {**os.environ, "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
               "PYTHONDONTWRITEBYTECODE": "1", "PYTEST_ADDOPTS": pytest_addopts}
        result = subprocess.run(
            [sys.executable, "tools/gate.py", action], cwd=self.root, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30,
        )
        self.assertEqual(result.returncode == 0, succeeds, result.stdout)
        return result.stdout

    def test_later_implementation_edits_allow_next_cumulative_gate(self):
        self.prepare_block(0)
        self.gate("B0")
        self.write("src/signal_ingest/policy.py", "MINIMUM_ROWS = 1000\nTIMEOUT = 5\n")
        self.prepare_block(1)
        self.gate("B1")
        self.assertIn("B2", self.gate("status"))

    def test_next_cumulative_run_detects_earlier_regression(self):
        self.prepare_block(0, "from src.signal_ingest.policy import MINIMUM_ROWS\n"
                              "def test_target_is_preserved():\n    assert MINIMUM_ROWS == 1000\n")
        self.gate("B0")
        self.write("src/signal_ingest/policy.py", "MINIMUM_ROWS = 0\n")
        self.prepare_block(1)
        self.assertIn("test_target_is_preserved", self.gate("B1", succeeds=False))
        self.assertFalse((self.root / "artifacts/gates/B1.json").exists())

    def test_terminal_status_reopens_after_a_production_edit(self):
        for number in range(10):
            self.prepare_block(number)
            self.gate(f"B{number}")
        closed_status = self.gate("status")
        self.write("src/signal_ingest/policy.py", "MINIMUM_ROWS = 0\n")
        stale_status = self.gate("status")
        self.assertNotEqual(closed_status, stale_status)
        self.assertIn("B9", stale_status)

    def test_skip_cannot_issue_a_receipt(self):
        self.prepare_block(0, "import pytest\ndef test_example():\n    pytest.skip('offline')\n")
        self.assertIn("1 skipped", self.gate("B0", succeeds=False))
        self.assertFalse((self.root / "artifacts/gates/B0.json").exists())

    def test_other_module_with_gate_filename_cannot_supply_gate_cases(self):
        self.prepare_block(0, "# This required suite has no tests.\n")
        self.write("tests/other/test_b0.py", PASSING_CASE)
        self.gate("B0", succeeds=False)
        self.assertFalse((self.root / "artifacts/gates/B0.json").exists())

    def test_environment_filter_cannot_hide_a_failing_case(self):
        self.prepare_block(0, FILTERED_CASES)
        result = self.gate("B0", succeeds=False, pytest_addopts="-k smoke")
        self.assertIn("test_required_regression", result)
        self.assertFalse((self.root / "artifacts/gates/B0.json").exists())

    def test_project_filter_cannot_hide_a_failing_case(self):
        self.prepare_block(0, FILTERED_CASES)
        self.write("pytest.ini", "[pytest]\naddopts = -k smoke\n")
        result = self.gate("B0", succeeds=False)
        self.assertIn("test_required_regression", result)
        self.assertFalse((self.root / "artifacts/gates/B0.json").exists())


if __name__ == "__main__":
    unittest.main()

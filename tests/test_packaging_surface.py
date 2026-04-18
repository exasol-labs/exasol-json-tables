#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def _pythonpath_env() -> dict[str, str]:
    env = os.environ.copy()
    python_root = str(ROOT / "python")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = python_root if not existing else python_root + os.pathsep + existing
    return env


def test_pyproject_defines_console_script() -> None:
    content = (ROOT / "pyproject.toml").read_text()
    assert '[project.scripts]' in content
    assert 'exasol-json-tables = "exasol_json_tables.cli:main"' in content


def test_module_entrypoint_help() -> None:
    result = subprocess.run(
        ["python3", "-m", "exasol_json_tables", "--help"],
        cwd=ROOT,
        env=_pythonpath_env(),
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Unified Exasol JSON Tables workflow CLI" in result.stdout
    assert "ingest-and-wrap" in result.stdout
    assert "structured-results" in result.stdout


def test_repo_wrapper_help() -> None:
    result = subprocess.run(
        ["python3", "tools/exasol_json_tables.py", "--help"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Unified Exasol JSON Tables workflow CLI" in result.stdout
    assert "wrap" in result.stdout


if __name__ == "__main__":
    test_pyproject_defines_console_script()
    test_module_entrypoint_help()
    test_repo_wrapper_help()
    print("-- packaging surface regression --")
    print("verified installable console-script metadata, module entrypoint, and compatibility wrapper")

#!/usr/bin/env python3

from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# Keep this lane intentionally focused on parser-sensitive and rewrite-sensitive behavior.
# These tests should stay sequential because they share Nano-backed fixture schemas.
PARSER_LANE_TESTS = [
    "tests/test_preprocessor_refactor_phase0.py",
    "tests/test_preprocessor_early_out.py",
    "tests/test_wrapper_errors.py",
    "tests/test_wrapper_to_json.py",
    "tests/test_wrapper_surface.py",
]


def main() -> None:
    for relative_path in PARSER_LANE_TESTS:
        print(f"RUN {relative_path}", flush=True)
        subprocess.run(["python3", str(ROOT / relative_path)], cwd=ROOT, check=True)
    print("-- preprocessor parser lane --")
    print("validated parser-focused rewrite behavior across baseline, errors, TO_JSON, and wrapper-surface tests")


if __name__ == "__main__":
    main()

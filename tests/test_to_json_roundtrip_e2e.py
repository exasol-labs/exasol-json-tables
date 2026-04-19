#!/usr/bin/env python3

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect


CLI = ROOT / "tools" / "exasol_json_tables.py"
FIXTURE_DIR = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures"
ROUNDTRIP_FIXTURES = [
    "sample.json",
    "nested.json",
    "arrays.json",
    "hetero_arrays.json",
    "edge_cases.json",
]


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def cleanup_package_schemas(con, package_config: dict[str, object]) -> None:
    for schema in [
        package_config["preprocessor"]["schema"],
        package_config["helperSchema"],
        package_config["wrapperSchema"],
        package_config["sourceSchema"],
    ]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def cleanup_named_workflow_schemas(con, workflow_name: str) -> None:
    token = workflow_name.upper()
    for schema in [
        f"EJT_{token}_PP",
        f"EJT_{token}_VIEW_INTERNAL",
        f"EJT_{token}_VIEW",
        f"EJT_{token}_SRC",
    ]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def load_expected_documents(path: Path) -> list[object]:
    value = json.loads(path.read_text())
    if not isinstance(value, list):
        raise AssertionError(f"Fixture {path} must contain a top-level JSON array for this roundtrip test.")
    return value


def roundtrip_fixture(fixture_name: str) -> None:
    fixture_path = FIXTURE_DIR / fixture_name
    expected_documents = load_expected_documents(fixture_path)

    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_roundtrip_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / fixture_path.name
        shutil.copyfile(fixture_path, input_path)

        workflow_name = f"roundtrip_{fixture_path.stem}"
        run_artifact_dir = artifact_root / workflow_name
        package_config_path = run_artifact_dir / f"{workflow_name}_wrapper_package.json"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "ingest-and-wrap",
                    "--input",
                    str(input_path),
                    "--name",
                    workflow_name,
                    "--artifact-dir",
                    str(artifact_root),
                    "--exasol-temp-dir",
                    str(staging_dir),
                    "--exasol-cleanup",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

            package_config = json.loads(package_config_path.read_text())
            root_view_name = fixture_path.stem

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{package_config["preprocessor"]["schema"]}"."{package_config["preprocessor"]["script"]}"'
                )
                rows = con.execute(
                    f'''
                    SELECT TO_JSON(*)
                    FROM "{package_config["wrapperSchema"]}"."{root_view_name}"
                    ORDER BY "_id"
                    '''
                ).fetchall()
            finally:
                try:
                    con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
                except Exception:
                    pass
                con.close()

            actual_documents = [json.loads(row[0]) for row in rows]
            assert_equal(actual_documents, expected_documents, f"{fixture_name} TO_JSON(*) roundtrip")
        finally:
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_to_json_roundtrip_complex_fixture_suite() -> None:
    for fixture_name in ROUNDTRIP_FIXTURES:
        roundtrip_fixture(fixture_name)


def main() -> None:
    test_to_json_roundtrip_complex_fixture_suite()
    print("-- TO_JSON end-to-end roundtrip regression --")
    print("verified TO_JSON(*) roundtrips for:", ", ".join(ROUNDTRIP_FIXTURES))


if __name__ == "__main__":
    main()

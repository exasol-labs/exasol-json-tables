#!/usr/bin/env python3

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect


SOURCE_SCHEMA = "JVS_INGEST_MANIFEST_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_INGEST_MANIFEST"
HELPER_SCHEMA = "JSON_VIEW_INGEST_MANIFEST_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_INGEST_MANIFEST_PP"
PREPROCESSOR_SCRIPT = "JSON_INGEST_MANIFEST_PREPROCESSOR"
PACKAGE_NAME = "ingest_manifest_wrapper"


def cleanup_schemas(con) -> None:
    for schema in [PREPROCESSOR_SCHEMA, HELPER_SCHEMA, WRAPPER_SCHEMA, SOURCE_SCHEMA]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def run_cargo_ingest(input_path: Path, manifest_path: Path, staging_dir: Path) -> None:
    subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(ROOT / "crates" / "json_tables_ingest" / "Cargo.toml"),
            "--",
            "--input",
            str(input_path),
            "--manifest-output",
            str(manifest_path),
            "--exasol",
            f"exasol://sys:exasol@127.0.0.1:8563/{SOURCE_SCHEMA}?tls=1&validateservercertificate=0",
            "--exasol-temp-dir",
            str(staging_dir),
            "--exasol-cleanup",
        ],
        cwd=ROOT,
        check=True,
    )


def run_wrapper_package_generate(manifest_path: Path, output_dir: Path) -> Path:
    package_config_path = output_dir / f"{PACKAGE_NAME}_package.json"
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "generate",
            "--source-schema",
            SOURCE_SCHEMA,
            "--wrapper-schema",
            WRAPPER_SCHEMA,
            "--helper-schema",
            HELPER_SCHEMA,
            "--preprocessor-schema",
            PREPROCESSOR_SCHEMA,
            "--preprocessor-script",
            PREPROCESSOR_SCRIPT,
            "--source-manifest",
            str(manifest_path),
            "--output-dir",
            str(output_dir),
            "--package-name",
            PACKAGE_NAME,
        ],
        cwd=ROOT,
        check=True,
    )
    return package_config_path


def run_wrapper_package_install(package_config_path: Path) -> None:
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(package_config_path),
        ],
        cwd=ROOT,
        check=True,
    )


def run_wrapper_package_validate(package_config_path: Path) -> None:
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "validate",
            "--package-config",
            str(package_config_path),
            "--check-installed",
        ],
        cwd=ROOT,
        check=True,
    )


def test_ingest_manifest_can_drive_wrapper_generation() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "nested.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_manifest_") as tmpdir:
        tmp = Path(tmpdir)
        input_path = tmp / "NESTED.json"
        manifest_path = tmp / "NESTED.source_manifest.json"
        staging_dir = tmp / "staging"
        output_dir = tmp / "package"
        shutil.copyfile(fixture_path, input_path)

        con = connect()
        try:
            cleanup_schemas(con)
            con.execute(f'CREATE SCHEMA "{SOURCE_SCHEMA}"')
        finally:
            con.close()

        try:
            run_cargo_ingest(input_path, manifest_path, staging_dir)
            manifest = json.loads(manifest_path.read_text())
            assert manifest["format"] == "exasol-json-tables-source-manifest"
            assert any(table["tableName"] == "NESTED" for table in manifest["tables"])

            package_config_path = run_wrapper_package_generate(manifest_path, output_dir)
            package_config = json.loads(package_config_path.read_text())
            assert package_config["sourceManifest"] == str(manifest_path.resolve())

            run_wrapper_package_install(package_config_path)
            run_wrapper_package_validate(package_config_path)

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{PREPROCESSOR_SCHEMA}"."{PREPROCESSOR_SCRIPT}"'
                )
                rows = con.execute(
                    f'''
                    SELECT
                      CAST("id" AS VARCHAR(20)),
                      CAST("child.a" AS VARCHAR(20)),
                      "meta.info.note"
                    FROM "{WRAPPER_SCHEMA}"."NESTED"
                    ORDER BY 1
                    '''
                ).fetchall()
            finally:
                con.close()

            assert rows == [
                ("1", "10", None),
                ("2", "20", None),
                ("3", None, "deep"),
            ]
        finally:
            con = connect()
            try:
                cleanup_schemas(con)
            finally:
                con.close()


if __name__ == "__main__":
    test_ingest_manifest_can_drive_wrapper_generation()
    print("-- ingest-manifest integration regression --")
    print("verified ingest manifest -> wrapper package generation -> install -> query")

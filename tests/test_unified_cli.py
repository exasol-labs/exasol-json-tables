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

SOURCE_SCHEMA = "JVS_CLI_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_CLI"
HELPER_SCHEMA = "JSON_VIEW_CLI_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_CLI_PP"
PREPROCESSOR_SCRIPT = "JSON_CLI_PREPROCESSOR"
PACKAGE_NAME = "cli_wrapper"
PREVIEW_SCHEMA = "JVS_CLI_STRUCT_PREVIEW"


def cleanup_schemas(con) -> None:
    for schema in [PREPROCESSOR_SCHEMA, HELPER_SCHEMA, WRAPPER_SCHEMA, SOURCE_SCHEMA, PREVIEW_SCHEMA]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def cleanup_package_schemas(con, package_config: dict[str, object]) -> None:
    for schema in [
        package_config["preprocessor"]["schema"],
        package_config["helperSchema"],
        package_config["wrapperSchema"],
        package_config["sourceSchema"],
    ]:
        con.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_unified_cli_ingest_wrap_validate_with_manifest_handoff() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "nested.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_dir = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "NESTED.json"
        shutil.copyfile(fixture_path, input_path)

        con = connect()
        try:
            cleanup_schemas(con)
            con.execute(f'CREATE SCHEMA "{SOURCE_SCHEMA}"')
        finally:
            con.close()

        try:
            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "ingest",
                    "--input",
                    str(input_path),
                    "--artifact-dir",
                    str(artifact_dir),
                    "--exasol",
                    f"exasol://sys:exasol@127.0.0.1:8563/{SOURCE_SCHEMA}?tls=1&validateservercertificate=0",
                    "--exasol-temp-dir",
                    str(staging_dir),
                    "--exasol-cleanup",
                ],
                cwd=ROOT,
                check=True,
            )

            manifest_paths = sorted(artifact_dir.glob("*.source_manifest.json"))
            assert len(manifest_paths) == 1
            source_manifest_path = manifest_paths[0]

            generate = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "wrap",
                    "generate",
                    "--artifact-dir",
                    str(artifact_dir),
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
                    "--package-name",
                    PACKAGE_NAME,
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            assert "Unified CLI using source manifest:" in generate.stdout

            package_config_path = artifact_dir / f"{PACKAGE_NAME}_package.json"
            package_config = json.loads(package_config_path.read_text())
            assert package_config["sourceManifest"] == str(source_manifest_path.resolve())

            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "wrap",
                    "install",
                    "--package-config",
                    str(package_config_path),
                ],
                cwd=ROOT,
                check=True,
            )

            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "validate",
                    "--package-config",
                    str(package_config_path),
                    "--check-installed",
                ],
                cwd=ROOT,
                check=True,
            )

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


def test_unified_cli_structured_results_preview_json() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_preview_") as tmpdir:
        tmp = Path(tmpdir)
        config_path = tmp / "order_report.json"
        config_path.write_text(
            json.dumps(
                {
                    "kind": "synthesized_family",
                    "rootTable": "ORDER_REPORT",
                    "tableSpecs": [
                        {
                            "tableName": "ORDER_REPORT",
                            "selectSql": """
                                SELECT
                                  CAST(1 AS DECIMAL(18,0)) AS "_id",
                                  CAST(101 AS DECIMAL(18,0)) AS "order_id",
                                  'open' AS "status",
                                  CAST(2 AS DECIMAL(18,0)) AS "lines|array"
                                FROM DUAL
                            """,
                        },
                        {
                            "tableName": "ORDER_REPORT_lines_arr",
                            "selectSql": """
                                SELECT
                                  CAST(1 AS DECIMAL(18,0)) AS "_parent",
                                  CAST(0 AS DECIMAL(18,0)) AS "_pos",
                                  'A' AS "sku"
                                FROM DUAL
                                UNION ALL
                                SELECT
                                  CAST(1 AS DECIMAL(18,0)),
                                  CAST(1 AS DECIMAL(18,0)),
                                  'B'
                                FROM DUAL
                            """,
                        },
                    ],
                },
                indent=2,
            )
            + "\n"
        )

        result = subprocess.run(
            [
                "python3",
                str(CLI),
                "structured-results",
                "preview-json",
                "--result-family-config",
                str(config_path),
                "--target-schema",
                PREVIEW_SCHEMA,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout)
        assert payload == [
            {
                "order_id": 101,
                "status": "open",
                "lines": [{"sku": "A"}, {"sku": "B"}],
            }
        ]


def test_unified_cli_wrap_deploy_chains_install_and_validate() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "nested.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_deploy_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_dir = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "NESTED.json"
        shutil.copyfile(fixture_path, input_path)

        con = connect()
        try:
            cleanup_schemas(con)
            con.execute(f'CREATE SCHEMA "{SOURCE_SCHEMA}"')
        finally:
            con.close()

        try:
            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "ingest",
                    "--input",
                    str(input_path),
                    "--artifact-dir",
                    str(artifact_dir),
                    "--exasol",
                    f"exasol://sys:exasol@127.0.0.1:8563/{SOURCE_SCHEMA}?tls=1&validateservercertificate=0",
                    "--exasol-temp-dir",
                    str(staging_dir),
                    "--exasol-cleanup",
                ],
                cwd=ROOT,
                check=True,
            )

            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "wrap",
                    "generate",
                    "--artifact-dir",
                    str(artifact_dir),
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
                    "--package-name",
                    PACKAGE_NAME,
                ],
                cwd=ROOT,
                check=True,
            )

            package_config_path = artifact_dir / f"{PACKAGE_NAME}_package.json"
            deploy = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "wrap",
                    "deploy",
                    "--package-config",
                    str(package_config_path),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            assert "Validated installed package for" in deploy.stdout

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{PREPROCESSOR_SCHEMA}"."{PREPROCESSOR_SCRIPT}"'
                )
                rows = con.execute(
                    f'''
                    SELECT
                      CAST("id" AS VARCHAR(20)),
                      CAST("child.a" AS VARCHAR(20))
                    FROM "{WRAPPER_SCHEMA}"."NESTED"
                    ORDER BY 1
                    '''
                ).fetchall()
            finally:
                con.close()

            assert rows == [("1", "10"), ("2", "20"), ("3", None)]
        finally:
            con = connect()
            try:
                cleanup_schemas(con)
            finally:
                con.close()


def test_unified_cli_ingest_and_wrap_with_derived_defaults() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "nested.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_phase5_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "NESTED.json"
        shutil.copyfile(fixture_path, input_path)

        workflow_name = "phase5_nested"
        run_artifact_dir = artifact_root / workflow_name
        package_config_path = run_artifact_dir / f"{workflow_name}_wrapper_package.json"
        package_config: Optional[dict[str, object]] = None

        try:
            result = subprocess.run(
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
            assert "Unified CLI completed ingest-and-wrap workflow." in result.stdout
            assert package_config_path.exists()
            assert (run_artifact_dir / "NESTED.source_manifest.json").exists()

            package_config = json.loads(package_config_path.read_text())
            assert package_config["sourceManifest"] == str((run_artifact_dir / "NESTED.source_manifest.json").resolve())

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{package_config["preprocessor"]["schema"]}"."{package_config["preprocessor"]["script"]}"'
                )
                rows = con.execute(
                    f'''
                    SELECT
                      CAST("id" AS VARCHAR(20)),
                      CAST("child.a" AS VARCHAR(20)),
                      "meta.info.note"
                    FROM "{package_config["wrapperSchema"]}"."NESTED"
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
            if package_config is not None:
                con = connect()
                try:
                    cleanup_package_schemas(con, package_config)
                finally:
                    con.close()


if __name__ == "__main__":
    test_unified_cli_ingest_wrap_validate_with_manifest_handoff()
    test_unified_cli_structured_results_preview_json()
    test_unified_cli_wrap_deploy_chains_install_and_validate()
    test_unified_cli_ingest_and_wrap_with_derived_defaults()
    print("-- unified cli regression --")
    print("verified ingest/wrap/validate orchestration, wrap deploy, ingest-and-wrap defaults, and structured-results preview-json")

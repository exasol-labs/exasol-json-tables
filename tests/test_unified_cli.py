#!/usr/bin/env python3

from __future__ import annotations

import json
import ssl
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import _bootstrap  # noqa: F401

from exasol_json_tables import cli as cli_module
from exasol_json_tables import wrapper_schema_support
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


def cleanup_named_workflow_schemas(con, workflow_name: str) -> None:
    token = workflow_name.upper()
    for schema in [
        f"EJT_{token}_PP",
        f"EJT_{token}_VIEW_INTERNAL",
        f"EJT_{token}_VIEW",
        f"EJT_{token}_SRC",
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
            assert "Validated installed query probes:" in deploy.stdout
            assert "qualified-helper" in deploy.stdout
            assert "TO_JSON(*)" in deploy.stdout

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
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

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
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_ingest_and_wrap_with_lowercase_root_name() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "sample.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_lowercase_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "sample.json"
        shutil.copyfile(fixture_path, input_path)

        workflow_name = "lowercase_sample"
        run_artifact_dir = artifact_root / workflow_name
        package_config_path = run_artifact_dir / f"{workflow_name}_wrapper_package.json"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

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
            package_config = json.loads(package_config_path.read_text())

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{package_config["preprocessor"]["schema"]}"."{package_config["preprocessor"]["script"]}"'
                )
                rows = con.execute(
                    f'''
                    SELECT
                      CAST("id" AS VARCHAR(20)),
                      JSON_AS_VARCHAR("name"),
                      COALESCE("meta.team", 'NULL'),
                      COALESCE("tags[LAST]", 'NULL')
                    FROM "{package_config["wrapperSchema"]}"."sample"
                    ORDER BY 1
                    '''
                ).fetchall()
                aliased_rows = con.execute(
                    f'''
                    SELECT
                      CAST(s."id" AS VARCHAR(20)),
                      COALESCE(JSON_AS_VARCHAR(s."name"), 'NULL'),
                      COALESCE(CAST(tag._index AS VARCHAR(10)), 'NULL'),
                      COALESCE(CAST(tag AS VARCHAR(20)), 'NULL'),
                      TO_JSON(s.*)
                    FROM "{package_config["wrapperSchema"]}"."sample" s
                    LEFT JOIN VALUE tag IN s."tags"
                    ORDER BY 1, 3
                    '''
                ).fetchall()
            finally:
                con.close()

            assert rows == [
                ("1", "Alice", "A", "y"),
                ("2", "Bob", "NULL", "NULL"),
                ("3", "Carol", "NULL", "NULL"),
            ]
            assert aliased_rows == [
                (
                    "1",
                    "Alice",
                    "0",
                    "x",
                    '{"active":true,"age":30,"id":1,"meta":{"team":"A"},"name":"Alice","note":null,"score":12.5,"tags":["x","y"]}',
                ),
                (
                    "1",
                    "Alice",
                    "1",
                    "y",
                    '{"active":true,"age":30,"id":1,"meta":{"team":"A"},"name":"Alice","note":null,"score":12.5,"tags":["x","y"]}',
                ),
                (
                    "2",
                    "Bob",
                    "NULL",
                    "NULL",
                    '{"active":false,"age":null,"id":2,"misc":"extra","name":"Bob","score":15}',
                ),
                (
                    "3",
                    "Carol",
                    "NULL",
                    "NULL",
                    '{"active":true,"age":28,"height":170.2,"id":3,"meta":{},"name":"Carol","note":"hello","score":null}',
                ),
            ]
        finally:
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_ingest_and_wrap_with_explicit_null_only_root_property() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "edge_cases.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_null_only_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "edge_cases.json"
        shutil.copyfile(fixture_path, input_path)

        workflow_name = "edge_cases_nulls"
        run_artifact_dir = artifact_root / workflow_name
        package_config_path = run_artifact_dir / f"{workflow_name}_wrapper_package.json"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

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
            package_config = json.loads(package_config_path.read_text())

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{package_config["preprocessor"]["schema"]}"."{package_config["preprocessor"]["script"]}"'
                )
                rows = con.execute(
                    f'''
                    SELECT
                      CAST(e."id" AS VARCHAR(20)),
                      CASE WHEN JSON_IS_EXPLICIT_NULL(e."only_null") THEN '1' ELSE '0' END,
                      COALESCE(JSON_AS_VARCHAR(e."only_null"), 'NULL'),
                      TO_JSON(e.*)
                    FROM "{package_config["wrapperSchema"]}"."edge_cases" e
                    ORDER BY 1
                    '''
                ).fetchall()
            finally:
                con.close()

            assert rows == [
                (
                    "1",
                    "1",
                    "NULL",
                    '{"id":1,"missing_vs_null":null,"mixed_num_arr":[1,2.5],"obj_arr":[{"a":1},{"a":null}],"only_null":null,"only_null_arr":[null,null]}',
                ),
                (
                    "2",
                    "0",
                    "NULL",
                    '{"arr_mix":[],"id":2,"missing_vs_null":10,"mixed_num_arr":[],"obj_arr":[],"only_null_arr":[]}',
                ),
                (
                    "3",
                    "1",
                    "NULL",
                    '{"arr_mix":[1,2],"id":3,"missing_vs_null":null,"mixed_num_arr":[3],"obj_arr":[{"a":2},{"a":3}],"only_null":null,"only_null_arr":[null]}',
                ),
            ]
        finally:
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_ingest_and_wrap_json_summary() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "nested.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_json_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "NESTED.json"
        shutil.copyfile(fixture_path, input_path)

        workflow_name = "json_summary"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

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
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(result.stdout)
            assert payload["schemaVersion"] == 1
            assert payload["status"] == "ok"
            assert payload["command"] == "ingest-and-wrap"
            assert payload["errors"] == []
            assert payload["workflowName"] == workflow_name
            assert payload["validatedInstalled"] is True
            assert "nextActions" in payload
            assert "artifacts" in payload
            assert "objects" in payload
            wrapper = payload["wrapper"]
            assert wrapper["preprocessor"]["activationRequired"] is True
            assert "ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT" in wrapper["preprocessor"]["activationSql"]
            assert "smokeTestSql" in wrapper
            assert len(wrapper["warnings"]) >= 2
            assert any("session-scoped" in warning for warning in wrapper["warnings"])
            assert any("wrapper schemas only" in warning for warning in wrapper["warnings"])
            assert payload["nextActions"]["activationSql"] == wrapper["preprocessor"]["activationSql"]
            assert payload["nextActions"]["smokeTestSql"] == wrapper["smokeTestSql"]
            assert payload["nextActions"]["publicViews"] == ["NESTED"]
            assert payload["artifacts"]["packageConfig"] == wrapper["packageConfig"]
            assert payload["objects"]["wrapperSchema"] == wrapper["wrapperSchema"]
            assert payload["objects"]["publicViews"] == ["NESTED"]
            assert wrapper["publicViews"] == ["NESTED"]

            validation = payload["validation"]
            assert validation["checkedInstalled"] is True
            installed = validation["installed"]
            assert installed["capabilities"]["qualifiedHelper"] == {"supported": True, "ok": True}
            assert installed["capabilities"]["toJson"] == {"supported": True, "ok": True}
            assert len(installed["probes"]) >= 2
            assert all("sql" in probe for probe in installed["probes"])

            package_config_path = Path(wrapper["packageConfig"])
            package_config = json.loads(package_config_path.read_text())

            validate_no_tls_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "validate",
                    "--package-config",
                    str(package_config_path),
                    "--check-installed",
                    "--no-tls",
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            validate_no_tls_payload = json.loads(validate_no_tls_result.stdout)
            assert validate_no_tls_payload["status"] == "ok"
            assert validate_no_tls_payload["command"] == "validate"

            wrap_install_no_tls_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "wrap",
                    "install",
                    "--package-config",
                    str(package_config_path),
                    "--skip-views",
                    "--skip-source-family",
                    "--skip-preprocessor",
                    "--no-tls",
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            wrap_install_no_tls_payload = json.loads(wrap_install_no_tls_result.stdout)
            assert wrap_install_no_tls_payload["status"] == "ok"
            assert wrap_install_no_tls_payload["command"] == "wrap install"

            con = connect()
            try:
                con.execute(wrapper["preprocessor"]["activationSql"].rstrip(";"))
                rows = con.execute(
                    f'''
                    SELECT
                      CAST("id" AS VARCHAR(20)),
                      CAST("child.a" AS VARCHAR(20))
                    FROM "{package_config["wrapperSchema"]}"."NESTED"
                    ORDER BY 1
                    '''
                ).fetchall()
            finally:
                con.close()

            assert rows == [("1", "10"), ("2", "20"), ("3", None)]
        finally:
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_validate_and_describe_json_surfaces() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "nested.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_describe_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "NESTED.json"
        shutil.copyfile(fixture_path, input_path)

        workflow_name = "describe_json"
        published_schema = "JVS_CLI_PUBLISHED"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                con.execute(f'DROP SCHEMA IF EXISTS "{published_schema}" CASCADE')
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

            ingest_wrap = subprocess.run(
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
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            wrap_payload = json.loads(ingest_wrap.stdout)
            package_config_path = Path(wrap_payload["wrapper"]["packageConfig"])
            package_config = json.loads(package_config_path.read_text())

            validate_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "validate",
                    "--package-config",
                    str(package_config_path),
                    "--check-installed",
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            validate_payload = json.loads(validate_result.stdout)
            assert validate_payload["status"] == "ok"
            assert validate_payload["command"] == "validate"
            assert validate_payload["checkedInstalled"] is True
            assert validate_payload["validation"]["installed"]["capabilities"]["qualifiedHelper"]["ok"] is True
            assert validate_payload["validation"]["installed"]["capabilities"]["toJson"]["ok"] is True
            assert validate_payload["validation"]["installed"]["metadata"]["integrity"]["publicViewsMatchManifest"] is True
            assert validate_payload["validation"]["installed"]["metadata"]["integrity"]["rootCountMatchesManifest"] is True

            describe_package_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "describe",
                    "package",
                    "--package-config",
                    str(package_config_path),
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            describe_package_payload = json.loads(describe_package_result.stdout)
            assert describe_package_payload["status"] == "ok"
            assert describe_package_payload["command"] == "describe package"
            assert describe_package_payload["description"]["wrapperSchema"] == package_config["wrapperSchema"]
            assert describe_package_payload["description"]["rootCount"] == 1
            root_description = describe_package_payload["description"]["roots"][0]
            assert root_description["publicView"] == "NESTED"
            assert "toJsonAll" in root_description["exampleQueries"]
            assert "qualifiedHelper" in root_description["exampleQueries"]

            con = connect()
            try:
                con.execute(f'CREATE SCHEMA "{published_schema}"')
                con.execute(
                    f'''
                    CREATE OR REPLACE VIEW "{published_schema}"."NESTED_PUBLISHED" AS
                    SELECT CAST("_id" AS VARCHAR(20)) AS doc_id
                    FROM "{package_config["wrapperSchema"]}"."NESTED"
                    '''
                )
            finally:
                con.close()

            describe_wrapper_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "describe",
                    "wrapper",
                    "--wrapper-schema",
                    str(package_config["wrapperSchema"]),
                    "--preprocessor-schema",
                    str(package_config["preprocessor"]["schema"]),
                    "--preprocessor-script",
                    str(package_config["preprocessor"]["script"]),
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            describe_wrapper_payload = json.loads(describe_wrapper_result.stdout)
            assert describe_wrapper_payload["status"] == "ok"
            assert describe_wrapper_payload["command"] == "describe wrapper"
            assert describe_wrapper_payload["description"]["wrapperSchema"] == package_config["wrapperSchema"]
            assert describe_wrapper_payload["discovery"]["autodiscoveredHelperSchema"] is True
            assert describe_wrapper_payload["discovery"]["surfaceKind"] == "wrapperPackage"
            assert describe_wrapper_payload["installedState"]["integrity"]["publicViewsMatchManifest"] is True
            assert describe_wrapper_payload["nextActions"]["activationRequired"] is True
            assert describe_wrapper_payload["nextActions"]["publicViews"] == ["NESTED"]
            assert (
                describe_wrapper_payload["nextActions"]["activationSql"]
                == wrap_payload["wrapper"]["preprocessor"]["activationSql"]
            )
            assert (
                describe_wrapper_payload["description"]["preprocessor"]["activationSql"]
                == wrap_payload["wrapper"]["preprocessor"]["activationSql"]
            )

            describe_wrappers_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "describe",
                    "wrappers",
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            describe_wrappers_payload = json.loads(describe_wrappers_result.stdout)
            assert describe_wrappers_payload["status"] == "ok"
            assert describe_wrappers_payload["command"] == "describe wrappers"
            assert describe_wrappers_payload["discovery"]["publishedConsumerSurfacesIncluded"] is False
            wrapper_schemas = [entry["description"]["wrapperSchema"] for entry in describe_wrappers_payload["wrappers"]]
            assert package_config["wrapperSchema"] in wrapper_schemas
            assert published_schema not in wrapper_schemas
            matching_entry = next(
                entry
                for entry in describe_wrappers_payload["wrappers"]
                if entry["wrapperSchema"] == package_config["wrapperSchema"]
            )
            assert matching_entry["helperSchema"] == package_config["helperSchema"]
            assert matching_entry["sourceSchema"] == package_config["sourceSchema"]
            assert matching_entry["publicViews"] == ["NESTED"]
        finally:
            con = connect()
            try:
                con.execute(f'DROP SCHEMA IF EXISTS "{published_schema}" CASCADE')
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_ingest_and_wrap_supports_object_fields_inside_array_items() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_array_object_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "V2BUG1_INPUT.json"
        input_documents = [
            {
                "id": "1",
                "reviews": [
                    {
                        "user": "alice",
                        "rating": 5,
                        "date": {"$date": "2026-01-01T00:00:00Z"},
                    }
                ],
            }
        ]
        input_path.write_text(json.dumps(input_documents))

        workflow_name = "v2bug1_arrobj"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

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
                    "--no-tls",
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(result.stdout)
            assert payload["status"] == "ok"
            assert payload["wrapper"]["publicViews"] == ["V2BUG1_INPUT"]
            package_config = json.loads(Path(payload["wrapper"]["packageConfig"]).read_text())

            con = connect()
            try:
                con.execute(payload["wrapper"]["preprocessor"]["activationSql"].rstrip(";"))
                rows = con.execute(
                    f'''
                    SELECT TO_JSON(*)
                    FROM "{package_config["wrapperSchema"]}"."V2BUG1_INPUT"
                    ORDER BY "_id"
                    '''
                ).fetchall()
            finally:
                con.close()

            assert [json.loads(row[0]) for row in rows] == input_documents
        finally:
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_supports_to_json_on_iterator_row_aliases() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "arrays.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_iterator_to_json_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / "ARRAYS.json"
        shutil.copyfile(fixture_path, input_path)

        workflow_name = "v2bug6_iterjson"
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()

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
                    "--json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(result.stdout)
            assert payload["status"] == "ok"
            package_config = json.loads(Path(payload["wrapper"]["packageConfig"]).read_text())

            con = connect()
            try:
                con.execute(payload["wrapper"]["preprocessor"]["activationSql"].rstrip(";"))
                rows = con.execute(
                    f'''
                    SELECT
                      CAST(s."id" AS VARCHAR(10)),
                      TO_JSON(item.*)
                    FROM "{package_config["wrapperSchema"]}"."ARRAYS" s
                    JOIN item IN s."objs"
                    ORDER BY s."_id", item._index
                    '''
                ).fetchall()
            finally:
                con.close()

            assert [(row[0], json.loads(row[1])) for row in rows] == [
                ("1", {"x": 1}),
                ("1", {"x": 2, "y": "a"}),
                ("3", {"x": 3.14, "inner": [{"z": True}, {"z": False}]}),
            ]
        finally:
            con = connect()
            try:
                if package_config is not None:
                    cleanup_package_schemas(con, package_config)
                cleanup_named_workflow_schemas(con, workflow_name)
            finally:
                con.close()


def test_unified_cli_json_failure_envelope() -> None:
    missing_package_config = ROOT / "dist" / "does_not_exist_package.json"
    result = subprocess.run(
        [
            "python3",
            str(CLI),
            "validate",
            "--package-config",
            str(missing_package_config),
            "--json",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["command"] == "validate"
    assert payload["errors"][0]["code"] == "FILE-NOT-FOUND"
    assert payload["errors"][0]["repro"]["argv"][0] == "validate"


def test_unified_cli_error_repro_redacts_password() -> None:
    missing_package_config = ROOT / "dist" / "does_not_exist_package.json"
    result = subprocess.run(
        [
            "python3",
            str(CLI),
            "validate",
            "--package-config",
            str(missing_package_config),
            "--password",
            "mysecret",
            "--json",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    payload = json.loads(result.stdout)
    argv = payload["errors"][0]["repro"]["argv"]
    assert argv[0] == "validate"
    assert "--password" in argv
    assert "mysecret" not in argv
    assert "***" in argv


def test_unified_cli_ingest_json_artifacts_are_structured() -> None:
    fixture_path = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "sample.json"
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_ingest_json_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_dir = tmp / "artifacts"
        input_path = tmp / "sample.json"
        shutil.copyfile(fixture_path, input_path)

        result = subprocess.run(
            [
                "python3",
                str(CLI),
                "ingest",
                "--input",
                str(input_path),
                "--artifact-dir",
                str(artifact_dir),
                "--json",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        assert payload["command"] == "ingest"
        artifacts = payload["artifacts"]
        assert Path(artifacts["sourceManifest"]).name.endswith(".source_manifest.json")
        assert Path(artifacts["outputDir"]).exists()
        assert artifacts["parquetFiles"]
        assert all(path.endswith(".parquet") for path in artifacts["parquetFiles"])


def test_unified_cli_ingest_error_codes() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_cli_ingest_errors_") as tmpdir:
        tmp = Path(tmpdir)

        invalid_json_path = tmp / "invalid.json"
        invalid_json_path.write_text('{"bad": [1,}')
        invalid_json = subprocess.run(
            [
                "python3",
                str(CLI),
                "ingest",
                "--input",
                str(invalid_json_path),
                "--artifact-dir",
                str(tmp / "invalid_artifacts"),
                "--json",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        invalid_payload = json.loads(invalid_json.stdout)
        assert invalid_json.returncode != 0
        assert invalid_payload["errors"][0]["code"] == "INGEST-JSON-PARSE-ERROR"

        unsupported_path = tmp / "unsupported.json"
        unsupported_path.write_text("[1, 2, 3]\n")
        unsupported_result = subprocess.run(
            [
                "python3",
                str(CLI),
                "ingest",
                "--input",
                str(unsupported_path),
                "--artifact-dir",
                str(tmp / "unsupported_artifacts"),
                "--json",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        unsupported_payload = json.loads(unsupported_result.stdout)
        assert unsupported_result.returncode != 0
        assert unsupported_payload["errors"][0]["code"] == "INGEST-UNSUPPORTED-INPUT-FORMAT"

        artifact_dir_file = tmp / "artifact_dir_file"
        artifact_dir_file.write_text("not a directory")
        filesystem_result = subprocess.run(
            [
                "python3",
                str(CLI),
                "ingest",
                "--input",
                str(unsupported_path),
                "--artifact-dir",
                str(artifact_dir_file),
                "--json",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        filesystem_payload = json.loads(filesystem_result.stdout)
        assert filesystem_result.returncode != 0
        assert filesystem_payload["errors"][0]["code"] == "INGEST-LOCAL-FILESYSTEM-ERROR"

        db_result = subprocess.run(
            [
                "python3",
                str(CLI),
                "ingest",
                "--input",
                str(unsupported_path),
                "--artifact-dir",
                str(tmp / "db_artifacts"),
                "--exasol",
                "exasol://sys:nottherightpassword@127.0.0.1:8563/JVS_BAD_AUTH?tls=1&validateservercertificate=0",
                "--json",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        db_payload = json.loads(db_result.stdout)
        assert db_result.returncode != 0
        assert db_payload["errors"][0]["code"] == "INGEST-DATABASE-IMPORT-ERROR"


def test_wrapper_generation_connection_ssl_options() -> None:
    captured: list[dict[str, object]] = []
    original_connect = wrapper_schema_support.pyexasol.connect
    try:
        def fake_connect(**kwargs):
            captured.append(kwargs)
            class DummyConnection:
                pass
            return DummyConnection()

        wrapper_schema_support.pyexasol.connect = fake_connect
        wrapper_schema_support.connect_for_generation("dsn", "user", "password")
        wrapper_schema_support.connect_for_generation(
            "dsn",
            "user",
            "password",
            validate_certificate=True,
        )
    finally:
        wrapper_schema_support.pyexasol.connect = original_connect

    assert captured[0]["websocket_sslopt"]["cert_reqs"] == ssl.CERT_NONE
    assert "websocket_sslopt" not in captured[1]


def test_unified_cli_schema_ensure_propagates_certificate_validation() -> None:
    calls: list[bool] = []

    class DummyConnection:
        def __init__(self) -> None:
            self.executed: list[str] = []

        def execute(self, sql: str):
            self.executed.append(sql)

        def close(self) -> None:
            pass

    original_connect = cli_module.connect_for_generation
    try:
        def fake_connect_for_generation(dsn, user, password, schema="SYS", validate_certificate=False):
            calls.append(bool(validate_certificate))
            return DummyConnection()

        cli_module.connect_for_generation = fake_connect_for_generation
        cli_module._ensure_schema_exists("dsn", "user", "password", "A_SCHEMA", validate_server_certificate=False)
        cli_module._ensure_schema_exists("dsn", "user", "password", "B_SCHEMA", validate_server_certificate=True)
    finally:
        cli_module.connect_for_generation = original_connect

    assert calls == [False, True]


if __name__ == "__main__":
    test_unified_cli_ingest_wrap_validate_with_manifest_handoff()
    test_unified_cli_structured_results_preview_json()
    test_unified_cli_wrap_deploy_chains_install_and_validate()
    test_unified_cli_ingest_and_wrap_with_derived_defaults()
    test_unified_cli_ingest_and_wrap_with_lowercase_root_name()
    test_unified_cli_ingest_and_wrap_json_summary()
    test_unified_cli_validate_and_describe_json_surfaces()
    test_unified_cli_json_failure_envelope()
    test_unified_cli_error_repro_redacts_password()
    test_unified_cli_ingest_json_artifacts_are_structured()
    test_unified_cli_ingest_error_codes()
    test_wrapper_generation_connection_ssl_options()
    test_unified_cli_schema_ensure_propagates_certificate_validation()
    print("-- unified cli regression --")
    print("verified ingest/wrap/validate orchestration, wrap deploy, ingest-and-wrap defaults, and structured-results preview-json")

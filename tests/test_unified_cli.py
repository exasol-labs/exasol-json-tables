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
            assert payload["artifacts"]["packageConfig"] == wrapper["packageConfig"]
            assert payload["objects"]["wrapperSchema"] == wrapper["wrapperSchema"]

            validation = payload["validation"]
            assert validation["checkedInstalled"] is True
            installed = validation["installed"]
            assert installed["capabilities"]["qualifiedHelper"] == {"supported": True, "ok": True}
            assert installed["capabilities"]["toJson"] == {"supported": True, "ok": True}
            assert len(installed["probes"]) >= 2
            assert all("sql" in probe for probe in installed["probes"])

            package_config_path = Path(wrapper["packageConfig"])
            package_config = json.loads(package_config_path.read_text())

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
        package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
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

            describe_wrapper_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "describe",
                    "wrapper",
                    "--wrapper-schema",
                    str(package_config["wrapperSchema"]),
                    "--helper-schema",
                    str(package_config["helperSchema"]),
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
            assert (
                describe_wrapper_payload["description"]["preprocessor"]["activationSql"]
                == wrap_payload["wrapper"]["preprocessor"]["activationSql"]
            )
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


if __name__ == "__main__":
    test_unified_cli_ingest_wrap_validate_with_manifest_handoff()
    test_unified_cli_structured_results_preview_json()
    test_unified_cli_wrap_deploy_chains_install_and_validate()
    test_unified_cli_ingest_and_wrap_with_derived_defaults()
    test_unified_cli_ingest_and_wrap_with_lowercase_root_name()
    test_unified_cli_ingest_and_wrap_json_summary()
    test_unified_cli_validate_and_describe_json_surfaces()
    test_unified_cli_json_failure_envelope()
    print("-- unified cli regression --")
    print("verified ingest/wrap/validate orchestration, wrap deploy, ingest-and-wrap defaults, and structured-results preview-json")

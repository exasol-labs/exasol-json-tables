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
FIXTURE_PATH = ROOT / "crates" / "json_tables_ingest" / "tests" / "fixtures" / "sample.json"

WORKFLOW_NAME = "quickstart_sample"
WORKFLOW_PACKAGE_NAME = "quickstart_sample_wrapper"

RESULT_SOURCE_SCHEMA = "JVS_SAMPLE_REPORT_SRC"
RESULT_WRAPPER_SCHEMA = "JSON_VIEW_SAMPLE_REPORT"
RESULT_HELPER_SCHEMA = "JSON_VIEW_SAMPLE_REPORT_INTERNAL"
RESULT_PP_SCHEMA = "JVS_SAMPLE_REPORT_PP"
RESULT_PP_SCRIPT = "JSON_SAMPLE_REPORT_PREPROCESSOR"
RESULT_PACKAGE_NAME = "sample_report"


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


def write_shape_config(path: Path, wrapper_schema: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "kind": "structured_shape",
                "rootTable": "SAMPLE_REPORT",
                "root": {
                    "fromSql": f'FROM "{wrapper_schema}"."sample" s',
                    "idSql": 's."id"',
                    "fields": [
                        {"name": "sample_id", "sql": 's."id"'},
                        {"name": "name", "sql": 'JSON_AS_VARCHAR(s."name")'},
                        {
                            "name": "note_state",
                            "sql": """CASE
                                WHEN JSON_IS_EXPLICIT_NULL(s."note") THEN 'explicit-null'
                                WHEN s."note" IS NULL THEN 'missing'
                                ELSE 'value'
                              END""",
                        },
                        {"name": "summary", "kind": "object_ref", "sql": 's."id"'},
                        {
                            "name": "tags",
                            "kind": "array_ref",
                            "sql": 'CASE WHEN s."tags[SIZE]" IS NULL THEN 0 ELSE s."tags[SIZE]" END',
                        },
                    ],
                    "objects": [
                        {
                            "name": "summary",
                            "fromSql": f'FROM "{wrapper_schema}"."sample" s',
                            "idSql": 's."id"',
                            "fields": [
                                {"name": "team", "sql": 's."meta.team"'},
                                {"name": "last_tag", "sql": 's."tags[LAST]"'},
                            ],
                        }
                    ],
                    "arrays": [
                        {
                            "name": "tags",
                            "fromSql": f'FROM "{wrapper_schema}"."sample" s JOIN VALUE tag IN s."tags"',
                            "parentIdSql": 's."id"',
                            "positionSql": "tag._index",
                            "valueSql": "tag",
                        }
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="exasol_json_tables_quickstart_shape_") as tmpdir:
        tmp = Path(tmpdir)
        artifact_root = tmp / "artifacts"
        staging_dir = tmp / "staging"
        input_path = tmp / FIXTURE_PATH.name
        result_package_dir = tmp / "result_package"
        result_shape_path = result_package_dir / "sample_report_shape.json"
        result_package_config_path = result_package_dir / f"{RESULT_PACKAGE_NAME}_package.json"
        shutil.copyfile(FIXTURE_PATH, input_path)

        workflow_config_path = artifact_root / WORKFLOW_NAME / f"{WORKFLOW_PACKAGE_NAME}_package.json"
        workflow_package_config: Optional[dict[str, object]] = None
        result_package_config: Optional[dict[str, object]] = None

        try:
            con = connect()
            try:
                cleanup_named_workflow_schemas(con, WORKFLOW_NAME)
                con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_SOURCE_SCHEMA}" CASCADE')
                con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_WRAPPER_SCHEMA}" CASCADE')
                con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_HELPER_SCHEMA}" CASCADE')
                con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_PP_SCHEMA}" CASCADE')
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
                    WORKFLOW_NAME,
                    "--wrapper-schema",
                    "JSON_VIEW_SAMPLE",
                    "--helper-schema",
                    "JSON_VIEW_SAMPLE_INTERNAL",
                    "--preprocessor-schema",
                    "JVS_SAMPLE_PP",
                    "--preprocessor-script",
                    "JSON_SAMPLE_PREPROCESSOR",
                    "--package-name",
                    WORKFLOW_PACKAGE_NAME,
                    "--artifact-dir",
                    str(artifact_root),
                    "--exasol-temp-dir",
                    str(staging_dir),
                    "--exasol-cleanup",
                ],
                cwd=ROOT,
                check=True,
            )

            workflow_package_config = json.loads(workflow_config_path.read_text())
            write_shape_config(result_shape_path, str(workflow_package_config["wrapperSchema"]))

            con = connect()
            try:
                con.execute(
                    f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{workflow_package_config["preprocessor"]["schema"]}"."{workflow_package_config["preprocessor"]["script"]}"'
                )
                helper_rows = con.execute(
                    f'''
                    SELECT
                      CAST("id" AS VARCHAR(10)),
                      COALESCE(JSON_AS_VARCHAR("name"), 'NULL'),
                      CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END
                    FROM "{workflow_package_config["wrapperSchema"]}"."sample"
                    ORDER BY "id"
                    '''
                ).fetchall()
                rowset_rows = con.execute(
                    f'''
                    SELECT
                      CAST(s."id" AS VARCHAR(10)),
                      COALESCE(JSON_AS_VARCHAR(s."name"), 'NULL'),
                      CASE WHEN JSON_IS_EXPLICIT_NULL(s."note") THEN '1' ELSE '0' END,
                      COALESCE(CAST(tag._index AS VARCHAR(10)), 'NULL'),
                      COALESCE(CAST(tag AS VARCHAR(20)), 'NULL')
                    FROM "{workflow_package_config["wrapperSchema"]}"."sample" s
                    LEFT JOIN VALUE tag IN s."tags"
                    ORDER BY 1, 4
                    '''
                ).fetchall()
            finally:
                try:
                    con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
                except Exception:
                    pass
                con.close()

            assert_equal(
                helper_rows,
                [
                    ("1", "Alice", "1"),
                    ("2", "Bob", "0"),
                    ("3", "Carol", "0"),
                ],
                "quickstart wrapper helper query",
            )
            assert_equal(
                rowset_rows,
                [
                    ("1", "Alice", "1", "0", "x"),
                    ("1", "Alice", "1", "1", "y"),
                    ("2", "Bob", "0", "NULL", "NULL"),
                    ("3", "Carol", "0", "NULL", "NULL"),
                ],
                "quickstart wrapper rowset query",
            )

            subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "structured-results",
                    "package",
                    "--source-schema",
                    RESULT_SOURCE_SCHEMA,
                    "--wrapper-schema",
                    RESULT_WRAPPER_SCHEMA,
                    "--helper-schema",
                    RESULT_HELPER_SCHEMA,
                    "--preprocessor-schema",
                    RESULT_PP_SCHEMA,
                    "--preprocessor-script",
                    RESULT_PP_SCRIPT,
                    "--output-dir",
                    str(result_package_dir),
                    "--package-name",
                    RESULT_PACKAGE_NAME,
                    "--result-family-config",
                    str(result_shape_path),
                ],
                cwd=ROOT,
                check=True,
            )

            result_package_config = json.loads(result_package_config_path.read_text())
            deploy_result = subprocess.run(
                [
                    "python3",
                    str(CLI),
                    "wrap",
                    "deploy",
                    "--package-config",
                    str(result_package_config_path),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            if "Validated installed package for" not in deploy_result.stdout:
                raise AssertionError("result package deploy should validate the installed generated package")
            if "Validated installed query probes: rowset, qualified-helper, TO_JSON(*)" not in deploy_result.stdout:
                raise AssertionError("result package deploy should prove rowset/helper/TO_JSON coverage")

            con = connect()
            try:
                con.execute(f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{RESULT_PP_SCHEMA}"."{RESULT_PP_SCRIPT}"')
                wrapper_rows = con.execute(
                    f"""
                    SELECT
                      CAST("sample_id" AS VARCHAR(10)),
                      "name",
                      "note_state",
                      COALESCE("summary.team", 'NULL'),
                      COALESCE("tags[LAST]", 'NULL')
                    FROM "{RESULT_WRAPPER_SCHEMA}"."SAMPLE_REPORT"
                    ORDER BY "sample_id"
                    """
                ).fetchall()
                json_rows = [
                    json.loads(row[0])
                    for row in con.execute(
                        f'''
                        SELECT TO_JSON(*)
                        FROM "{RESULT_WRAPPER_SCHEMA}"."SAMPLE_REPORT"
                        ORDER BY "_id"
                        '''
                    ).fetchall()
                ]
            finally:
                try:
                    con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
                except Exception:
                    pass
                con.close()

            assert_equal(
                wrapper_rows,
                [
                    ("1", "Alice", "explicit-null", "A", "y"),
                    ("2", "Bob", "missing", "NULL", "NULL"),
                    ("3", "Carol", "value", "NULL", "NULL"),
                ],
                "structured result wrapper query",
            )
            assert_equal(
                json_rows,
                [
                    {
                        "name": "Alice",
                        "note_state": "explicit-null",
                        "sample_id": 1,
                        "summary": {"last_tag": "y", "team": "A"},
                        "tags": ["x", "y"],
                    },
                    {
                        "name": "Bob",
                        "note_state": "missing",
                        "sample_id": 2,
                        "summary": {},
                        "tags": [],
                    },
                    {
                        "name": "Carol",
                        "note_state": "value",
                        "sample_id": 3,
                        "summary": {},
                        "tags": [],
                    },
                ],
                "quickstart structured-result TO_JSON rows",
            )
        finally:
            con = connect()
            try:
                if result_package_config is not None:
                    cleanup_package_schemas(con, result_package_config)
                else:
                    con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_SOURCE_SCHEMA}" CASCADE')
                    con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_WRAPPER_SCHEMA}" CASCADE')
                    con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_HELPER_SCHEMA}" CASCADE')
                    con.execute(f'DROP SCHEMA IF EXISTS "{RESULT_PP_SCHEMA}" CASCADE')
                if workflow_package_config is not None:
                    cleanup_package_schemas(con, workflow_package_config)
                cleanup_named_workflow_schemas(con, WORKFLOW_NAME)
            finally:
                con.close()

    print("-- quickstart structured-result regression --")
    print("verified generated wrapper -> structured result -> TO_JSON(*) flow")


if __name__ == "__main__":
    main()

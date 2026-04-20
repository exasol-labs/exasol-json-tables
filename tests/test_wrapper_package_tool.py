#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import _bootstrap  # noqa: F401

from _fixture_expected_json import sample_fixture_documents
from generate_json_export_views_sql import json_export_view_name
from nano_support import ROOT, connect, install_source_fixture


PACKAGE_DIR = ROOT / "dist" / "wrapper_package_tool_test"
PACKAGE_NAME = "json_wrapper_pkg"
PACKAGE_CONFIG_PATH = PACKAGE_DIR / f"{PACKAGE_NAME}_package.json"
REGENERATED_PREPROCESSOR_PATH = PACKAGE_DIR / f"{PACKAGE_NAME}_preprocessor_regenerated.sql"
WRAPPER_SCHEMA = "JSON_VIEW_PKG"
HELPER_SCHEMA = "JSON_VIEW_PKG_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_WRAP_PKG_PP"
PREPROCESSOR_SCRIPT = "JSON_WRAPPER_PKG_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def project_top_level(rows: list[dict[str, object]], keys: list[str]) -> list[dict[str, object]]:
    projected: list[dict[str, object]] = []
    for row in rows:
        projected_row: dict[str, object] = {}
        for key in keys:
            if key in row:
                projected_row[key] = row[key]
        projected.append(projected_row)
    return projected


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
    finally:
        con.close()

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "generate",
            "--source-schema",
            "JVS_SRC",
            "--wrapper-schema",
            WRAPPER_SCHEMA,
            "--helper-schema",
            HELPER_SCHEMA,
            "--preprocessor-schema",
            PREPROCESSOR_SCHEMA,
            "--preprocessor-script",
            PREPROCESSOR_SCRIPT,
            "--output-dir",
            str(PACKAGE_DIR),
            "--package-name",
            PACKAGE_NAME,
        ],
        check=True,
    )

    package_config = json.loads(PACKAGE_CONFIG_PATH.read_text())
    assert_equal(package_config["wrapperSchema"], WRAPPER_SCHEMA, "package config wrapper schema")
    assert_equal(package_config["helperSchema"], HELPER_SCHEMA, "package config helper schema")
    assert_equal(
        package_config["helperProfile"]["explicitNullFunctionNames"],
        ["JSON_IS_EXPLICIT_NULL", "JNULL"],
        "package config explicit-null helper profile",
    )
    assert_equal(
        package_config["helperProfile"]["variantTypeofFunctionNames"],
        ["JSON_TYPEOF"],
        "package config variant typeof helper profile",
    )
    assert_equal(
        package_config["helperProfile"]["toJsonFunctionNames"],
        ["TO_JSON"],
        "package config TO_JSON helper profile",
    )
    assert_equal(
        package_config["preprocessor"]["libraryScript"],
        "JVS_PREPROCESSOR_LIB",
        "package config preprocessor library script",
    )
    assert_equal(
        package_config["generatedFiles"]["viewsSql"],
        f"{PACKAGE_NAME}_views.sql",
        "package config relative views path",
    )
    assert_equal(
        package_config["generatedFiles"]["preprocessorLibrarySql"],
        f"{PACKAGE_NAME}_preprocessor_library.sql",
        "package config relative preprocessor library path",
    )

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "regenerate-preprocessor",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
            "--output",
            str(REGENERATED_PREPROCESSOR_PATH),
        ],
        check=True,
    )

    original_preprocessor_sql = (PACKAGE_DIR / package_config["generatedFiles"]["preprocessorSql"]).read_text()
    original_preprocessor_library_sql = (
        PACKAGE_DIR / package_config["generatedFiles"]["preprocessorLibrarySql"]
    ).read_text()
    regenerated_preprocessor_sql = REGENERATED_PREPROCESSOR_PATH.read_text()
    assert_equal(
        regenerated_preprocessor_sql,
        original_preprocessor_sql,
        "targeted preprocessor regeneration",
    )
    if "exa.import" not in original_preprocessor_sql:
        raise AssertionError("generated wrapper preprocessor should import the shared preprocessor library")
    if "function rewrite(sqltext, config)" not in original_preprocessor_library_sql:
        raise AssertionError("generated preprocessor library should expose the shared rewrite entrypoint")

    install_result = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    install_stdout = install_result.stdout
    if "Next steps:" not in install_stdout:
        raise AssertionError("install output should include a next-step heading")
    if f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{PREPROCESSOR_SCHEMA}"."{PREPROCESSOR_SCRIPT}";' not in install_stdout:
        raise AssertionError("install output should include an activation snippet")
    if f'FROM "{WRAPPER_SCHEMA}"."DEEPDOC"' not in install_stdout:
        raise AssertionError("install output should include a smoke-test query against the wrapper schema")
    if 'JSON_AS_VARCHAR("title")' not in install_stdout:
        raise AssertionError("install output should prefer a visible helper-based smoke-test field")
    if 'AS "sample_id"' not in install_stdout or 'AS "sample_value"' not in install_stdout:
        raise AssertionError("install output should show contextual smoke-test columns")

    activate_result = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
            "--activate-session",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    activate_stdout = activate_result.stdout
    if "Activated preprocessor in the installer session and ran the smoke test." not in activate_stdout:
        raise AssertionError("install --activate-session should confirm activation and smoke test execution")
    if "Activation note: this activation is session-local" not in activate_stdout:
        raise AssertionError("install --activate-session should explain the session-local activation scope")
    if "Smoke test rows:" not in activate_stdout:
        raise AssertionError("install --activate-session should print smoke test rows")
    if "deep-alpha" not in activate_stdout:
        raise AssertionError("install --activate-session should surface a visible non-NULL smoke-test value")

    validate_result = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "validate",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
            "--check-installed",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    validate_stdout = validate_result.stdout
    if "Activation reminder:" not in validate_stdout:
        raise AssertionError("validate --check-installed should print an activation reminder")
    if "Validated installed query probes: rowset, qualified-helper, TO_JSON(*)" not in validate_stdout:
        raise AssertionError("validate --check-installed should report the installed query probes it executed")
    if f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{PREPROCESSOR_SCHEMA}"."{PREPROCESSOR_SCRIPT}";' not in validate_stdout:
        raise AssertionError("validate --check-installed should print an activation snippet")
    if f'FROM "{WRAPPER_SCHEMA}"."DEEPDOC"' not in validate_stdout or 'JSON_AS_VARCHAR("title")' not in validate_stdout:
        raise AssertionError("validate --check-installed should print the high-signal smoke-test query")

    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}")
        sample_expected = sample_fixture_documents()
        rows = con.execute(
            f"""
            SELECT
              CAST("id" AS VARCHAR(10)),
              CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
              COALESCE(JSON_TYPEOF("value"), 'MISSING'),
              COALESCE(JSON_AS_VARCHAR("value"), 'NULL'),
              COALESCE(CAST(JSON_AS_DECIMAL("value") AS VARCHAR(60)), 'NULL')
            FROM {WRAPPER_SCHEMA}.SAMPLE
            ORDER BY "id"
            """
        ).fetchall()
        assert_equal(
            rows,
            [
                ("1", "0", "NUMBER", "42", "42"),
                ("2", "1", "STRING", "43", "43"),
                ("3", "0", "NULL", "NULL", "NULL"),
            ],
            "installed wrapper package query",
        )
        to_json_rows = [
            (json.loads(row[0]), json.loads(row[1]), json.loads(row[2]), json.loads(row[3]))
            for row in con.execute(
                f"""
                SELECT
                  TO_JSON(*) AS full_json,
                  TO_JSON("id", "meta") AS subset_json,
                  TO_JSON("meta") AS meta_json,
                  TO_JSON("meta") AS meta_json_again
                FROM {WRAPPER_SCHEMA}.SAMPLE
                ORDER BY "id"
                """
            ).fetchall()
        ]
        assert_equal(
            [row[0] for row in to_json_rows],
            sample_expected,
            "installed package TO_JSON(*) rows",
        )
        assert_equal(
            [row[1] for row in to_json_rows],
            project_top_level(sample_expected, ["id", "meta"]),
            "installed package TO_JSON subset rows",
        )
        assert_equal(
            [row[2] for row in to_json_rows],
            project_top_level(sample_expected, ["meta"]),
            "installed package repeated TO_JSON rows",
        )
        assert_equal(
            [row[3] for row in to_json_rows],
            project_top_level(sample_expected, ["meta"]),
            "installed package repeated TO_JSON rows again",
        )
        regular_rows = [
            json.loads(row[0])
            for row in con.execute(
                """
                SELECT TO_JSON("id", "name")
                FROM JVS_SRC.SAMPLE
                ORDER BY "_id"
                """
            ).fetchall()
        ]
        assert_equal(
            regular_rows,
            [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}, {"id": 3, "name": "gamma"}],
            "installed package regular-table TO_JSON rows",
        )

        helper_view_names = {
            row[0]
            for row in con.execute(
                f"""
                SELECT OBJECT_NAME
                FROM SYS.EXA_ALL_OBJECTS
                WHERE ROOT_NAME = '{HELPER_SCHEMA}'
                  AND OBJECT_TYPE = 'VIEW'
                ORDER BY OBJECT_NAME
                """
            ).fetchall()
        }
        manifest_path = (PACKAGE_CONFIG_PATH.parent / package_config["generatedFiles"]["manifest"]).resolve()
        manifest = json.loads(manifest_path.read_text())
        expected_helper_views = {json_export_view_name(str(table["tableName"])) for table in manifest["tables"]}
        missing_helper_views = expected_helper_views - helper_view_names
        if missing_helper_views:
            raise AssertionError(f"installed package should materialize hidden export views; missing {sorted(missing_helper_views)}")

        helper_script_names = {
            row[0]
            for row in con.execute(
                f"""
                SELECT SCRIPT_NAME
                FROM SYS.EXA_ALL_SCRIPTS
                WHERE SCRIPT_SCHEMA = '{HELPER_SCHEMA}'
                ORDER BY SCRIPT_NAME
                """
            ).fetchall()
        }
        expected_helper_scripts = {
            "JSON_QUOTE_STRING",
            "JSON_OBJECT_FROM_FRAGMENTS",
            "JSON_ARRAY_FROM_JSON_SORTED",
            "JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS",
            "JSON_OBJECT_FROM_NAME_VALUE_PAIRS",
        }
        missing_helper_scripts = expected_helper_scripts - helper_script_names
        if missing_helper_scripts:
            raise AssertionError(
                f"installed package should materialize JSON export helper scripts; missing {sorted(missing_helper_scripts)}"
            )
    finally:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        con.close()

    print("-- wrapper package tool regression --")
    print("generated, regenerated, installed, and validated wrapper package:", PACKAGE_CONFIG_PATH)


if __name__ == "__main__":
    main()

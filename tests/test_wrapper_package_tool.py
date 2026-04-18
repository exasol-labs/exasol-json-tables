#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import _bootstrap  # noqa: F401

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
        package_config["generatedFiles"]["viewsSql"],
        f"{PACKAGE_NAME}_views.sql",
        "package config relative views path",
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
    regenerated_preprocessor_sql = REGENERATED_PREPROCESSOR_PATH.read_text()
    assert_equal(
        regenerated_preprocessor_sql,
        original_preprocessor_sql,
        "targeted preprocessor regeneration",
    )

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
    if f'ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = "{PREPROCESSOR_SCHEMA}"."{PREPROCESSOR_SCRIPT}";' not in validate_stdout:
        raise AssertionError("validate --check-installed should print an activation snippet")
    if f'FROM "{WRAPPER_SCHEMA}"."DEEPDOC"' not in validate_stdout or 'JSON_AS_VARCHAR("title")' not in validate_stdout:
        raise AssertionError("validate --check-installed should print the high-signal smoke-test query")

    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}")
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
    finally:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        con.close()

    print("-- wrapper package tool regression --")
    print("generated, regenerated, installed, and validated wrapper package:", PACKAGE_CONFIG_PATH)


if __name__ == "__main__":
    main()

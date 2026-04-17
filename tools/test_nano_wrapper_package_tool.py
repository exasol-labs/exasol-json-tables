#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from nano_support import ROOT, bundle_adapter, connect, install_virtual_schema_fixture


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
        install_virtual_schema_fixture(con, bundle_adapter(), include_deep_fixture=True)
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

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
        ],
        check=True,
    )

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "validate",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
            "--check-installed",
        ],
        check=True,
    )

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

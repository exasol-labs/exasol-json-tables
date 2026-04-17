#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from nano_support import ROOT, bundle_adapter, connect, install_preprocessor, install_virtual_schema_fixture


PACKAGE_DIR = ROOT / "dist" / "wrapper_phase6_eval"
PACKAGE_NAME = "json_wrapper_phase6"
PACKAGE_CONFIG_PATH = PACKAGE_DIR / f"{PACKAGE_NAME}_package.json"
WRAPPER_SCHEMA = "JSON_VIEW_P6"
HELPER_SCHEMA = "JSON_VIEW_P6_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_WRAP_P6_PP"
PREPROCESSOR_SCRIPT = "JSON_WRAPPER_PHASE6_PREPROCESSOR"
UDF_SCHEMA = "JVS_PHASE6_UDF"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_true(condition: bool, label: str) -> None:
    if not condition:
        raise AssertionError(label)


def run_package_tool(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python3", str(ROOT / "tools" / "wrapper_package_tool.py"), *args],
        check=True,
        capture_output=True,
        text=True,
    )


def fetch_wrapper(sql: str) -> list[tuple]:
    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}")
        return con.execute(sql).fetchall()
    finally:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        con.close()


def fetch_virtual(sql: str) -> list[tuple]:
    con = connect()
    try:
        install_preprocessor(
            con,
            function_names=["JSON_IS_EXPLICIT_NULL"],
            rewrite_path_identifiers=True,
            virtual_schemas=["JSON_VS"],
        )
        return con.execute(sql).fetchall()
    finally:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        con.close()


def column_names(schema: str, table: str) -> list[str]:
    con = connect()
    try:
        rows = con.execute(
            f"""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = '{schema}'
              AND COLUMN_TABLE = '{table}'
            ORDER BY COLUMN_ORDINAL_POSITION
            """
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()


def install_identity_udf() -> None:
    con = connect()
    try:
        con.execute(f"DROP SCHEMA IF EXISTS {UDF_SCHEMA} CASCADE")
        con.execute(f"CREATE SCHEMA {UDF_SCHEMA}")
        con.execute(
            f"""CREATE OR REPLACE LUA SCALAR SCRIPT {UDF_SCHEMA}.IDENTITY_VARCHAR(x VARCHAR(2000))
RETURNS VARCHAR(2000) AS
function run(ctx)
    return ctx.x
end
/"""
        )
    finally:
        con.close()


def setup_fixture() -> None:
    con = connect()
    try:
        install_virtual_schema_fixture(con, bundle_adapter(), include_deep_fixture=True)
    finally:
        con.close()


def generate_install_validate_package() -> tuple[dict, str]:
    run_package_tool(
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
    )
    package_config = json.loads(PACKAGE_CONFIG_PATH.read_text())
    preprocessor_path = PACKAGE_DIR / package_config["generatedFiles"]["preprocessorSql"]
    preprocessor_sql = preprocessor_path.read_text()
    run_package_tool("install", "--package-config", str(PACKAGE_CONFIG_PATH))
    run_package_tool("validate", "--package-config", str(PACKAGE_CONFIG_PATH), "--check-installed")
    return package_config, preprocessor_sql


def main() -> None:
    setup_fixture()
    install_identity_udf()

    package_config, original_preprocessor_sql = generate_install_validate_package()

    helper_variant_rows = fetch_wrapper(
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          COALESCE(JSON_TYPEOF("value"), 'MISSING'),
          COALESCE(JSON_AS_VARCHAR("value"), 'NULL'),
          COALESCE(CAST(JSON_AS_DECIMAL("value") AS VARCHAR(60)), 'NULL')
        FROM {WRAPPER_SCHEMA}.SAMPLE
        ORDER BY "id"
        """
    )
    virtual_variant_rows = fetch_virtual(
        """
        SELECT
          CAST("id" AS VARCHAR(10)),
          COALESCE(TYPEOF("value"), 'MISSING'),
          COALESCE(CAST("value" AS VARCHAR(100)), 'NULL'),
          COALESCE(CAST(CAST("value" AS DECIMAL(36,18)) AS VARCHAR(60)), 'NULL')
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
        """
    )
    assert_equal(helper_variant_rows, virtual_variant_rows, "wrapper helper variant parity")

    helper_null_rows = fetch_wrapper(
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
          CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
          CASE WHEN JSON_IS_EXPLICIT_NULL("value") THEN '1' ELSE '0' END
        FROM {WRAPPER_SCHEMA}.SAMPLE
        ORDER BY "id"
        """
    )
    virtual_null_rows = fetch_virtual(
        """
        SELECT
          CAST("id" AS VARCHAR(10)),
          CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
          CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
          CASE WHEN JSON_IS_EXPLICIT_NULL("value") THEN '1' ELSE '0' END
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
        """
    )
    assert_equal(helper_null_rows, virtual_null_rows, "wrapper explicit-null parity")

    udf_rows = fetch_wrapper(
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          COALESCE({UDF_SCHEMA}.IDENTITY_VARCHAR(JSON_AS_VARCHAR("value")), 'NULL'),
          COALESCE({UDF_SCHEMA}.IDENTITY_VARCHAR("child.value"), 'NULL')
        FROM {WRAPPER_SCHEMA}.SAMPLE
        ORDER BY "id"
        """
    )
    assert_equal(
        udf_rows,
        [("1", "42", "child-1"), ("2", "43", "NULL"), ("3", "NULL", "NULL")],
        "wrapper helper values should remain UDF-friendly",
    )

    wrapper_builtin_rows = fetch_wrapper(
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          TYPEOF("value"),
          TYPEOF("shape"),
          COALESCE(CAST("value" AS VARCHAR(100)), 'NULL'),
          COALESCE(CAST("shape" AS VARCHAR(100)), 'NULL')
        FROM {WRAPPER_SCHEMA}.SAMPLE
        ORDER BY "id"
        """
    )
    virtual_builtin_rows = fetch_virtual(
        """
        SELECT
          CAST("id" AS VARCHAR(10)),
          TYPEOF("value"),
          TYPEOF("shape"),
          COALESCE(CAST("value" AS VARCHAR(100)), 'NULL'),
          COALESCE(CAST("shape" AS VARCHAR(100)), 'NULL')
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
        """
    )
    assert_true(wrapper_builtin_rows != virtual_builtin_rows, "wrapper built-in variant behavior should differ from virtual-schema built-ins")
    assert_true(len({row[1] for row in wrapper_builtin_rows}) == 1, "wrapper built-in TYPEOF(value) should reflect one projected SQL type")
    assert_true(len({row[2] for row in wrapper_builtin_rows}) == 1, "wrapper built-in TYPEOF(shape) should reflect one projected SQL type")
    assert_true(len({row[1] for row in virtual_builtin_rows}) > 1, "virtual built-in TYPEOF(value) should vary by JSON variant")
    assert_true(len({row[2] for row in virtual_builtin_rows}) > 1, "virtual built-in TYPEOF(shape) should vary by JSON variant")

    wrapper_columns_before_refresh = column_names(WRAPPER_SCHEMA, "SAMPLE")
    assert_true("status" not in {name.lower() for name in wrapper_columns_before_refresh}, "status should not exist before refresh")

    con = connect()
    try:
        con.execute('ALTER TABLE JVS_SRC.SAMPLE ADD COLUMN "status" VARCHAR(20)')
        con.execute('UPDATE JVS_SRC.SAMPLE SET "status" = CASE "id" WHEN 1 THEN \'new\' WHEN 2 THEN \'archived\' ELSE NULL END')
    finally:
        con.close()

    wrapper_columns_stale = column_names(WRAPPER_SCHEMA, "SAMPLE")
    assert_equal(wrapper_columns_stale, wrapper_columns_before_refresh, "wrapper columns should stay stale before regeneration")

    _, refreshed_preprocessor_sql = generate_install_validate_package()

    wrapper_columns_after_refresh = column_names(WRAPPER_SCHEMA, "SAMPLE")
    assert_true("status" in {name.lower() for name in wrapper_columns_after_refresh}, "status should appear after package refresh")

    refreshed_rows = fetch_wrapper(
        f"""
        SELECT
          CAST("id" AS VARCHAR(10)),
          COALESCE("status", 'NULL'),
          CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
          COALESCE(JSON_AS_VARCHAR("value"), 'NULL')
        FROM {WRAPPER_SCHEMA}.SAMPLE
        ORDER BY "id"
        """
    )
    assert_equal(
        refreshed_rows,
        [("1", "new", "0", "42"), ("2", "archived", "1", "43"), ("3", "NULL", "0", "NULL")],
        "refreshed wrapper package query",
    )

    print("-- phase 6 wrapper evaluation --")
    print("package config:", PACKAGE_CONFIG_PATH)
    print("helper variant parity:", helper_variant_rows)
    print("explicit-null parity:", helper_null_rows)
    print("udf interoperability:", udf_rows)
    print("wrapper built-in rows:", wrapper_builtin_rows)
    print("virtual built-in rows:", virtual_builtin_rows)
    print("pre-refresh columns:", wrapper_columns_before_refresh)
    print("post-refresh columns:", wrapper_columns_after_refresh)
    print("refreshed rows:", refreshed_rows)
    print("preprocessor SQL changed after additive source DDL refresh:", refreshed_preprocessor_sql != original_preprocessor_sql)


if __name__ == "__main__":
    main()

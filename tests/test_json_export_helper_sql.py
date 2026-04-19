#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import subprocess

import _bootstrap  # noqa: F401

from generate_json_export_helper_sql import (
    JSON_ARRAY_FROM_JSON_SORTED_SCRIPT,
    JSON_OBJECT_FROM_FRAGMENTS_SCRIPT,
    JSON_OBJECT_FROM_NAME_VALUE_PAIRS_SCRIPT,
    JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS_SCRIPT,
    JSON_QUOTE_STRING_SCRIPT,
    generate_json_export_helper_sql_text,
    generate_json_export_helper_statements,
    helper_names,
    install_json_export_helpers,
)
from nano_support import ROOT, connect


SCHEMA = "JVS_JSON_EXPORT_HELPER_TEST"
OUTPUT_PATH = ROOT / "dist" / "json_export_helpers_test.sql"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_contains(text: str, expected: str, label: str) -> None:
    if expected not in text:
        raise AssertionError(f"{label} mismatch.\nExpected substring: {expected!r}\nActual: {text}")


def main() -> None:
    statements = generate_json_export_helper_statements(SCHEMA)
    assert_equal(len(statements), 6, "helper statement count")
    sql_text = generate_json_export_helper_sql_text(SCHEMA)
    assert_contains(sql_text, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", "create schema statement")
    assert_contains(
        sql_text,
        f"CREATE OR REPLACE LUA SCALAR SCRIPT {SCHEMA}.{JSON_QUOTE_STRING_SCRIPT}",
        "quote-string helper SQL",
    )
    assert_contains(
        sql_text,
        f"CREATE OR REPLACE LUA SET SCRIPT {SCHEMA}.{JSON_OBJECT_FROM_FRAGMENTS_SCRIPT}",
        "object-fragments helper SQL",
    )
    assert_contains(
        sql_text,
        f"CREATE OR REPLACE LUA SET SCRIPT {SCHEMA}.{JSON_ARRAY_FROM_JSON_SORTED_SCRIPT}",
        "array-sorted helper SQL",
    )
    assert_contains(
        sql_text,
        f"CREATE OR REPLACE LUA SCALAR SCRIPT {SCHEMA}.{JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS_SCRIPT}",
        "optional-fragments helper SQL",
    )
    assert_contains(
        sql_text,
        f"CREATE OR REPLACE LUA SCALAR SCRIPT {SCHEMA}.{JSON_OBJECT_FROM_NAME_VALUE_PAIRS_SCRIPT}",
        "name-value helper SQL",
    )

    names = helper_names(SCHEMA)
    assert_equal(names.schema, SCHEMA, "helper schema name")
    assert_equal(
        names.json_quote_string,
        f'"{SCHEMA}"."{JSON_QUOTE_STRING_SCRIPT}"',
        "qualified quote-string helper name",
    )

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "generate_json_export_helper_sql.py"),
            "--schema",
            SCHEMA,
            "--output",
            str(OUTPUT_PATH),
        ],
        check=True,
    )
    assert_equal(OUTPUT_PATH.read_text(), sql_text, "tool-generated helper SQL")

    con = connect()
    try:
        con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        install_json_export_helpers(con, SCHEMA)
        install_json_export_helpers(con, SCHEMA)

        installed_objects = con.execute(
            f"""
            SELECT OBJECT_NAME, OBJECT_TYPE
            FROM SYS.EXA_ALL_OBJECTS
            WHERE ROOT_NAME = '{SCHEMA}'
            ORDER BY OBJECT_NAME
            """
        ).fetchall()
        assert_equal(
            installed_objects,
            [
                (JSON_ARRAY_FROM_JSON_SORTED_SCRIPT, "SCRIPT"),
                (JSON_OBJECT_FROM_FRAGMENTS_SCRIPT, "SCRIPT"),
                (JSON_OBJECT_FROM_NAME_VALUE_PAIRS_SCRIPT, "SCRIPT"),
                (JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS_SCRIPT, "SCRIPT"),
                (JSON_QUOTE_STRING_SCRIPT, "SCRIPT"),
            ],
            "installed helper objects",
        )

        escaped = con.execute(
            f"""
            SELECT {names.json_quote_string}('line "one"' || CHR(10) || 'tab' || CHR(9) || 'slash\\') AS j
            FROM DUAL
            """
        ).fetchall()[0][0]
        assert_equal(json.loads(escaped), 'line "one"\ntab\tslash\\', "JSON_QUOTE_STRING behavior")

        object_json = con.execute(
            f"""
            SELECT {names.json_object_from_fragments}(ord, frag) AS j
            FROM (
                SELECT CAST(2 AS DECIMAL(18,0)) AS ord, '"name":"alpha"' AS frag FROM DUAL
                UNION ALL
                SELECT CAST(1 AS DECIMAL(18,0)), '"id":1' FROM DUAL
            ) t
            """
        ).fetchall()[0][0]
        assert_equal(json.loads(object_json), {"id": 1, "name": "alpha"}, "JSON_OBJECT_FROM_FRAGMENTS behavior")

        array_json = con.execute(
            f"""
            SELECT {names.json_array_from_json_sorted}(pos, child_json) AS j
            FROM (
                SELECT CAST(2 AS DECIMAL(18,0)) AS pos, '"tail"' AS child_json FROM DUAL
                UNION ALL
                SELECT CAST(0 AS DECIMAL(18,0)), '"head"' FROM DUAL
                UNION ALL
                SELECT CAST(1 AS DECIMAL(18,0)), '"mid"' FROM DUAL
            ) t
            """
        ).fetchall()[0][0]
        assert_equal(json.loads(array_json), ["head", "mid", "tail"], "JSON_ARRAY_FROM_JSON_SORTED behavior")

        subset_json = con.execute(
            f"""
            SELECT {names.json_object_from_optional_fragments}(NULL, '"id":1', NULL, '"name":"alpha"') AS j
            FROM DUAL
            """
        ).fetchall()[0][0]
        assert_equal(
            json.loads(subset_json),
            {"id": 1, "name": "alpha"},
            "JSON_OBJECT_FROM_OPTIONAL_FRAGMENTS behavior",
        )

        row_object_json = con.execute(
            f"""
            SELECT {names.json_object_from_name_value_pairs}(
                'id', CAST(1 AS DECIMAL(18,0)),
                'name', 'alpha',
                'enabled', TRUE,
                'missing', CAST(NULL AS DECIMAL(18,0))
            ) AS j
            FROM DUAL
            """
        ).fetchall()[0][0]
        assert_equal(
            json.loads(row_object_json),
            {"id": 1, "name": "alpha", "enabled": True, "missing": None},
            "JSON_OBJECT_FROM_NAME_VALUE_PAIRS behavior",
        )
    finally:
        con.close()

    print("-- json export helper SQL regression --")
    print("generated, installed, and validated helper layer:", Path(OUTPUT_PATH))


if __name__ == "__main__":
    main()

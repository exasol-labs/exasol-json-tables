#!/usr/bin/env python3

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_EARLY_OUT_PP"
PREPROCESSOR_SCRIPT = "JSON_EARLY_OUT_PREPROCESSOR"
REGULAR_TABLE = "REGULAR_ROWS"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def fetch_json_rows(con, sql: str) -> list[object]:
    return [json.loads(row[0]) for row in con.execute(sql).fetchall()]


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=False)
        install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
        )
        install_wrapper_preprocessor(
            con,
            [WRAPPER_SCHEMA],
            [HELPER_SCHEMA],
            schema_name=PREPROCESSOR_SCHEMA,
            script_name=PREPROCESSOR_SCRIPT,
        )
        con.execute(
            f'''
            CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.{REGULAR_TABLE} (
              "id" DECIMAL(18,0),
              "name" VARCHAR(100),
              "active" BOOLEAN
            )
            '''
        )
        con.execute(
            f"""
            INSERT INTO {SOURCE_SCHEMA}.{REGULAR_TABLE} VALUES
              (1, 'alpha', TRUE),
              (2, 'beta', FALSE),
              (3, 'gamma', NULL)
            """
        )

        pass_through_rows = con.execute("SELECT 'plain-sql-path'").fetchall()
        assert_equal(pass_through_rows, [("plain-sql-path",)], "unrelated SQL pass-through")

        quoted_source_rows = con.execute(
            f'SELECT CAST("id" AS VARCHAR(10)) FROM "{SOURCE_SCHEMA}"."SAMPLE" WHERE "id" IN (1, 2) ORDER BY "id"'
        ).fetchall()
        assert_equal(quoted_source_rows, [("1",), ("2",)], "quoted source query after early-out")

        wrapper_plain_rows = con.execute(
            f'SELECT CAST("id" AS VARCHAR(10)) FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"'
        ).fetchall()
        assert_equal(wrapper_plain_rows, [("1",), ("2",), ("3",)], "plain wrapper query after early-out")

        helper_rows = con.execute(
            f"""
            SELECT COALESCE(JSON_AS_VARCHAR(s."name"), 'NULL')
            FROM {WRAPPER_SCHEMA}.SAMPLE s
            ORDER BY s."_id"
            """
        ).fetchall()
        assert_equal(helper_rows, [("alpha",), ("beta",), ("gamma",)], "helper-name fast path rewrite")

        dotted_path_rows = con.execute(
            f"""
            SELECT COALESCE("meta.info.note", 'NULL')
            FROM "{WRAPPER_SCHEMA}"."SAMPLE"
            ORDER BY "id"
            """
        ).fetchall()
        assert_equal(dotted_path_rows, [("deep",), ("NULL",), ("NULL",)], "quoted dotted path rewrite")

        bracket_path_rows = con.execute(
            f"""
            SELECT COALESCE("items[LAST].value", 'NULL')
            FROM "{WRAPPER_SCHEMA}"."SAMPLE"
            ORDER BY "id"
            """
        ).fetchall()
        assert_equal(bracket_path_rows, [("second",), ("only",), ("NULL",)], "quoted bracket path rewrite")

        correlated_rowset_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10))
            FROM {WRAPPER_SCHEMA}.SAMPLE s
            WHERE EXISTS (
              SELECT 1
              FROM item IN s."items"
              WHERE item.label = 'B' AND item.value = 'second'
            )
            ORDER BY s."id"
            """
        ).fetchall()
        assert_equal(correlated_rowset_rows, [("1",)], "correlated rowset after early-out")

        regular_table_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} ORDER BY "id"',
        )
        assert_equal(
            regular_table_rows,
            [
                {"active": True, "id": 1, "name": "alpha"},
                {"active": False, "id": 2, "name": "beta"},
                {"active": None, "id": 3, "name": "gamma"},
            ],
            "regular-table TO_JSON after early-out",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
    print("-- preprocessor early-out regression --")
    print(
        "validated helper-name, dotted-path, bracket-path, iterator-rowset, and regular-table TO_JSON handling "
        "under stage-0 detection"
    )

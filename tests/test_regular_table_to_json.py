#!/usr/bin/env python3

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_REGULAR_TO_JSON_PP"
PREPROCESSOR_SCRIPT = "JSON_REGULAR_TO_JSON_PREPROCESSOR"
REGULAR_TABLE = "REGULAR_ROWS"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_contains(text: str, expected: str, label: str) -> None:
    if expected not in text:
        raise AssertionError(f"{label} mismatch.\nExpected substring: {expected!r}\nActual: {text}")


def fetch_json_rows(con, sql: str) -> list[object]:
    return [json.loads(row[0]) for row in con.execute(sql).fetchall()]


def fetch_error_text(con, sql: str) -> str:
    try:
        con.execute(sql).fetchall()
    except Exception as exc:
        return str(exc)
    raise AssertionError(f"Expected query to fail: {sql}")


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

        expected_full_rows = [
            {"active": True, "id": 1, "name": "alpha"},
            {"active": False, "id": 2, "name": "beta"},
            {"active": None, "id": 3, "name": "gamma"},
        ]
        expected_subset_rows = [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]

        full_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} ORDER BY "id"',
        )
        con.execute(f"OPEN SCHEMA {SOURCE_SCHEMA}")
        unqualified_full_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {REGULAR_TABLE} ORDER BY "id"',
        )
        alias_star_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(s.*) FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} s ORDER BY s."id"',
        )
        subset_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("id", "name") FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} ORDER BY "id"',
        )
        joined_subset_rows = fetch_json_rows(
            con,
            f'''
            SELECT TO_JSON(s."id", s."name")
            FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} s
            JOIN {SOURCE_SCHEMA}.{REGULAR_TABLE} peer
              ON s."id" = peer."id"
            ORDER BY s."id"
            ''',
        )
        joined_star_rows = fetch_json_rows(
            con,
            f'''
            SELECT TO_JSON(s.*)
            FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} s
            JOIN {SOURCE_SCHEMA}.{REGULAR_TABLE} peer
              ON s."id" = peer."id"
            ORDER BY s."id"
            ''',
        )

        assert_equal(full_rows, expected_full_rows, "regular-table TO_JSON(*) rows")
        assert_equal(unqualified_full_rows, expected_full_rows, "unqualified regular-table TO_JSON(*) rows")
        assert_equal(alias_star_rows, expected_full_rows, "regular-table TO_JSON(alias.*) rows")
        assert_equal(subset_rows, expected_subset_rows, "regular-table TO_JSON subset rows")
        assert_equal(joined_subset_rows, expected_subset_rows, "joined regular-table TO_JSON subset rows")
        assert_equal(joined_star_rows, expected_full_rows, "joined regular-table TO_JSON(alias.*) rows")

        joined_plain_star_error = fetch_error_text(
            con,
            f'''
            SELECT TO_JSON(*)
            FROM {SOURCE_SCHEMA}.{REGULAR_TABLE} s
            JOIN {SOURCE_SCHEMA}.{REGULAR_TABLE} peer
              ON s."id" = peer."id"
            ORDER BY s."id"
            ''',
        )
        derived_source_error = fetch_error_text(
            con,
            f'''
            SELECT TO_JSON(*)
            FROM (
              SELECT *
              FROM {SOURCE_SCHEMA}.{REGULAR_TABLE}
            ) s
            ORDER BY "id"
            ''',
        )
        raw_source_star_error = fetch_error_text(
            con,
            f'SELECT TO_JSON(*) FROM {SOURCE_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        raw_source_alias_star_error = fetch_error_text(
            con,
            f'SELECT TO_JSON(s.*) FROM {SOURCE_SCHEMA}.SAMPLE s ORDER BY s."_id"',
        )

        assert_contains(
            joined_plain_star_error,
            "TO_JSON(*) is not supported in joined queries.",
            "joined regular-table plain-star error",
        )
        assert_contains(
            derived_source_error,
            "TO_JSON does not resolve through derived-table aliases yet.",
            "derived-source regular-table TO_JSON error",
        )
        assert_contains(
            raw_source_star_error,
            "TO_JSON(*) on source-family tables would expose internal contract columns.",
            "raw source plain-star error",
        )
        assert_contains(
            raw_source_alias_star_error,
            "TO_JSON(*) on source-family tables would expose internal contract columns.",
            "raw source alias-star error",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

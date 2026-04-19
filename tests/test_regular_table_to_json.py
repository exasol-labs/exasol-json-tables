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

        expected_full_rows = [
            {
                "_id": 1,
                "child|n": False,
                "child|object": 1,
                "id": 1,
                "items|array": 2,
                "meta|object": 10,
                "name": "alpha",
                "note": "x",
                "note|n": False,
                "shape|array": None,
                "shape|object": 10,
                "tags|array": 2,
                "value": 42,
                "value|n": False,
                "value|string": None,
            },
            {
                "_id": 2,
                "child|n": False,
                "child|object": None,
                "id": 2,
                "items|array": 1,
                "meta|object": 20,
                "name": "beta",
                "note": None,
                "note|n": True,
                "shape|array": 3,
                "shape|object": None,
                "tags|array": 1,
                "value": None,
                "value|n": False,
                "value|string": "43",
            },
            {
                "_id": 3,
                "child|n": True,
                "child|object": None,
                "id": 3,
                "items|array": None,
                "meta|object": None,
                "name": "gamma",
                "note": None,
                "note|n": False,
                "shape|array": None,
                "shape|object": None,
                "tags|array": None,
                "value": None,
                "value|n": True,
                "value|string": None,
            },
        ]
        expected_subset_rows = [
            {"id": 1, "name": "alpha"},
            {"id": 2, "name": "beta"},
            {"id": 3, "name": "gamma"},
        ]

        full_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(*) FROM {SOURCE_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        con.execute(f"OPEN SCHEMA {SOURCE_SCHEMA}")
        unqualified_full_rows = fetch_json_rows(
            con,
            'SELECT TO_JSON(*) FROM SAMPLE ORDER BY "_id"',
        )
        alias_star_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON(s.*) FROM {SOURCE_SCHEMA}.SAMPLE s ORDER BY s."_id"',
        )
        subset_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("id", "name") FROM {SOURCE_SCHEMA}.SAMPLE ORDER BY "_id"',
        )
        joined_subset_rows = fetch_json_rows(
            con,
            f'''
            SELECT TO_JSON(s."id", s."name")
            FROM {SOURCE_SCHEMA}.SAMPLE s
            JOIN {SOURCE_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
            ''',
        )
        joined_star_rows = fetch_json_rows(
            con,
            f'''
            SELECT TO_JSON(s.*)
            FROM {SOURCE_SCHEMA}.SAMPLE s
            JOIN {SOURCE_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
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
            FROM {SOURCE_SCHEMA}.SAMPLE s
            JOIN {SOURCE_SCHEMA}.SAMPLE peer
              ON s."_id" = peer."_id"
            ORDER BY s."_id"
            ''',
        )
        derived_source_error = fetch_error_text(
            con,
            f'''
            SELECT TO_JSON(*)
            FROM (
              SELECT *
              FROM {SOURCE_SCHEMA}.SAMPLE
            ) s
            ORDER BY "_id"
            ''',
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
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

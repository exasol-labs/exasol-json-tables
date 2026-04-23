#!/usr/bin/env python3

from __future__ import annotations

import _bootstrap  # noqa: F401

from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_USER_STUDY_PP"
PREPROCESSOR_SCRIPT = "JSON_USER_STUDY_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_contains(text: str, expected: str, label: str) -> None:
    if expected not in text:
        raise AssertionError(f"{label} mismatch.\nExpected substring: {expected!r}\nActual: {text}")


def fetch_all(con, sql: str) -> list[tuple]:
    return con.execute(sql).fetchall()


def fetch_error(con, sql: str) -> str:
    try:
        con.execute(sql).fetchall()
    except Exception as exc:
        return str(exc)
    raise AssertionError(f"Expected query to fail: {sql}")


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
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

        prepared_selector_con = connect()
        try:
            install_wrapper_preprocessor(
                prepared_selector_con,
                [WRAPPER_SCHEMA],
                [HELPER_SCHEMA],
                schema_name=PREPROCESSOR_SCHEMA,
                script_name=PREPROCESSOR_SCRIPT,
            )
            prepared_stmt = prepared_selector_con.create_prepared_statement(
                """
                SELECT
                  CAST("id" AS VARCHAR(10)) AS doc_id,
                  COALESCE("tags[PARAM]", 'NULL') AS tag_by_param
                FROM JSON_VIEW.SAMPLE
                ORDER BY "id"
                """,
            )
            prepared_stmt.execute_prepared([(1,)])
            prepared_selector_rows = prepared_stmt.fetchall()
        finally:
            prepared_selector_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
            prepared_selector_con.close()
        assert_equal(
            prepared_selector_rows,
            [("1", "blue"), ("2", "NULL"), ("3", "NULL")],
            "BUG-002 PARAM selector syntax",
        )

        duplicate_name_rows = fetch_all(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              "meta.info.note",
              "items[LAST].nested.note"
            FROM JSON_VIEW.SAMPLE
            ORDER BY "id"
            """,
        )
        assert_equal(
            duplicate_name_rows,
            [("1", "deep", "nested-b"), ("2", None, None), ("3", None, None)],
            "BUG-003 duplicate path output names",
        )

        iterator_array_error = fetch_error(
            con,
            """
            SELECT s."id", item."nested.items[LAST]" AS last_nested_item
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items"
            ORDER BY s."id", item._index
            """,
        )
        assert_contains(
            iterator_array_error,
            "Bracket access on object-array elements requires a trailing property",
            "BUG-004 iterator object-array bracket error",
        )

        aggregate_rows = fetch_all(
            con,
            'SELECT CAST(COUNT(DISTINCT "child.value") AS VARCHAR(10)) AS cnt FROM JSON_VIEW.SAMPLE',
        )
        assert_equal(aggregate_rows, [("1",)], "BUG-005 aggregate path rewrite")

        to_json_bracket_error = fetch_error(
            con,
            'SELECT TO_JSON("id", "tags[SIZE]") FROM JSON_VIEW.SAMPLE ORDER BY "_id"',
        )
        assert_contains(
            to_json_bracket_error,
            'bracket expressions such as "tags[SIZE]" are not supported',
            "BUG-006 TO_JSON bracket argument error",
        )

        selector_type_error = fetch_error(
            con,
            'SELECT "id", "items[child]" FROM JSON_VIEW.SAMPLE ORDER BY "id"',
        )
        assert_contains(
            selector_type_error,
            'Array selector "child" resolves to a nested object/array reference',
            "BUG-010 selector type error",
        )

        method_iterator_alias_error = fetch_error(
            con,
            """
            SELECT CAST(s."id" AS VARCHAR(10)), method
            FROM JSON_VIEW.SAMPLE s
            JOIN VALUE method IN s."tags"
            ORDER BY 1, 2
            """,
        )
        assert_contains(
            method_iterator_alias_error,
            "METHOD_",
            "BUG-012 method iterator alias rewrite",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

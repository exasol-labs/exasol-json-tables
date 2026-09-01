#!/usr/bin/env python3

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from personal_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


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
        con.execute(
            f'''
            CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.BUG063_OPTIONAL_STRINGS (
              "_id" DECIMAL(18,0) NOT NULL,
              "k" DECIMAL(10,0),
              "note" VARCHAR(200),
              "note|empty" BOOLEAN,
              "note|n" BOOLEAN
            )
            '''
        )
        con.execute(
            f'''
            INSERT INTO {SOURCE_SCHEMA}.BUG063_OPTIONAL_STRINGS VALUES
              (1, 1, 'has value', FALSE, FALSE),
              (2, 2, NULL, FALSE, TRUE),
              (3, 3, NULL, FALSE, FALSE),
              (4, 4, NULL, TRUE, FALSE)
            '''
        )
        con.execute(
            f'''
            CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.BUG063_EMPTY_MASK_ONLY (
              "_id" DECIMAL(18,0) NOT NULL,
              "k" DECIMAL(10,0),
              "note|empty" BOOLEAN
            )
            '''
        )
        con.execute(
            f'''
            INSERT INTO {SOURCE_SCHEMA}.BUG063_EMPTY_MASK_ONLY VALUES
              (1, 1, TRUE),
              (2, 2, FALSE)
            '''
        )
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

        optional_string_rows = fetch_all(
            con,
            f'''
            SELECT "k", "note"
            FROM {WRAPPER_SCHEMA}.BUG063_OPTIONAL_STRINGS
            ORDER BY "k"
            ''',
        )
        assert_equal(
            optional_string_rows,
            [(1, "has value"), (2, None), (3, None), (4, None)],
            "BUG-063 empty-string mask excluded from logical value",
        )
        assert_equal(
            fetch_all(
                con,
                f'SELECT COUNT(*) FROM {WRAPPER_SCHEMA}.BUG063_OPTIONAL_STRINGS WHERE "note" IS NULL',
            ),
            [(3,)],
            "BUG-063 optional-string null filtering",
        )

        optional_string_json_rows = fetch_all(
            con,
            f'''
            SELECT TO_JSON(*)
            FROM {WRAPPER_SCHEMA}.BUG063_OPTIONAL_STRINGS
            ORDER BY "k"
            ''',
        )
        expected_optional_string_documents = [
            {"k": 1, "note": "has value"},
            {"k": 2, "note": None},
            {"k": 3},
            {"k": 4, "note": ""},
        ]
        assert_equal(
            [json.loads(row[0]) for row in optional_string_json_rows],
            expected_optional_string_documents,
            "BUG-063 optional-string TO_JSON semantics",
        )
        for row in optional_string_json_rows:
            if row[0].count('"note":') > 1:
                raise AssertionError(f"BUG-063 duplicate note key in TO_JSON output: {row[0]}")

        assert_equal(
            fetch_all(
                con,
                f'''
                SELECT JSON_TYPEOF("note")
                FROM {WRAPPER_SCHEMA}.BUG063_OPTIONAL_STRINGS
                ORDER BY "k"
                ''',
            ),
            [("STRING",), ("NULL",), (None,), ("STRING",)],
            "BUG-063 empty-string JSON type",
        )

        empty_mask_only_json_rows = fetch_all(
            con,
            f'''
            SELECT TO_JSON(*), TO_JSON("note")
            FROM {WRAPPER_SCHEMA}.BUG063_EMPTY_MASK_ONLY
            ORDER BY "k"
            ''',
        )
        assert_equal(
            [(json.loads(full_row), json.loads(selected)) for full_row, selected in empty_mask_only_json_rows],
            [
                ({"k": 1, "note": ""}, {"note": ""}),
                ({"k": 2}, {}),
            ],
            "BUG-063 empty-mask-only TO_JSON semantics",
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

        iterator_array_rows = fetch_all(
            con,
            """
            SELECT s."id", item."nested.items[LAST]" AS last_nested_item
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items"
            ORDER BY s."id", item._index
            """,
        )
        assert_equal(
            iterator_array_rows,
            [(1, "na-2"), (1, "nb-1"), (2, None)],
            "BUG-004 iterator object-array bracket path",
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

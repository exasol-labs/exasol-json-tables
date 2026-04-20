#!/usr/bin/env python3

from __future__ import annotations

import json

import _bootstrap  # noqa: F401

from _fixture_expected_json import sample_fixture_documents
from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_SCHEMA = "JSON_VIEW_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_REWRITE_GATE_PP"
PREPROCESSOR_SCRIPT = "JSON_REWRITE_GATE_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def fetch_json_rows(con, sql: str) -> list[object]:
    return [json.loads(row[0]) for row in con.execute(sql).fetchall()]


def project_top_level(rows: list[dict[str, object]], keys: list[str]) -> list[dict[str, object]]:
    projected: list[dict[str, object]] = []
    for row in rows:
        projected_row: dict[str, object] = {}
        for key in keys:
            if key in row:
                projected_row[key] = row[key]
        projected.append(projected_row)
    return projected


def flatten_array_property(rows: list[dict[str, object]], row_key: str, array_key: str) -> list[tuple[object, dict[str, object]]]:
    flattened: list[tuple[object, dict[str, object]]] = []
    for row in rows:
        row_identifier = row[row_key]
        array_value = row.get(array_key)
        if not isinstance(array_value, list):
            continue
        for element in array_value:
            if isinstance(element, dict):
                flattened.append((row_identifier, element))
    return flattened


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

        sample_expected = sample_fixture_documents()

        helper_only_rows = con.execute(
            f'SELECT JSON_TYPEOF("value") FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"'
        ).fetchall()
        path_only_rows = con.execute(
            f'SELECT COALESCE("meta.info.note", \'NULL\') FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"'
        ).fetchall()
        path_and_helper_rows = con.execute(
            f'SELECT COALESCE(JSON_AS_VARCHAR("meta.info.note"), \'NULL\') FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "id"'
        ).fetchall()
        rowset_only_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10)), item._index
            FROM "{WRAPPER_SCHEMA}"."SAMPLE" s
            JOIN item IN s."items"
            ORDER BY s."_id", item._index
            """
        ).fetchall()
        rowset_exists_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10))
            FROM "{WRAPPER_SCHEMA}"."SAMPLE" s
            WHERE EXISTS (
              SELECT 1
              FROM item IN s."items"
              WHERE item.label = 'B' AND item.value = 'second'
            )
            ORDER BY s."id"
            """
        ).fetchall()
        iterator_path_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10)), COALESCE(item."nested.note", 'NULL')
            FROM "{WRAPPER_SCHEMA}"."SAMPLE" s
            JOIN item IN s."items"
            ORDER BY s."_id", item._index
            """
        ).fetchall()
        iterator_helper_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10)), JSON_TYPEOF(item."value")
            FROM "{WRAPPER_SCHEMA}"."SAMPLE" s
            JOIN item IN s."items"
            ORDER BY s."_id", item._index
            """
        ).fetchall()
        iterator_path_and_helper_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10)), COALESCE(JSON_AS_VARCHAR(item."nested.note"), 'NULL')
            FROM "{WRAPPER_SCHEMA}"."SAMPLE" s
            JOIN item IN s."items"
            ORDER BY s."_id", item._index
            """
        ).fetchall()
        to_json_root_subset_rows = fetch_json_rows(
            con,
            f'SELECT TO_JSON("meta", "items") FROM "{WRAPPER_SCHEMA}"."SAMPLE" ORDER BY "_id"',
        )
        to_json_iterator_star_rows = con.execute(
            f"""
            SELECT CAST(s."id" AS VARCHAR(10)), TO_JSON(item.*)
            FROM "{WRAPPER_SCHEMA}"."SAMPLE" s
            JOIN item IN s."items"
            ORDER BY s."_id", item._index
            """
        ).fetchall()

        assert_equal(helper_only_rows, [("NUMBER",), ("STRING",), ("NULL",)], "helper-only rewrite")
        assert_equal(path_only_rows, [("deep",), ("NULL",), ("NULL",)], "path-only rewrite")
        assert_equal(path_and_helper_rows, [("deep",), ("NULL",), ("NULL",)], "path+helper rewrite")
        assert_equal(
            rowset_only_rows,
            [("1", 0), ("1", 1), ("2", 0)],
            "rowset-only rewrite",
        )
        assert_equal(rowset_exists_rows, [("1",)], "rowset EXISTS rewrite")
        assert_equal(
            iterator_path_rows,
            [("1", "nested-a"), ("1", "nested-b"), ("2", "NULL")],
            "iterator+path rewrite",
        )
        assert_equal(
            iterator_helper_rows,
            [("1", "STRING"), ("1", "STRING"), ("2", "STRING")],
            "iterator+helper rewrite",
        )
        assert_equal(
            iterator_path_and_helper_rows,
            [("1", "nested-a"), ("1", "nested-b"), ("2", "NULL")],
            "iterator+path+helper rewrite",
        )
        assert_equal(
            to_json_root_subset_rows,
            project_top_level(sample_expected, ["meta", "items"]),
            "TO_JSON root subset rewrite",
        )
        assert_equal(
            [(row[0], json.loads(row[1])) for row in to_json_iterator_star_rows],
            [(str(sample_id), item_json) for sample_id, item_json in flatten_array_property(sample_expected, "id", "items")],
            "TO_JSON iterator-star rewrite",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()
    print("-- preprocessor rewrite hotspots regression --")
    print("validated helper-only, path-only, rowset-only, mixed iterator rewrites, and TO_JSON rewrite-heavy paths")

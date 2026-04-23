#!/usr/bin/env python3

import subprocess

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


PUBLIC_WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_WRAPPER_SCHEMA = "JSON_VIEW_INTERNAL"
DEEP_LEAF_PATH = '"chain.next.next.next.next.next.next.next.leaf_note"'
DEEP_ENTRY_LAST_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[LAST].value"'
DEEP_ENTRY_SIZE_PATH = '"chain.next.next.next.next.next.next.next.entries[SIZE]"'
DEEP_ENTRY_ARRAY_PATH = 'd."chain.next.next.next.next.next.next.next.entries"'


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def fetch_all(sql: str) -> list[tuple]:
    con = connect()
    try:
        install_wrapper_preprocessor(con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])
        return con.execute(sql).fetchall()
    finally:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        con.close()


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        manifest = install_wrapper_views(
            con,
            source_schema="JVS_SRC",
            wrapper_schema=PUBLIC_WRAPPER_SCHEMA,
            helper_schema=HELPER_WRAPPER_SCHEMA,
            generate_preprocessor=True,
        )
        install_wrapper_preprocessor(con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])

        assert_equal(manifest["publicSchema"], PUBLIC_WRAPPER_SCHEMA, "manifest public schema")
        assert_equal(manifest["helperSchema"], HELPER_WRAPPER_SCHEMA, "manifest helper schema")
        manifest_roots = sorted(root["tableName"] for root in manifest["roots"])
        assert_equal(manifest_roots, ["DEEPDOC", "SAMPLE"], "manifest root tables")

        public_tables = con.execute(f"""
            SELECT DISTINCT COLUMN_TABLE
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = '{PUBLIC_WRAPPER_SCHEMA}'
            ORDER BY COLUMN_TABLE
        """).fetchall()
        assert_equal(public_tables, [("DEEPDOC",), ("SAMPLE",)], "public wrapper tables")

        helper_tables = {name for (name,) in con.execute(f"""
            SELECT DISTINCT COLUMN_TABLE
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = '{HELPER_WRAPPER_SCHEMA}'
            ORDER BY COLUMN_TABLE
        """).fetchall()}
        required_helper_tables = {
            "SAMPLE",
            "SAMPLE_child",
            "SAMPLE_items_arr",
            "SAMPLE_items_arr_nested",
            "SAMPLE_items_arr_nested_items_arr",
            "DEEPDOC",
            "DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr",
            "__JVS_ROOTS",
            "__JVS_RELATIONSHIPS",
            "__JVS_COLUMN_MEMBERS",
        }
        missing_helper_tables = sorted(required_helper_tables - helper_tables)
        if missing_helper_tables:
            raise AssertionError(f"missing helper tables/views: {missing_helper_tables}")

        wrapper_columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VIEW' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        assert_equal(
            wrapper_columns,
            [("_id",), ("id",), ("name",), ("note",), ("child|object",), ("meta|object",), ("value",), ("shape",), ("tags|array",), ("items|array",)],
            "wrapper SAMPLE column names",
        )

        deep_wrapper_columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VIEW' AND COLUMN_TABLE = 'DEEPDOC'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        assert_equal(
            deep_wrapper_columns,
            [("_id",), ("doc_id",), ("title",), ("profile|object",), ("chain|object",), ("tags|array",), ("metrics|array",)],
            "wrapper DEEPDOC column names",
        )

        helper_relationships = con.execute(f"""
            SELECT ROOT_TABLE, PARENT_TABLE, CHILD_TABLE, SEGMENT_NAME, RELATION_KIND
            FROM {HELPER_WRAPPER_SCHEMA}."__JVS_RELATIONSHIPS"
            WHERE ROOT_TABLE IN ('SAMPLE', 'DEEPDOC')
            ORDER BY ROOT_TABLE, PARENT_TABLE, CHILD_TABLE, SEGMENT_NAME
        """).fetchall()
        expected_relationship_subset = [
            ("SAMPLE", "SAMPLE", "SAMPLE_child", "child", "object"),
            ("SAMPLE", "SAMPLE", "SAMPLE_items_arr", "items", "array"),
            ("SAMPLE", "SAMPLE", "SAMPLE_meta", "meta", "object"),
            ("DEEPDOC", "DEEPDOC", "DEEPDOC_profile", "profile", "object"),
            ("DEEPDOC", "DEEPDOC", "DEEPDOC_metrics_arr", "metrics", "array"),
        ]
        for expected_row in expected_relationship_subset:
            if expected_row not in helper_relationships:
                raise AssertionError(f"missing relationship row {expected_row!r}")
    finally:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        con.close()

    packaged_sql = (ROOT / "dist" / "json_wrapper_preprocessor_packaged_test.sql").read_text()
    if "Configured function names: JSON_IS_EXPLICIT_NULL, JNULL, JSON_TYPEOF, JSON_AS_VARCHAR, JSON_AS_DECIMAL, JSON_AS_BOOLEAN, TO_JSON" not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should enable the standard wrapper helper aliases")
    if "JSON syntax allowed only for configured JSON schemas: JSON_VIEW" not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should be scoped to the public wrapper schema")
    if "Helper rewrite mode: wrapper semantic helpers" not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should use wrapper semantic helper rewrite mode")
    if 'exa.import("JVS_WRAP_PP.JVS_PREPROCESSOR_LIB", "JVS_PREPROCESSOR_LIB")' not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should import the shared preprocessor library")
    if "\nALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = " in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should not auto-activate by default")

    activated_output = ROOT / "dist" / "json_wrapper_preprocessor_packaged_activate.sql"
    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "generate_wrapper_views_sql.py"),
            "--dsn",
            "127.0.0.1:8563",
            "--user",
            "sys",
            "--password",
            "exasol",
            "--source-schema",
            "JVS_SRC",
            "--wrapper-schema",
            PUBLIC_WRAPPER_SCHEMA,
            "--helper-schema",
            HELPER_WRAPPER_SCHEMA,
            "--output",
            str(ROOT / "dist" / "json_wrapper_views_activate_test.sql"),
            "--manifest-output",
            str(ROOT / "dist" / "json_wrapper_manifest_activate_test.json"),
            "--preprocessor-output",
            str(activated_output),
            "--activate-preprocessor-session",
        ],
        check=True,
    )
    activated_sql = activated_output.read_text()
    if "ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;" not in activated_sql:
        raise AssertionError("expected activated wrapper package output to include explicit activation SQL")

    normalized_rows = fetch_all("""
        SELECT
          CAST("_id" AS VARCHAR(20)) AS root_id,
          CAST("id" AS VARCHAR(20)) AS doc_id,
          COALESCE("name", 'NULL') AS name_value,
          COALESCE("note", 'NULL') AS note_value,
          COALESCE(CAST("child|object" AS VARCHAR(20)), 'NULL') AS child_ref,
          COALESCE(CAST("meta|object" AS VARCHAR(20)), 'NULL') AS meta_ref,
          COALESCE(CAST("value" AS VARCHAR(100)), 'NULL') AS value_text,
          COALESCE(CAST("shape" AS VARCHAR(100)), 'NULL') AS shape_text,
          COALESCE(CAST("tags|array" AS VARCHAR(20)), 'NULL') AS tags_size,
          COALESCE(CAST("items|array" AS VARCHAR(20)), 'NULL') AS items_size
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(
        normalized_rows,
        [("1", "1", "alpha", "x", "1", "10", "42", "10", "2", "2"), ("2", "2", "beta", "NULL", "NULL", "20", "43", "3", "1", "1"), ("3", "3", "gamma", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")],
        "normalized wrapper rows",
    )

    path_rows = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("child.value", 'NULL') AS child_value,
          CASE WHEN "meta.info.note" IS NULL THEN 'NULL' ELSE "meta.info.note" END AS deep_note,
          COALESCE("tags[LAST]", 'NULL') AS last_tag
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(path_rows, [("1", "child-1", "deep", "blue"), ("2", "NULL", "NULL", "green"), ("3", "NULL", "NULL", "NULL")], "path syntax")

    array_rows = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("tags[FIRST]", 'NULL') AS first_tag,
          COALESCE("tags[LAST]", 'NULL') AS last_tag,
          COALESCE(CAST("tags[SIZE]" AS VARCHAR(20)), 'NULL') AS tag_count,
          COALESCE("items[LAST].value", 'NULL') AS last_item_value,
          COALESCE("meta.items[LAST].value", 'NULL') AS last_meta_item_value
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(array_rows, [("1", "red", "blue", "2", "second", "m2"), ("2", "green", "green", "1", "only", "m3"), ("3", "NULL", "NULL", "NULL", "NULL", "NULL")], "array syntax")

    dynamic_index_rows = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("tags[id]", 'NULL') AS tag_by_id,
          COALESCE("items[id].value", 'NULL') AS item_by_id
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(
        dynamic_index_rows,
        [("1", "blue", "second"), ("2", "NULL", "NULL"), ("3", "NULL", "NULL")],
        "field-driven array selector syntax",
    )

    prepared_selector_con = connect()
    try:
        install_wrapper_preprocessor(prepared_selector_con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])
        prepared_stmt = prepared_selector_con.create_prepared_statement(
            """
            SELECT
              CAST("id" AS VARCHAR(10)) AS doc_id,
              COALESCE("tags[?]", 'NULL') AS tag_by_param
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
        "parameterized array selector syntax",
    )

    rowset_rows = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)),
          CAST(item._index AS VARCHAR(10)),
          item.value,
          item.label
        FROM JSON_VIEW.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
    """)
    assert_equal(rowset_rows, [("1", "0", "first", "A"), ("1", "1", "second", "B"), ("2", "0", "only", "C")], "rowset syntax")

    correlated_rowset_rows = fetch_all("""
        SELECT CAST(s."id" AS VARCHAR(10))
        FROM JSON_VIEW.SAMPLE s
        WHERE EXISTS (
          SELECT 1
          FROM item IN s."items"
          WHERE item.label = 'B' AND item.value = 'second'
        )
        ORDER BY s."id"
    """)
    assert_equal(correlated_rowset_rows, [("1",)], "correlated object-array rowset syntax")

    correlated_value_rowset_rows = fetch_all("""
        SELECT CAST(s."id" AS VARCHAR(10))
        FROM JSON_VIEW.SAMPLE s
        WHERE EXISTS (
          SELECT 1
          FROM VALUE tag IN s."tags"
          WHERE tag = 'blue'
        )
        ORDER BY s."id"
    """)
    assert_equal(correlated_value_rowset_rows, [("1",)], "correlated scalar-array rowset syntax")

    iterator_helper_rows = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)) AS doc_id,
          CAST(item._index AS VARCHAR(10)) AS item_index,
          COALESCE(JSON_TYPEOF(item."value"), 'MISSING') AS item_value_type,
          COALESCE(JSON_AS_VARCHAR(item."value"), 'NULL') AS item_value_text,
          COALESCE(CAST(JSON_AS_DECIMAL(item."amount") AS VARCHAR(60)), 'NULL') AS item_amount_decimal,
          COALESCE(CAST(JSON_AS_BOOLEAN(item."enabled") AS VARCHAR(10)), 'NULL') AS item_enabled_boolean,
          CASE WHEN JSON_IS_EXPLICIT_NULL(item."optional") THEN '1' ELSE '0' END AS item_optional_explicit_null,
          COALESCE(JSON_AS_VARCHAR(item."optional"), 'NULL') AS item_optional_text
        FROM JSON_VIEW.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
    """)
    assert_equal(
        iterator_helper_rows,
        [("1", "0", "STRING", "first", "7", "TRUE", "0", "x"), ("1", "1", "STRING", "second", "NULL", "FALSE", "1", "NULL"), ("2", "0", "STRING", "only", "5", "NULL", "0", "NULL")],
        "iterator helper semantics",
    )

    iterator_path_rows = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)) AS doc_id,
          CAST(item._index AS VARCHAR(10)) AS item_index,
          COALESCE(item."nested.note", 'NULL') AS nested_note,
          COALESCE(CAST(item."nested.score" AS VARCHAR(20)), 'NULL') AS nested_score,
          COALESCE(CAST(item."nested.active" AS VARCHAR(10)), 'NULL') AS nested_active,
          COALESCE(item."nested.items[LAST].value", 'NULL') AS nested_last_item
        FROM JSON_VIEW.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
    """)
    assert_equal(
        iterator_path_rows,
        [("1", "0", "nested-a", "11", "TRUE", "na-2"), ("1", "1", "nested-b", "12", "FALSE", "nb-1"), ("2", "0", "NULL", "NULL", "NULL", "NULL")],
        "iterator path syntax",
    )

    iterator_dynamic_index_rows = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)) AS doc_id,
          CAST(item._index AS VARCHAR(10)) AS item_index,
          COALESCE(CAST(item."nested.pick" AS VARCHAR(10)), 'NULL') AS nested_pick,
          COALESCE(item."nested.items[pick].value", 'NULL') AS nested_pick_value
        FROM JSON_VIEW.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
    """)
    assert_equal(
        iterator_dynamic_index_rows,
        [("1", "0", "1", "na-2"), ("1", "1", "0", "nb-1"), ("2", "0", "NULL", "NULL")],
        "iterator field-driven array selector syntax",
    )

    deep_path_rows = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          COALESCE("profile.prefs.theme", 'NULL') AS theme_value,
          COALESCE({DEEP_LEAF_PATH}, 'NULL') AS deep_leaf_value,
          COALESCE({DEEP_ENTRY_LAST_VALUE_PATH}, 'NULL') AS deep_last_entry,
          COALESCE(CAST({DEEP_ENTRY_SIZE_PATH} AS VARCHAR(20)), 'NULL') AS deep_entry_size
        FROM JSON_VIEW.DEEPDOC
        ORDER BY "doc_id"
    """)
    assert_equal(deep_path_rows, [("101", "dark", "bottom", "e2", "3"), ("102", "NULL", "NULL", "other", "1"), ("103", "NULL", "NULL", "NULL", "NULL")], "deep path syntax")

    deep_rowset_rows = fetch_all(f"""
        SELECT
          CAST(d."doc_id" AS VARCHAR(10)) AS doc_id,
          CAST(entry._index AS VARCHAR(10)) AS entry_index,
          entry.value,
          entry.kind,
          COALESCE(CAST(extra._index AS VARCHAR(10)), 'NULL') AS extra_index,
          COALESCE(extra, 'NULL') AS extra_value
        FROM JSON_VIEW.DEEPDOC d
        JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
        LEFT JOIN VALUE extra IN entry."extras"
        ORDER BY d."doc_id", entry._index, extra._index
    """)
    assert_equal(
        deep_rowset_rows,
        [("101", "0", "e0", "root", "0", "x0"), ("101", "0", "e0", "root", "1", "x1"), ("101", "1", "e1", "mid", "NULL", "NULL"), ("101", "2", "e2", "tail", "0", "tail-extra"), ("102", "0", "other", "solo", "0", "solo-extra")],
        "deep rowset syntax",
    )

    iterator_bracket_rows = fetch_all(f"""
        SELECT
          CAST(d."doc_id" AS VARCHAR(10)) AS doc_id,
          CAST(entry._index AS VARCHAR(10)) AS entry_index,
          COALESCE(entry."extras[LAST]", 'NULL') AS last_extra
        FROM JSON_VIEW.DEEPDOC d
        JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
        ORDER BY d."doc_id", entry._index
    """)
    assert_equal(
        iterator_bracket_rows,
        [("101", "0", "x1"), ("101", "1", "NULL"), ("101", "2", "tail-extra"), ("102", "0", "solo-extra")],
        "iterator bracket syntax",
    )

    explicit_null_rows = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS note_explicit_null,
          CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS note_missing,
          CASE WHEN JSON_IS_EXPLICIT_NULL("value") THEN '1' ELSE '0' END AS value_explicit_null
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(explicit_null_rows, [("1", "0", "0", "0"), ("2", "1", "0", "0"), ("3", "0", "1", "1")], "root explicit-null semantics")

    deep_explicit_null_rows = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          CASE WHEN JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END AS profile_explicit_null,
          CASE WHEN "profile.nickname" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END AS profile_missing,
          CASE WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END AS deep_explicit_null,
          CASE WHEN {DEEP_LEAF_PATH} IS NULL AND NOT JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END AS deep_missing
        FROM JSON_VIEW.DEEPDOC
        ORDER BY "doc_id"
    """)
    assert_equal(deep_explicit_null_rows, [("101", "1", "0", "0", "0"), ("102", "0", "1", "1", "0"), ("103", "0", "1", "0", "1")], "deep explicit-null semantics")

    root_variant_rows = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE(JSON_TYPEOF("value"), 'MISSING') AS value_type,
          COALESCE(JSON_AS_VARCHAR("value"), 'NULL') AS value_text,
          COALESCE(CAST(JSON_AS_DECIMAL("value") AS VARCHAR(60)), 'NULL') AS value_decimal,
          COALESCE(JSON_TYPEOF("shape"), 'MISSING') AS shape_type,
          COALESCE(CAST(JSON_AS_BOOLEAN("meta.flag") AS VARCHAR(10)), 'NULL') AS meta_flag
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(root_variant_rows, [("1", "NUMBER", "42", "42", "OBJECT", "TRUE"), ("2", "STRING", "43", "43", "ARRAY", "FALSE"), ("3", "NULL", "NULL", "NULL", "MISSING", "NULL")], "root variant semantics")

    mixed_type_rows = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)),
          TYPEOF("value"),
          COALESCE(JSON_TYPEOF("value"), 'MISSING')
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    mixed_type_rows_qualified = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)),
          TYPEOF(s."value"),
          COALESCE(JSON_TYPEOF(s."value"), 'MISSING')
        FROM JSON_VIEW.SAMPLE s
        ORDER BY s."id"
    """)
    assert_equal(mixed_type_rows, mixed_type_rows_qualified, "mixed built-in and JSON typeof semantics")

    deep_variant_rows = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          COALESCE(JSON_AS_VARCHAR("profile.nickname"), 'NULL') AS profile_nickname_value,
          COALESCE(JSON_AS_VARCHAR({DEEP_LEAF_PATH}), 'NULL') AS deep_leaf_value,
          COALESCE(JSON_TYPEOF("chain.next.next.next.next.next.next.next.reading"), 'MISSING') AS reading_type,
          COALESCE(JSON_AS_VARCHAR("chain.next.next.next.next.next.next.next.reading"), 'NULL') AS reading_text,
          COALESCE(CAST(JSON_AS_DECIMAL("chain.next.next.next.next.next.next.next.reading") AS VARCHAR(60)), 'NULL') AS reading_decimal
        FROM JSON_VIEW.DEEPDOC
        ORDER BY "doc_id"
    """)
    assert_equal(deep_variant_rows, [("101", "NULL", "bottom", "NUMBER", "100", "100"), ("102", "NULL", "NULL", "STRING", "101", "101"), ("103", "NULL", "NULL", "MISSING", "NULL", "NULL")], "deep variant semantics")

    deep_filter_rows = fetch_all(f"""
        SELECT CAST("doc_id" AS VARCHAR(10)) AS doc_id
        FROM JSON_VIEW.DEEPDOC
        WHERE "tags[LAST]" = 'gamma'
           OR {DEEP_ENTRY_LAST_VALUE_PATH} = 'other'
           OR "metrics[SIZE]" = 1
        ORDER BY "doc_id"
    """)
    assert_equal(deep_filter_rows, [("101",), ("102",)], "deep filter semantics")

    cte_path_rows = fetch_all("""
        WITH base AS (
          SELECT
            CAST("id" AS VARCHAR(10)) AS doc_id,
            COALESCE("child.value", 'NULL') AS child_value
          FROM JSON_VIEW.SAMPLE
        )
        SELECT doc_id, child_value
        FROM base
        ORDER BY doc_id
    """)
    assert_equal(cte_path_rows, [("1", "child-1"), ("2", "NULL"), ("3", "NULL")], "cte path syntax")

    cte_helper_rows = fetch_all("""
        WITH base AS (
          SELECT
            CAST("id" AS VARCHAR(10)) AS doc_id,
            COALESCE(JSON_TYPEOF("value"), 'MISSING') AS value_type
          FROM JSON_VIEW.SAMPLE
        )
        SELECT doc_id, value_type
        FROM base
        ORDER BY doc_id
    """)
    assert_equal(cte_helper_rows, [("1", "NUMBER"), ("2", "STRING"), ("3", "NULL")], "cte helper syntax")

    derived_table_rows = fetch_all("""
        SELECT *
        FROM (
          SELECT
            CAST("id" AS VARCHAR(10)) AS doc_id,
            COALESCE("child.value", 'NULL') AS child_value
          FROM JSON_VIEW.SAMPLE
        ) t
        ORDER BY doc_id
    """)
    assert_equal(derived_table_rows, [("1", "child-1"), ("2", "NULL"), ("3", "NULL")], "derived-table path syntax")

    alias_rows = fetch_all("""
        SELECT
          COALESCE("child.value", 'NULL') AS "child.value",
          COALESCE("tags[LAST]", 'NULL') AS "tags[LAST]"
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """)
    assert_equal(alias_rows, [("child-1", "blue"), ("NULL", "green"), ("NULL", "NULL")], "path-like output aliases")

    ddl_con = connect()
    try:
        ddl_con.execute("DROP SCHEMA IF EXISTS JVS_ANALYTICS CASCADE")
        ddl_con.execute("CREATE SCHEMA JVS_ANALYTICS")
        install_wrapper_preprocessor(ddl_con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])
        ddl_con.execute("""
            CREATE OR REPLACE VIEW JVS_ANALYTICS.SAMPLE_PATHS AS
            SELECT
              CAST("id" AS VARCHAR(10)) AS doc_id,
              COALESCE("child.value", 'NULL') AS child_value,
              COALESCE(JSON_TYPEOF("value"), 'MISSING') AS value_type
            FROM JSON_VIEW.SAMPLE
        """)
        ddl_con.execute("""
            CREATE OR REPLACE TABLE JVS_ANALYTICS.SAMPLE_NOTES AS
            SELECT
              CAST("id" AS VARCHAR(10)) AS doc_id,
              COALESCE("meta.info.note", 'NULL') AS deep_note
            FROM JSON_VIEW.SAMPLE
        """)
        ddl_con.execute("""
            CREATE OR REPLACE VIEW JVS_ANALYTICS.SAMPLE_ITEMS AS
            SELECT
              CAST(s."id" AS VARCHAR(10)) AS doc_id,
              CAST(item."_index" AS VARCHAR(10)) AS item_index,
              item.value,
              item.label
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items"
        """)
        ddl_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        ddl_path_rows = ddl_con.execute("""
            SELECT doc_id, child_value, value_type
            FROM JVS_ANALYTICS.SAMPLE_PATHS
            ORDER BY doc_id
        """).fetchall()
        ddl_note_rows = ddl_con.execute("""
            SELECT doc_id, deep_note
            FROM JVS_ANALYTICS.SAMPLE_NOTES
            ORDER BY doc_id
        """).fetchall()
        ddl_rowset_rows = ddl_con.execute("""
            SELECT doc_id, item_index, "value", "label"
            FROM JVS_ANALYTICS.SAMPLE_ITEMS
            ORDER BY doc_id, item_index
        """).fetchall()
    finally:
        try:
            ddl_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        ddl_con.close()

    assert_equal(
        ddl_path_rows,
        [("1", "child-1", "NUMBER"), ("2", "NULL", "STRING"), ("3", "NULL", "NULL")],
        "create view path/helper syntax",
    )
    assert_equal(
        ddl_note_rows,
        [("1", "deep"), ("2", "NULL"), ("3", "NULL")],
        "create table path syntax",
    )
    assert_equal(
        ddl_rowset_rows,
        [("1", "0", "first", "A"), ("1", "1", "second", "B"), ("2", "0", "only", "C")],
        "create view rowset syntax",
    )

    udf_wrapper = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)),
          "child.value"
        FROM JSON_VIEW.SAMPLE
        WHERE COALESCE("child.value", 'NULL') <> 'NULL'
        ORDER BY "id"
    """)
    assert_equal(udf_wrapper, [("1", "child-1")], "wrapper path query should behave like ordinary SQL surface")

    print("-- wrapper surface regression --")
    print("manifest roots:", manifest_roots)
    print("public tables:", public_tables)
    print("columns:", wrapper_columns)
    print("normalized rows:", normalized_rows)
    print("path rows:", path_rows)
    print("array rows:", array_rows)
    print("rowset rows:", rowset_rows)
    print("iterator helper rows:", iterator_helper_rows)
    print("deep path rows:", deep_path_rows)
    print("deep rowset rows:", deep_rowset_rows)
    print("root explicit-null rows:", explicit_null_rows)
    print("deep explicit-null rows:", deep_explicit_null_rows)
    print("root variant rows:", root_variant_rows)
    print("deep variant rows:", deep_variant_rows)
    print("cte path rows:", cte_path_rows)
    print("cte helper rows:", cte_helper_rows)
    print("alias rows:", alias_rows)
    print("ddl path rows:", ddl_path_rows)
    print("ddl note rows:", ddl_note_rows)
    print("ddl rowset rows:", ddl_rowset_rows)


if __name__ == "__main__":
    main()

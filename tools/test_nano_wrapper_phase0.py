#!/usr/bin/env python3

import subprocess

from nano_support import (
    ROOT,
    bundle_adapter,
    install_preprocessor,
    connect,
    install_virtual_schema_fixture,
    install_wrapper_preprocessor,
    install_wrapper_views,
)


PUBLIC_WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_WRAPPER_SCHEMA = "JSON_VIEW_INTERNAL"
DEEP_LEAF_PATH = '"chain.next.next.next.next.next.next.next.leaf_note"'
DEEP_ENTRY_LAST_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[LAST].value"'
DEEP_ENTRY_SIZE_PATH = '"chain.next.next.next.next.next.next.next.entries[SIZE]"'
DEEP_ENTRY_ARRAY_PATH = 'd."chain.next.next.next.next.next.next.next.entries"'


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def fetch_all(sql: str, *, wrapper: bool) -> list[tuple]:
    con = connect()
    try:
        if wrapper:
            install_wrapper_preprocessor(con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])
        else:
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


def main() -> None:
    con = connect()
    try:
        install_virtual_schema_fixture(con, bundle_adapter(), include_deep_fixture=True)
        manifest = install_wrapper_views(
            con,
            source_schema="JVS_SRC",
            wrapper_schema=PUBLIC_WRAPPER_SCHEMA,
            helper_schema=HELPER_WRAPPER_SCHEMA,
            generate_preprocessor=True,
        )
        install_wrapper_preprocessor(con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])

        if manifest["publicSchema"] != PUBLIC_WRAPPER_SCHEMA:
            raise AssertionError(f"unexpected public schema in manifest: {manifest['publicSchema']}")
        if manifest["helperSchema"] != HELPER_WRAPPER_SCHEMA:
            raise AssertionError(f"unexpected helper schema in manifest: {manifest['helperSchema']}")
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
        virtual_columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        assert_equal(wrapper_columns, virtual_columns, "wrapper vs virtual column names")

        deep_wrapper_columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VIEW' AND COLUMN_TABLE = 'DEEPDOC'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        deep_virtual_columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'DEEPDOC'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        assert_equal(deep_wrapper_columns, deep_virtual_columns, "deep wrapper vs virtual column names")

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
    if "Configured function names: JSON_IS_EXPLICIT_NULL, JNULL, JSON_TYPEOF, JSON_AS_VARCHAR, JSON_AS_DECIMAL, JSON_AS_BOOLEAN" not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should enable Phase 4 helper aliases")
    if "JSON syntax allowed only for configured JSON schemas: JSON_VIEW" not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should be scoped to the public wrapper schema")
    if "Helper rewrite mode: wrapper semantic helpers" not in packaged_sql:
        raise AssertionError("packaged wrapper preprocessor should use wrapper semantic helper rewrite mode in Phase 4")
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

    normalized_virtual_rows = fetch_all("""
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
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
    """, wrapper=False)
    normalized_wrapper_rows = fetch_all("""
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
    """, wrapper=True)
    assert_equal(normalized_wrapper_rows, normalized_virtual_rows, "normalized select star parity")

    path_virtual = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("child.value", 'NULL') AS child_value,
          CASE
            WHEN "meta.info.note" IS NULL THEN 'NULL'
            ELSE "meta.info.note"
          END AS deep_note,
          COALESCE("tags[LAST]", 'NULL') AS last_tag
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
    """, wrapper=False)
    path_wrapper = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("child.value", 'NULL') AS child_value,
          CASE
            WHEN "meta.info.note" IS NULL THEN 'NULL'
            ELSE "meta.info.note"
          END AS deep_note,
          COALESCE("tags[LAST]", 'NULL') AS last_tag
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """, wrapper=True)
    assert_equal(path_wrapper, path_virtual, "path syntax parity")

    array_virtual = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("tags[FIRST]", 'NULL') AS first_tag,
          COALESCE("tags[LAST]", 'NULL') AS last_tag,
          COALESCE(CAST("tags[SIZE]" AS VARCHAR(20)), 'NULL') AS tag_count,
          COALESCE("items[LAST].value", 'NULL') AS last_item_value,
          COALESCE("meta.items[LAST].value", 'NULL') AS last_meta_item_value
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
    """, wrapper=False)
    array_wrapper = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE("tags[FIRST]", 'NULL') AS first_tag,
          COALESCE("tags[LAST]", 'NULL') AS last_tag,
          COALESCE(CAST("tags[SIZE]" AS VARCHAR(20)), 'NULL') AS tag_count,
          COALESCE("items[LAST].value", 'NULL') AS last_item_value,
          COALESCE("meta.items[LAST].value", 'NULL') AS last_meta_item_value
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """, wrapper=True)
    assert_equal(array_wrapper, array_virtual, "array syntax parity")

    rowset_virtual = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)),
          CAST(item._index AS VARCHAR(10)),
          item.value,
          item.label
        FROM JSON_VS.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
    """, wrapper=False)
    rowset_wrapper = fetch_all("""
        SELECT
          CAST(s."id" AS VARCHAR(10)),
          CAST(item._index AS VARCHAR(10)),
          item.value,
          item.label
        FROM JSON_VIEW.SAMPLE s
        JOIN item IN s."items"
        ORDER BY s."id", item._index
    """, wrapper=True)
    assert_equal(rowset_wrapper, rowset_virtual, "rowset syntax parity")

    deep_path_virtual = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          COALESCE("profile.prefs.theme", 'NULL') AS theme_value,
          COALESCE({DEEP_LEAF_PATH}, 'NULL') AS deep_leaf_value,
          COALESCE({DEEP_ENTRY_LAST_VALUE_PATH}, 'NULL') AS deep_last_entry,
          COALESCE(CAST({DEEP_ENTRY_SIZE_PATH} AS VARCHAR(20)), 'NULL') AS deep_entry_size
        FROM JSON_VS.DEEPDOC
        ORDER BY "doc_id"
    """, wrapper=False)
    deep_path_wrapper = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          COALESCE("profile.prefs.theme", 'NULL') AS theme_value,
          COALESCE({DEEP_LEAF_PATH}, 'NULL') AS deep_leaf_value,
          COALESCE({DEEP_ENTRY_LAST_VALUE_PATH}, 'NULL') AS deep_last_entry,
          COALESCE(CAST({DEEP_ENTRY_SIZE_PATH} AS VARCHAR(20)), 'NULL') AS deep_entry_size
        FROM JSON_VIEW.DEEPDOC
        ORDER BY "doc_id"
    """, wrapper=True)
    assert_equal(deep_path_wrapper, deep_path_virtual, "deep path parity")

    deep_rowset_virtual = fetch_all(f"""
        SELECT
          CAST(d."doc_id" AS VARCHAR(10)) AS doc_id,
          CAST(entry._index AS VARCHAR(10)) AS entry_index,
          entry.value,
          entry.kind,
          COALESCE(CAST(extra._index AS VARCHAR(10)), 'NULL') AS extra_index,
          COALESCE(extra, 'NULL') AS extra_value
        FROM JSON_VS.DEEPDOC d
        JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
        LEFT JOIN VALUE extra IN entry."extras"
        ORDER BY d."doc_id", entry._index, extra._index
    """, wrapper=False)
    deep_rowset_wrapper = fetch_all(f"""
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
    """, wrapper=True)
    assert_equal(deep_rowset_wrapper, deep_rowset_virtual, "deep rowset parity")

    explicit_null_virtual = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS note_explicit_null,
          CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS note_missing,
          CASE WHEN JSON_IS_EXPLICIT_NULL("value") THEN '1' ELSE '0' END AS value_explicit_null
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
    """, wrapper=False)
    explicit_null_wrapper = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS note_explicit_null,
          CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS note_missing,
          CASE WHEN JSON_IS_EXPLICIT_NULL("value") THEN '1' ELSE '0' END AS value_explicit_null
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """, wrapper=True)
    assert_equal(explicit_null_wrapper, explicit_null_virtual, "root explicit-null parity")

    deep_explicit_null_virtual = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          CASE WHEN JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END AS profile_explicit_null,
          CASE WHEN "profile.nickname" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END AS profile_missing,
          CASE WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END AS deep_explicit_null,
          CASE WHEN {DEEP_LEAF_PATH} IS NULL AND NOT JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END AS deep_missing
        FROM JSON_VS.DEEPDOC
        ORDER BY "doc_id"
    """, wrapper=False)
    deep_explicit_null_wrapper = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          CASE WHEN JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END AS profile_explicit_null,
          CASE WHEN "profile.nickname" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END AS profile_missing,
          CASE WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END AS deep_explicit_null,
          CASE WHEN {DEEP_LEAF_PATH} IS NULL AND NOT JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END AS deep_missing
        FROM JSON_VIEW.DEEPDOC
        ORDER BY "doc_id"
    """, wrapper=True)
    assert_equal(deep_explicit_null_wrapper, deep_explicit_null_virtual, "deep explicit-null parity")

    root_variant_virtual = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE(TYPEOF("value"), 'MISSING') AS value_type,
          COALESCE(CAST("value" AS VARCHAR(100)), 'NULL') AS value_text,
          COALESCE(CAST(CAST("value" AS DECIMAL(36,18)) AS VARCHAR(60)), 'NULL') AS value_decimal,
          COALESCE(TYPEOF("shape"), 'MISSING') AS shape_type,
          COALESCE(CAST("meta.flag" AS VARCHAR(10)), 'NULL') AS meta_flag
        FROM JSON_VS.SAMPLE
        ORDER BY "id"
    """, wrapper=False)
    root_variant_wrapper = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)) AS doc_id,
          COALESCE(JSON_TYPEOF("value"), 'MISSING') AS value_type,
          COALESCE(JSON_AS_VARCHAR("value"), 'NULL') AS value_text,
          COALESCE(CAST(JSON_AS_DECIMAL("value") AS VARCHAR(60)), 'NULL') AS value_decimal,
          COALESCE(JSON_TYPEOF("shape"), 'MISSING') AS shape_type,
          COALESCE(CAST(JSON_AS_BOOLEAN("meta.flag") AS VARCHAR(10)), 'NULL') AS meta_flag
        FROM JSON_VIEW.SAMPLE
        ORDER BY "id"
    """, wrapper=True)
    assert_equal(root_variant_wrapper, root_variant_virtual, "root variant parity")

    deep_variant_virtual = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          COALESCE(CAST("profile.nickname" AS VARCHAR(100)), 'NULL') AS profile_nickname_value,
          COALESCE(CAST({DEEP_LEAF_PATH} AS VARCHAR(100)), 'NULL') AS deep_leaf_value,
          COALESCE(TYPEOF("chain.next.next.next.next.next.next.next.reading"), 'MISSING') AS reading_type,
          COALESCE(CAST("chain.next.next.next.next.next.next.next.reading" AS VARCHAR(100)), 'NULL') AS reading_text,
          COALESCE(CAST(CAST("chain.next.next.next.next.next.next.next.reading" AS DECIMAL(36,18)) AS VARCHAR(60)), 'NULL') AS reading_decimal
        FROM JSON_VS.DEEPDOC
        ORDER BY "doc_id"
    """, wrapper=False)
    deep_variant_wrapper = fetch_all(f"""
        SELECT
          CAST("doc_id" AS VARCHAR(10)) AS doc_id,
          COALESCE(JSON_AS_VARCHAR("profile.nickname"), 'NULL') AS profile_nickname_value,
          COALESCE(JSON_AS_VARCHAR({DEEP_LEAF_PATH}), 'NULL') AS deep_leaf_value,
          COALESCE(JSON_TYPEOF("chain.next.next.next.next.next.next.next.reading"), 'MISSING') AS reading_type,
          COALESCE(JSON_AS_VARCHAR("chain.next.next.next.next.next.next.next.reading"), 'NULL') AS reading_text,
          COALESCE(CAST(JSON_AS_DECIMAL("chain.next.next.next.next.next.next.next.reading") AS VARCHAR(60)), 'NULL') AS reading_decimal
        FROM JSON_VIEW.DEEPDOC
        ORDER BY "doc_id"
    """, wrapper=True)
    assert_equal(deep_variant_wrapper, deep_variant_virtual, "deep variant parity")

    deep_filter_virtual = fetch_all(f"""
        SELECT CAST("doc_id" AS VARCHAR(10)) AS doc_id
        FROM JSON_VS.DEEPDOC
        WHERE "tags[LAST]" = 'gamma'
           OR {DEEP_ENTRY_LAST_VALUE_PATH} = 'other'
           OR "metrics[SIZE]" = 1
        ORDER BY "doc_id"
    """, wrapper=False)
    deep_filter_wrapper = fetch_all(f"""
        SELECT CAST("doc_id" AS VARCHAR(10)) AS doc_id
        FROM JSON_VIEW.DEEPDOC
        WHERE "tags[LAST]" = 'gamma'
           OR {DEEP_ENTRY_LAST_VALUE_PATH} = 'other'
           OR "metrics[SIZE]" = 1
        ORDER BY "doc_id"
    """, wrapper=True)
    assert_equal(deep_filter_wrapper, deep_filter_virtual, "deep filter parity")

    udf_wrapper = fetch_all("""
        SELECT
          CAST("id" AS VARCHAR(10)),
          "child.value"
        FROM JSON_VIEW.SAMPLE
        WHERE COALESCE("child.value", 'NULL') <> 'NULL'
        ORDER BY "id"
    """, wrapper=True)
    assert_equal(udf_wrapper, [("1", "child-1")], "wrapper path query should behave like ordinary SQL surface")

    print("-- wrapper phase 4 parity --")
    print("manifest roots:", manifest_roots)
    print("public tables:", public_tables)
    print("columns:", wrapper_columns)
    print("normalized rows:", normalized_wrapper_rows)
    print("path parity:", path_wrapper)
    print("array parity:", array_wrapper)
    print("rowset parity:", rowset_wrapper)
    print("deep path parity:", deep_path_wrapper)
    print("deep rowset parity:", deep_rowset_wrapper)
    print("root explicit-null parity:", explicit_null_wrapper)
    print("deep explicit-null parity:", deep_explicit_null_wrapper)
    print("root variant parity:", root_variant_wrapper)
    print("deep variant parity:", deep_variant_wrapper)


if __name__ == "__main__":
    main()

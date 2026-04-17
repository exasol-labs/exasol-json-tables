#!/usr/bin/env python3

import _bootstrap  # noqa: F401

from nano_support import connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


PUBLIC_WRAPPER_SCHEMA = "JSON_VIEW"
HELPER_WRAPPER_SCHEMA = "JSON_VIEW_INTERNAL"
MODEL_SCHEMA = "JVS_PHASE4_MODEL"
UDF_SCHEMA = "JVS_PHASE4_UDF"
DEEP_ENTRY_ARRAY_PATH = 'd."chain.next.next.next.next.next.next.next.entries"'


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=True)
        install_wrapper_views(
            con,
            source_schema="JVS_SRC",
            wrapper_schema=PUBLIC_WRAPPER_SCHEMA,
            helper_schema=HELPER_WRAPPER_SCHEMA,
            generate_preprocessor=True,
        )
        install_wrapper_preprocessor(con, [PUBLIC_WRAPPER_SCHEMA], [HELPER_WRAPPER_SCHEMA])

        con.execute(f"DROP SCHEMA IF EXISTS {MODEL_SCHEMA} CASCADE")
        con.execute(f"CREATE SCHEMA {MODEL_SCHEMA}")
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

        nested_cte_rows = con.execute(
            """
            WITH item_base AS (
              SELECT
                CAST(s."id" AS VARCHAR(10)) AS doc_id,
                CAST(item._index AS VARCHAR(10)) AS item_index,
                COALESCE(JSON_TYPEOF(item."value"), 'MISSING') AS item_value_type,
                COALESCE(JSON_AS_VARCHAR(item."nested.items[LAST].value"), 'NULL') AS nested_last_value,
                CASE WHEN JSON_IS_EXPLICIT_NULL(item."optional") THEN '1' ELSE '0' END AS optional_is_null,
                COALESCE(s."meta.items[LAST].value", 'NULL') AS meta_last_value
              FROM JSON_VIEW.SAMPLE s
              JOIN item IN s."items"
            ), shaped AS (
              SELECT
                doc_id,
                item_index,
                item_value_type,
                nested_last_value,
                optional_is_null,
                meta_last_value
              FROM item_base
            )
            SELECT *
            FROM shaped
            ORDER BY doc_id, item_index
            """
        ).fetchall()
        assert_equal(
            nested_cte_rows,
            [
                ("1", "0", "STRING", "na-2", "0", "m2"),
                ("1", "1", "STRING", "nb-1", "1", "m2"),
                ("2", "0", "STRING", "NULL", "0", "m3"),
            ],
            "nested CTE stack",
        )

        stacked_derived_rows = con.execute(
            """
            SELECT *
            FROM (
              SELECT doc_id, item_index, item_value_text, meta_last_value
              FROM (
                SELECT
                  CAST(s."id" AS VARCHAR(10)) AS doc_id,
                  CAST(item._index AS VARCHAR(10)) AS item_index,
                  COALESCE(JSON_AS_VARCHAR(item."value"), 'NULL') AS item_value_text,
                  COALESCE(s."meta.items[LAST].value", 'NULL') AS meta_last_value
                FROM JSON_VIEW.SAMPLE s
                JOIN item IN s."items"
              ) inner_q
            ) outer_q
            ORDER BY doc_id, item_index
            """
        ).fetchall()
        assert_equal(
            stacked_derived_rows,
            [("1", "0", "first", "m2"), ("1", "1", "second", "m2"), ("2", "0", "only", "m3")],
            "stacked derived tables",
        )

        union_rows = con.execute(
            """
            SELECT source_name, doc_id, payload
            FROM (
              SELECT 'sample' AS source_name, CAST("id" AS VARCHAR(10)) AS doc_id, COALESCE(JSON_AS_VARCHAR("value"), 'NULL') AS payload
              FROM JSON_VIEW.SAMPLE
              UNION ALL
              SELECT 'deep' AS source_name, CAST("doc_id" AS VARCHAR(10)) AS doc_id, COALESCE(JSON_AS_VARCHAR("chain.next.next.next.next.next.next.next.reading"), 'NULL') AS payload
              FROM JSON_VIEW.DEEPDOC
            ) unioned
            ORDER BY source_name, doc_id
            """
        ).fetchall()
        assert_equal(
            union_rows,
            [
                ("deep", "101", "100"),
                ("deep", "102", "101"),
                ("deep", "103", "NULL"),
                ("sample", "1", "42"),
                ("sample", "2", "43"),
                ("sample", "3", "NULL"),
            ],
            "union all with wrapper expressions",
        )

        grouped_rows = con.execute(
            """
            SELECT
              COALESCE(JSON_TYPEOF("value"), 'MISSING') AS value_type,
              COUNT(*) AS row_count
            FROM JSON_VIEW.SAMPLE
            GROUP BY COALESCE(JSON_TYPEOF("value"), 'MISSING')
            ORDER BY value_type
            """
        ).fetchall()
        assert_equal(
            grouped_rows,
            [("NULL", 1), ("NUMBER", 1), ("STRING", 1)],
            "group by / order by projected wrapper expressions",
        )

        con.execute(
            f"""
            CREATE OR REPLACE VIEW {MODEL_SCHEMA}.ITEM_MODEL AS
            WITH expanded AS (
              SELECT
                CAST(s."id" AS VARCHAR(10)) AS doc_id,
                CAST(item._index AS VARCHAR(10)) AS item_index,
                COALESCE(JSON_TYPEOF(item."value"), 'MISSING') AS item_value_type,
                COALESCE(JSON_AS_VARCHAR(item."nested.items[LAST].value"), 'NULL') AS nested_last_value,
                COALESCE(s."meta.items[LAST].value", 'NULL') AS meta_last_value
              FROM JSON_VIEW.SAMPLE s
              JOIN item IN s."items"
            )
            SELECT *
            FROM expanded
            """
        )
        persisted_view_rows = con.execute(
            f"""
            SELECT *
            FROM {MODEL_SCHEMA}.ITEM_MODEL
            ORDER BY doc_id, item_index
            """
        ).fetchall()
        assert_equal(
            persisted_view_rows,
            [
                ("1", "0", "STRING", "na-2", "m2"),
                ("1", "1", "STRING", "nb-1", "m2"),
                ("2", "0", "STRING", "NULL", "m3"),
            ],
            "persisted view with mixed helper and rowset logic",
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE {MODEL_SCHEMA}.DEEP_ENTRY_MODEL AS
            SELECT
              CAST(d."doc_id" AS VARCHAR(10)) AS doc_id,
              CAST(entry._index AS VARCHAR(10)) AS entry_index,
              COALESCE(JSON_AS_VARCHAR(entry."value"), 'NULL') AS entry_value_text,
              COALESCE(entry."extras[LAST]", 'NULL') AS last_extra,
              COALESCE(d."chain.next.next.next.next.next.next.next.leaf_note", 'NULL') AS deep_leaf
            FROM JSON_VIEW.DEEPDOC d
            JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
            """
        )
        persisted_table_rows = con.execute(
            f"""
            SELECT *
            FROM {MODEL_SCHEMA}.DEEP_ENTRY_MODEL
            ORDER BY doc_id, entry_index
            """
        ).fetchall()
        assert_equal(
            persisted_table_rows,
            [
                ("101", "0", "e0", "x1", "bottom"),
                ("101", "1", "e1", "NULL", "bottom"),
                ("101", "2", "e2", "tail-extra", "bottom"),
                ("102", "0", "other", "solo-extra", "NULL"),
            ],
            "persisted table with mixed deep path and rowset logic",
        )

        udf_rows = con.execute(
            f"""
            SELECT
              CAST(s."id" AS VARCHAR(10)) AS doc_id,
              CAST(item._index AS VARCHAR(10)) AS item_index,
              COALESCE({UDF_SCHEMA}.IDENTITY_VARCHAR(JSON_AS_VARCHAR(item."value")), 'NULL') AS passthrough_value,
              COALESCE({UDF_SCHEMA}.IDENTITY_VARCHAR(item."nested.items[LAST].value"), 'NULL') AS passthrough_nested
            FROM JSON_VIEW.SAMPLE s
            JOIN item IN s."items"
            ORDER BY doc_id, item_index
            """
        ).fetchall()
        assert_equal(
            udf_rows,
            [("1", "0", "first", "na-2"), ("1", "1", "second", "nb-1"), ("2", "0", "only", "NULL")],
            "UDF usage on iterator-local helper expressions",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    print("-- wrapper modeling regression --")
    print("Phase 4 modeling and BI patterns passed against Nano.")


if __name__ == "__main__":
    main()

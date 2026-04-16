#!/usr/bin/env python3

from pathlib import Path
import subprocess

from nano_support import bundle_adapter, connect, install_preprocessor, install_virtual_schema_fixture


ROOT = Path(__file__).resolve().parents[1]


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def main() -> None:
    adapter_code = bundle_adapter()
    con = connect()
    try:
        install_virtual_schema_fixture(con, adapter_code)
        install_preprocessor(con, ["JSON_IS_EXPLICIT_NULL", "JNULL"], rewrite_path_identifiers=True)

        generated_sql = (ROOT / "dist" / "json_null_preprocessor_test.sql").read_text()
        if "\nALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = " in generated_sql:
            raise AssertionError("generated installer should not auto-activate the preprocessor")
        if "OPEN SCHEMA" in generated_sql:
            raise AssertionError("generated installer should not change the current schema")

        activated_output = ROOT / "dist" / "json_null_preprocessor_test_activate.sql"
        subprocess.run(
            [
                "python3",
                str(ROOT / "tools" / "generate_preprocessor_sql.py"),
                "--function-name",
                "JSON_IS_EXPLICIT_NULL",
                "--rewrite-path-identifiers",
                "--activate-session",
                "--output",
                str(activated_output),
            ],
            check=True,
        )
        activated_sql = activated_output.read_text()
        if "ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_PP.JSON_NULL_PREPROCESSOR;" not in activated_sql:
            raise AssertionError("expected --activate-session output to include explicit activation SQL")
        if "OPEN SCHEMA" in activated_sql:
            raise AssertionError("activated installer should not change the current schema")

        columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        assert_equal(
            columns,
            [
                ("_id",),
                ("id",),
                ("name",),
                ("note",),
                ("child|object",),
                ("meta|object",),
                ("value",),
                ("shape",),
                ("tags|array",),
                ("items|array",),
            ],
            "virtual schema columns",
        )

        alias_rows = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
                CASE WHEN JNULL("value") THEN '1' ELSE '0' END
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(alias_rows, [("1", "0", "0"), ("2", "1", "0"), ("3", "0", "1")], "function alias query")

        path_rows = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                COALESCE("child.value", 'NULL'),
                CASE
                    WHEN "meta.flag" IS NULL THEN 'NULL'
                    WHEN "meta.flag" THEN 'TRUE'
                    ELSE 'FALSE'
                END,
                COALESCE("meta.info.note", 'NULL')
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(
            path_rows,
            [("1", "child-1", "TRUE", "deep"), ("2", "NULL", "FALSE", "NULL"), ("3", "NULL", "NULL", "NULL")],
            "path syntax query",
        )

        array_rows = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                "tags[0]" AS "tag0",
                "tags[1]" AS "tag1",
                "tags[FIRST]" AS "tag_first",
                "tags[LAST]" AS "tag_last",
                CAST("tags[SIZE]" AS VARCHAR(20)) AS "tag_size",
                "items[1].value" AS "item1_value",
                "items[FIRST].label" AS "item_first_label",
                "items[LAST].value" AS "item_last_value",
                CAST("items[SIZE]" AS VARCHAR(20)) AS "item_size",
                "meta.items[1].value" AS "meta_item1_value",
                "meta.items[LAST].value" AS "meta_item_last_value",
                CAST("meta.items[SIZE]" AS VARCHAR(20)) AS "meta_item_size"
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(
            array_rows,
            [
                ("1", "red", "blue", "red", "blue", "2", "second", "A", "second", "2", "m2", "m2", "2"),
                ("2", "green", None, "green", "green", "1", None, "C", "only", "1", None, "m3", "1"),
                ("3", None, None, None, None, None, None, None, None, None, None, None, None),
            ],
            "array access query",
        )

        array_filter_rows = con.execute("""
            SELECT CAST("id" AS VARCHAR(10))
            FROM JSON_VS.SAMPLE
            WHERE "tags[0]" = 'red'
               OR "meta.items[LAST].value" = 'm2'
            ORDER BY "id"
        """).fetchall()
        assert_equal(array_filter_rows, [("1",)], "array predicate query")

        array_size_filter_rows = con.execute("""
            SELECT CAST("id" AS VARCHAR(10))
            FROM JSON_VS.SAMPLE
            WHERE "tags[SIZE]" = 2
               OR "meta.items[SIZE]" = 1
            ORDER BY "id"
        """).fetchall()
        assert_equal(array_size_filter_rows, [("1",), ("2",)], "array size predicate query")

        alias_stmt = con.execute("""
            SELECT
                COALESCE("meta.info.note", 'NULL') AS "child.value"
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """)
        alias_columns = list(alias_stmt.columns().keys())
        alias_result_rows = alias_stmt.fetchall()
        assert_equal(alias_columns, ["child.value"], "dotted alias columns")
        assert_equal(alias_result_rows, [("deep",), ("NULL",), ("NULL",)], "dotted alias query")

        comment_stmt = con.execute("""
            SELECT "id" -- "meta.info.note"
            FROM JSON_VS.SAMPLE
            ORDER BY 1
        """)
        comment_columns = list(comment_stmt.columns().keys())
        comment_rows = comment_stmt.fetchall()
        assert_equal(comment_columns, ["id"], "comment query columns")
        assert_equal(comment_rows, [(1,), (2,), (3,)], "comment query")

        block_comment_stmt = con.execute("""
            SELECT "id" /* "meta.info.note" */
            FROM JSON_VS.SAMPLE
            ORDER BY 1
        """)
        block_comment_columns = list(block_comment_stmt.columns().keys())
        block_comment_rows = block_comment_stmt.fetchall()
        assert_equal(block_comment_columns, ["id"], "block comment query columns")
        assert_equal(block_comment_rows, [(1,), (2,), (3,)], "block comment query")

        missing_rows = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
                CASE WHEN "child|object" IS NULL AND NOT JNULL("child|object") THEN '1' ELSE '0' END
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(missing_rows, [("1", "0", "0"), ("2", "0", "1"), ("3", "1", "0")], "missing value query")

        explain_rows = con.execute("""
            EXPLAIN VIRTUAL
            SELECT CASE WHEN JSON_IS_EXPLICIT_NULL("SAMPLE"."note") THEN 1 ELSE 0 END,
                   "child.value", "meta.info.note", "tags[FIRST]", "tags[LAST]", "tags[SIZE]",
                   "items[LAST].value", "meta.items[LAST].value", "meta.items[SIZE]"
            FROM JSON_VS.SAMPLE
        """).fetchall()
        explain_sql = explain_rows[0][1]
        for expected_fragment in ['"note|n"', '"SAMPLE_child"', '"SAMPLE_meta_info"', '"SAMPLE_tags_arr"',
                                  '"SAMPLE_items_arr"', '"SAMPLE_meta_items_arr"', '"_parent"', '"_pos"',
                                  '"tags|array"', '"items|array"', '- 1']:
            if expected_fragment not in explain_sql:
                raise AssertionError(f'Expected EXPLAIN VIRTUAL SQL to reference {expected_fragment}, got: {explain_sql}')
        if "(SELECT" in explain_sql:
            raise AssertionError(f'Expected join-mode array access without scalar subqueries, got: {explain_sql}')

        print("-- preprocessor regression --")
        print("columns:", columns)
        print("function aliases:", alias_rows)
        print("path syntax:", path_rows)
        print("array access:", array_rows)
        print("array predicate:", array_filter_rows)
        print("array size predicate:", array_size_filter_rows)
        print("dotted alias:", alias_result_rows)
        print("comment query:", comment_rows)
        print("block comment query:", block_comment_rows)
        print("missing values:", missing_rows)
        print("explain virtual:", explain_sql)
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

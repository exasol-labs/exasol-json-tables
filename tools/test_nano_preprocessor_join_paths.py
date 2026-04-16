#!/usr/bin/env python3

from nano_support import bundle_adapter, connect, install_preprocessor, install_virtual_schema_fixture


DEEP_LEAF_PATH = '"chain.next.next.next.next.next.next.next.leaf_note"'
DEEP_READING_PATH = '"chain.next.next.next.next.next.next.next.reading"'
DEEP_ENTRY_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[2].value"'
DEEP_ENTRY_LAST_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[LAST].value"'
DEEP_ENTRY_SIZE_PATH = '"chain.next.next.next.next.next.next.next.entries[SIZE]"'


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def main() -> None:
    adapter_code = bundle_adapter()
    con = connect()
    try:
        install_virtual_schema_fixture(con, adapter_code, include_deep_fixture=True)
        install_preprocessor(con, ["JSON_IS_EXPLICIT_NULL", "JNULL"], rewrite_path_identifiers=True)

        sample_columns = [name for (name,) in con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()]
        deep_columns = [name for (name,) in con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'DEEPDOC'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()]
        for columns, label in [(sample_columns, "sample"), (deep_columns, "deepdoc")]:
            leaked = [name for name in columns if name.startswith("__path__")]
            if leaked:
                raise AssertionError(f"{label} metadata leaked synthetic path columns: {leaked}")

        shallow_rows = con.execute("""
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
            shallow_rows,
            [("1", "child-1", "TRUE", "deep"), ("2", "NULL", "FALSE", "NULL"), ("3", "NULL", "NULL", "NULL")],
            "shallow path query",
        )

        deep_rows = con.execute(f"""
            SELECT
                CAST("doc_id" AS VARCHAR(10)),
                COALESCE("profile.prefs.theme", 'NULL'),
                COALESCE({DEEP_LEAF_PATH}, 'NULL'),
                COALESCE(TYPEOF({DEEP_READING_PATH}), 'MISSING'),
                COALESCE(CAST({DEEP_READING_PATH} AS VARCHAR(100)), 'NULL')
            FROM JSON_VS.DEEPDOC
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(
            deep_rows,
            [
                ("101", "dark", "bottom", "NUMBER", "100"),
                ("102", "NULL", "NULL", "STRING", "101"),
                ("103", "NULL", "NULL", "MISSING", "NULL"),
            ],
            "deep path query",
        )

        array_rows = con.execute(f"""
            SELECT
                CAST("doc_id" AS VARCHAR(10)),
                COALESCE("tags[FIRST]", 'NULL') AS "tag_first",
                COALESCE("tags[LAST]", 'NULL') AS "tag_last",
                COALESCE(CAST("tags[SIZE]" AS VARCHAR(20)), 'NULL') AS "tag_size",
                COALESCE(CAST("metrics[1]" AS VARCHAR(20)), 'NULL') AS "metric1",
                COALESCE(CAST("metrics[LAST]" AS VARCHAR(20)), 'NULL') AS "metric_last",
                COALESCE(CAST("metrics[SIZE]" AS VARCHAR(20)), 'NULL') AS "metric_size",
                COALESCE({DEEP_ENTRY_VALUE_PATH}, 'NULL') AS "entry2_value",
                COALESCE({DEEP_ENTRY_LAST_VALUE_PATH}, 'NULL') AS "entry_last_value",
                COALESCE(CAST({DEEP_ENTRY_SIZE_PATH} AS VARCHAR(20)), 'NULL') AS "entry_size"
            FROM JSON_VS.DEEPDOC
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(
            array_rows,
            [
                ("101", "alpha", "gamma", "3", "20", "30", "3", "e2", "e2", "3"),
                ("102", "delta", "delta", "1", "NULL", "7", "1", "NULL", "other", "1"),
                ("103", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"),
            ],
            "array access with join-mode paths",
        )

        array_filter_rows = con.execute(f"""
            SELECT CAST("doc_id" AS VARCHAR(10))
            FROM JSON_VS.DEEPDOC
            WHERE "metrics[LAST]" = 30
               OR {DEEP_ENTRY_LAST_VALUE_PATH} = 'other'
               OR "tags[SIZE]" = 1
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(array_filter_rows, [("101",), ("102",)], "array predicate with join-mode paths")

        null_rows = con.execute(f"""
            SELECT
                CAST("doc_id" AS VARCHAR(10)),
                CASE WHEN JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END,
                CASE WHEN "profile.nickname" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END,
                CASE WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END,
                CASE WHEN {DEEP_LEAF_PATH} IS NULL AND NOT JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END
            FROM JSON_VS.DEEPDOC
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(
            null_rows,
            [("101", "1", "0", "0", "0"), ("102", "0", "1", "1", "0"), ("103", "0", "1", "0", "1")],
            "null semantics with join-mode paths",
        )

        alias_stmt = con.execute("""
            SELECT
                COALESCE("meta.info.note", 'NULL') AS "child.value"
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """)
        alias_columns = list(alias_stmt.columns().keys())
        alias_rows = alias_stmt.fetchall()
        assert_equal(alias_columns, ["child.value"], "join-mode dotted alias columns")
        assert_equal(alias_rows, [("deep",), ("NULL",), ("NULL",)], "join-mode dotted alias query")

        explain_sql = con.execute("""
            EXPLAIN VIRTUAL
            SELECT "child.value", "meta.info.note", "tags[LAST]", "tags[SIZE]", "meta.items[LAST].value"
            FROM JSON_VS.SAMPLE
        """).fetchall()[0][1]
        if "__path__" in explain_sql:
            raise AssertionError(f"join-mode explain should not reference synthetic path columns: {explain_sql}")
        for expected in ['"SAMPLE_child"', '"SAMPLE_meta_info"', '"SAMPLE_tags_arr"', '"SAMPLE_meta_items_arr"',
                         '"tags|array"', '"items|array"', 'LEFT OUTER JOIN', '- 1']:
            if expected not in explain_sql:
                raise AssertionError(f'Expected join-mode EXPLAIN to include {expected!r}, got: {explain_sql}')
        if "(SELECT" in explain_sql:
            raise AssertionError(f"join-mode explain should not contain scalar subqueries for bracket access: {explain_sql}")

        print("-- join-mode sample columns --")
        print(sample_columns)
        print("-- join-mode deep columns --")
        print(deep_columns)
        print("-- join-mode shallow paths --")
        print(shallow_rows)
        print("-- join-mode deep paths --")
        print(deep_rows)
        print("-- join-mode arrays --")
        print(array_rows)
        print("-- join-mode array predicate --")
        print(array_filter_rows)
        print("-- join-mode null semantics --")
        print(null_rows)
        print("-- join-mode dotted alias --")
        print(alias_rows)
        print("-- join-mode explain virtual --")
        print(explain_sql)
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

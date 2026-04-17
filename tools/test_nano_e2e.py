#!/usr/bin/env python3

from nano_support import bundle_adapter, connect, install_preprocessor, install_virtual_schema_fixture


DEEP_LEAF_PATH = '"chain.next.next.next.next.next.next.next.leaf_note"'
DEEP_READING_PATH = '"chain.next.next.next.next.next.next.next.reading"'
DEEP_ENTRY_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[2].value"'
DEEP_ENTRY_KIND_PATH = '"chain.next.next.next.next.next.next.next.entries[1].kind"'
DEEP_ENTRY_FIRST_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[FIRST].value"'
DEEP_ENTRY_LAST_VALUE_PATH = '"chain.next.next.next.next.next.next.next.entries[LAST].value"'
DEEP_ENTRY_SIZE_PATH = '"chain.next.next.next.next.next.next.next.entries[SIZE]"'
DEEP_ENTRY_ARRAY_PATH = 'd."chain.next.next.next.next.next.next.next.entries"'


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def assert_contains_all(text: str, fragments: list[str], label: str) -> None:
    missing = [fragment for fragment in fragments if fragment not in text]
    if missing:
        raise AssertionError(f"{label} missing fragments {missing!r}.\nActual text: {text}")


def main() -> None:
    adapter_code = bundle_adapter()
    con = connect()
    try:
        install_virtual_schema_fixture(con, adapter_code, include_deep_fixture=True)
        install_preprocessor(con, ["JSON_IS_EXPLICIT_NULL", "JNULL"], rewrite_path_identifiers=True)

        columns = con.execute("""
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'DEEPDOC'
            ORDER BY COLUMN_ORDINAL_POSITION
        """).fetchall()
        expected_subset = {"_id", "doc_id", "title", "profile|object", "chain|object", "tags|array", "metrics|array"}
        actual_columns = {name for (name,) in columns}
        missing_columns = sorted(expected_subset - actual_columns)
        if missing_columns:
            raise AssertionError(f"deep metadata missing columns: {missing_columns}")
        leaked_columns = sorted(name for name in actual_columns if name.startswith("__path__"))
        if leaked_columns:
            raise AssertionError(f"deep metadata should not expose synthetic path columns: {leaked_columns}")
        for forbidden in ["nickname|n", "leaf_note|n", "reading|string", "reading|n"]:
            if forbidden in actual_columns:
                raise AssertionError(f"deep metadata should hide physical helper column {forbidden!r}")

        deep_null_rows = con.execute(f"""
            SELECT
                CAST("doc_id" AS VARCHAR(10)),
                COALESCE("profile.prefs.theme", 'NULL'),
                CASE WHEN JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END,
                CASE WHEN "profile.nickname" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("profile.nickname") THEN '1' ELSE '0' END,
                COALESCE({DEEP_LEAF_PATH}, 'NULL'),
                CASE WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END,
                CASE WHEN {DEEP_LEAF_PATH} IS NULL AND NOT JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN '1' ELSE '0' END
            FROM JSON_VS.DEEPDOC
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(
            deep_null_rows,
            [
                ("101", "dark", "1", "0", "bottom", "0", "0"),
                ("102", "NULL", "0", "1", "NULL", "1", "0"),
                ("103", "NULL", "0", "1", "NULL", "0", "1"),
            ],
            "deep null semantics",
        )

        deep_variant_rows = con.execute(f"""
            SELECT
                CAST("doc_id" AS VARCHAR(10)),
                COALESCE(TYPEOF({DEEP_READING_PATH}), 'MISSING'),
                COALESCE(CAST({DEEP_READING_PATH} AS VARCHAR(100)), 'NULL'),
                COALESCE(CAST(CAST({DEEP_READING_PATH} AS DECIMAL(18,0)) AS VARCHAR(20)), 'NULL')
            FROM JSON_VS.DEEPDOC
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(
            deep_variant_rows,
            [
                ("101", "NUMBER", "100", "100"),
                ("102", "STRING", "101", "101"),
                ("103", "MISSING", "NULL", "NULL"),
            ],
            "deep variant behavior",
        )

        deep_array_rows = con.execute(f"""
            SELECT
                CAST("doc_id" AS VARCHAR(10)),
                COALESCE("tags[FIRST]", 'NULL') AS "tag_first",
                COALESCE("tags[LAST]", 'NULL') AS "tag_last",
                COALESCE(CAST("tags[SIZE]" AS VARCHAR(20)), 'NULL') AS "tag_size",
                COALESCE(CAST("metrics[1]" AS VARCHAR(20)), 'NULL') AS "metric1",
                COALESCE(CAST("metrics[LAST]" AS VARCHAR(20)), 'NULL') AS "metric_last",
                COALESCE(CAST("metrics[SIZE]" AS VARCHAR(20)), 'NULL') AS "metric_size",
                COALESCE({DEEP_ENTRY_VALUE_PATH}, 'NULL') AS "entry2_value",
                COALESCE({DEEP_ENTRY_KIND_PATH}, 'NULL') AS "entry1_kind",
                COALESCE({DEEP_ENTRY_FIRST_VALUE_PATH}, 'NULL') AS "entry_first_value",
                COALESCE({DEEP_ENTRY_LAST_VALUE_PATH}, 'NULL') AS "entry_last_value",
                COALESCE(CAST({DEEP_ENTRY_SIZE_PATH} AS VARCHAR(20)), 'NULL') AS "entry_size"
            FROM JSON_VS.DEEPDOC
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(
            deep_array_rows,
            [
                ("101", "alpha", "gamma", "3", "20", "30", "3", "e2", "mid", "e0", "e2", "3"),
                ("102", "delta", "delta", "1", "NULL", "7", "1", "NULL", "NULL", "other", "other", "1"),
                ("103", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"),
            ],
            "deep array access",
        )

        array_filter_rows = con.execute(f"""
            SELECT CAST("doc_id" AS VARCHAR(10))
            FROM JSON_VS.DEEPDOC
            WHERE "metrics[LAST]" = 30
               OR {DEEP_ENTRY_LAST_VALUE_PATH} = 'other'
               OR "tags[SIZE]" = 1
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(array_filter_rows, [("101",), ("102",)], "deep array filter query")

        nested_iteration_rows = con.execute(f"""
            SELECT
                CAST(d."doc_id" AS VARCHAR(10)),
                CAST(entry._index AS VARCHAR(10)),
                entry.value,
                entry.kind,
                COALESCE(CAST(extra._index AS VARCHAR(10)), 'NULL'),
                COALESCE(extra, 'NULL')
            FROM JSON_VS.DEEPDOC d
            JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
            LEFT JOIN VALUE extra IN entry."extras"
            ORDER BY d."doc_id", entry._index, extra._index
        """).fetchall()
        assert_equal(
            nested_iteration_rows,
            [
                ("101", "0", "e0", "root", "0", "x0"),
                ("101", "0", "e0", "root", "1", "x1"),
                ("101", "1", "e1", "mid", "NULL", "NULL"),
                ("101", "2", "e2", "tail", "0", "tail-extra"),
                ("102", "0", "other", "solo", "0", "solo-extra"),
            ],
            "nested array iteration query",
        )

        iteration_aggregate_rows = con.execute(f"""
            SELECT
                CAST(d."doc_id" AS VARCHAR(10)),
                COUNT(*),
                MAX(entry._index)
            FROM JSON_VS.DEEPDOC d
            JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
            GROUP BY d."doc_id"
            ORDER BY d."doc_id"
        """).fetchall()
        assert_equal(
            iteration_aggregate_rows,
            [("101", 3, 2), ("102", 1, 0)],
            "deep iteration aggregate query",
        )

        correlated_iteration_rows = con.execute(f"""
            SELECT CAST(d."doc_id" AS VARCHAR(10))
            FROM JSON_VS.DEEPDOC d
            WHERE EXISTS (
                SELECT 1
                FROM entry IN {DEEP_ENTRY_ARRAY_PATH}
                WHERE entry.kind = 'tail'
            )
               OR EXISTS (
                SELECT 1
                FROM entry IN {DEEP_ENTRY_ARRAY_PATH}
                LEFT JOIN VALUE extra IN entry."extras"
                WHERE extra = 'solo-extra'
            )
            ORDER BY d."doc_id"
        """).fetchall()
        assert_equal(correlated_iteration_rows, [("101",), ("102",)], "deep correlated iteration query")

        value_iteration_rows = con.execute("""
            SELECT
                CAST(d."doc_id" AS VARCHAR(10)),
                CAST(metric._index AS VARCHAR(10)),
                CAST(metric AS VARCHAR(10))
            FROM JSON_VS.DEEPDOC d
            JOIN VALUE metric IN d."metrics"
            WHERE metric >= 20
            ORDER BY d."doc_id", metric._index
        """).fetchall()
        assert_equal(
            value_iteration_rows,
            [("101", "1", "20"), ("101", "2", "30")],
            "deep scalar iteration query",
        )

        filtered_rows = con.execute(f"""
            SELECT CAST("doc_id" AS VARCHAR(10))
            FROM JSON_VS.DEEPDOC
            WHERE JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH})
               OR CAST({DEEP_READING_PATH} AS DECIMAL(18,0)) = 100
            ORDER BY "doc_id"
        """).fetchall()
        assert_equal(filtered_rows, [("101",), ("102",)], "deep filter query")

        aggregation_rows = con.execute(f"""
            SELECT
                COALESCE(TYPEOF({DEEP_READING_PATH}), 'MISSING') AS reading_type,
                COUNT(*)
            FROM JSON_VS.DEEPDOC
            GROUP BY COALESCE(TYPEOF({DEEP_READING_PATH}), 'MISSING')
            ORDER BY reading_type
        """).fetchall()
        assert_equal(
            aggregation_rows,
            [("MISSING", 1), ("NUMBER", 1), ("STRING", 1)],
            "deep aggregation query",
        )

        status_rows = con.execute(f"""
            SELECT
                CASE
                    WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN 'explicit-null'
                    WHEN {DEEP_LEAF_PATH} IS NULL THEN 'missing'
                    ELSE 'value'
                END AS leaf_status,
                COUNT(*)
            FROM JSON_VS.DEEPDOC
            GROUP BY
                CASE
                    WHEN JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH}) THEN 'explicit-null'
                    WHEN {DEEP_LEAF_PATH} IS NULL THEN 'missing'
                    ELSE 'value'
                END
            ORDER BY leaf_status
        """).fetchall()
        assert_equal(
            status_rows,
            [("explicit-null", 1), ("missing", 1), ("value", 1)],
            "deep null status aggregation",
        )

        explain_sql = con.execute(f"""
            EXPLAIN VIRTUAL
            SELECT
                "profile.prefs.theme",
                CASE WHEN JSON_IS_EXPLICIT_NULL("profile.nickname") THEN 1 ELSE 0 END,
                {DEEP_LEAF_PATH},
                TYPEOF({DEEP_READING_PATH}),
                "tags[SIZE]",
                CAST("metrics[LAST]" AS DECIMAL(18,0)),
                {DEEP_ENTRY_LAST_VALUE_PATH},
                entry._index,
                entry.value,
                extra
            FROM JSON_VS.DEEPDOC d
            JOIN entry IN {DEEP_ENTRY_ARRAY_PATH}
            LEFT JOIN VALUE extra IN entry."extras"
            WHERE JSON_IS_EXPLICIT_NULL({DEEP_LEAF_PATH})
               OR CAST({DEEP_READING_PATH} AS DECIMAL(18,0)) = 100
        """).fetchall()[0][1]
        assert_contains_all(
            explain_sql,
            [
                '"DEEPDOC_profile"',
                '"DEEPDOC_profile_prefs"',
                '"DEEPDOC_chain_next_next_next_next_next_next_next"',
                '"DEEPDOC_metrics_arr"',
                '"DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr"',
                '"DEEPDOC_chain_next_next_next_next_next_next_next_entries_arr_extras_arr"',
                '"nickname|n"',
                '"leaf_note|n"',
                '"reading|string"',
                '"tags|array"',
                '"metrics|array"',
                '"entries|array"',
                '"_parent"',
                '"_pos"',
                'LEFT OUTER JOIN',
                'CASE',
                '- 1',
            ],
            "deep explain virtual",
        )
        if "(SELECT" in explain_sql:
            raise AssertionError(f"deep explain should not contain scalar subqueries for array access: {explain_sql}")

        print("-- deep e2e metadata --")
        print(columns)
        print("-- deep null semantics --")
        print(deep_null_rows)
        print("-- deep variant behavior --")
        print(deep_variant_rows)
        print("-- deep array access --")
        print(deep_array_rows)
        print("-- deep array filter query --")
        print(array_filter_rows)
        print("-- deep nested iteration query --")
        print(nested_iteration_rows)
        print("-- deep iteration aggregate query --")
        print(iteration_aggregate_rows)
        print("-- deep correlated iteration query --")
        print(correlated_iteration_rows)
        print("-- deep scalar iteration query --")
        print(value_iteration_rows)
        print("-- deep filter query --")
        print(filtered_rows)
        print("-- deep aggregation query --")
        print(aggregation_rows)
        print("-- deep null status aggregation --")
        print(status_rows)
        print("-- deep explain virtual --")
        print(explain_sql)
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

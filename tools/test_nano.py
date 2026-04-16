#!/usr/bin/env python3

from nano_support import bundle_adapter, connect, install_virtual_schema_fixture, print_query_rows


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def main() -> None:
    adapter_code = bundle_adapter()
    con = connect()
    try:
        install_virtual_schema_fixture(con, adapter_code)

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

        select_star = con.execute('SELECT * FROM JSON_VS.SAMPLE ORDER BY "id"').fetchall()
        assert_equal(
            select_star,
            [
                (1, 1, "alpha", "x", 1, 10, 42, 10, 2, 2),
                (2, 2, "beta", None, None, 20, None, None, 1, 1),
                (3, 3, "gamma", None, None, None, None, None, None, None),
            ],
            "select star rows",
        )

        null_rows = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                CASE WHEN "note" IS NULL THEN '1' ELSE '0' END,
                CASE WHEN "child|object" IS NULL THEN '1' ELSE '0' END
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(null_rows, [("1", "0", "0"), ("2", "1", "1"), ("3", "1", "1")], "plain SQL null checks")

        variant_types = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                COALESCE(TYPEOF("value"), 'MISSING'),
                COALESCE(TYPEOF("shape"), 'MISSING')
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(
            variant_types,
            [("1", "NUMBER", "OBJECT"), ("2", "STRING", "ARRAY"), ("3", "NULL", "MISSING")],
            "variant typeof query",
        )

        variant_casts = con.execute("""
            SELECT
                CAST("id" AS VARCHAR(10)),
                COALESCE(CAST("value" AS VARCHAR(100)), 'NULL'),
                COALESCE(CAST(CAST("value" AS DECIMAL(18,0)) AS VARCHAR(20)), 'NULL'),
                COALESCE(CAST(CAST("shape" AS DECIMAL(18,0)) AS VARCHAR(20)), 'NULL')
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """).fetchall()
        assert_equal(
            variant_casts,
            [("1", "42", "42", "10"), ("2", "43", "43", "3"), ("3", "NULL", "NULL", "NULL")],
            "variant cast query",
        )

        explain_sql = con.execute("""
            EXPLAIN VIRTUAL
            SELECT TYPEOF("value"), CAST("value" AS VARCHAR(100)), TYPEOF("shape")
            FROM JSON_VS.SAMPLE
        """).fetchall()[0][1]
        for expected_fragment in ['"value|string"', '"shape|array"', 'CASE']:
            if expected_fragment not in explain_sql:
                raise AssertionError(
                    f'Expected EXPLAIN VIRTUAL SQL to include {expected_fragment!r}, got: {explain_sql}'
                )

        print_query_rows(con, "virtual schema columns", """
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
        """)
        print_query_rows(con, "select star", 'SELECT * FROM JSON_VS.SAMPLE ORDER BY "id"')
        print_query_rows(con, "plain SQL null checks", """
            SELECT
                CAST("id" AS VARCHAR(10)),
                CASE WHEN "note" IS NULL THEN '1' ELSE '0' END,
                CASE WHEN "child|object" IS NULL THEN '1' ELSE '0' END
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """)
        print_query_rows(con, "variant typeof", """
            SELECT
                CAST("id" AS VARCHAR(10)),
                COALESCE(TYPEOF("value"), 'MISSING'),
                COALESCE(TYPEOF("shape"), 'MISSING')
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """)
        print_query_rows(con, "variant casts", """
            SELECT
                CAST("id" AS VARCHAR(10)),
                COALESCE(CAST("value" AS VARCHAR(100)), 'NULL'),
                COALESCE(CAST(CAST("value" AS DECIMAL(18,0)) AS VARCHAR(20)), 'NULL'),
                COALESCE(CAST(CAST("shape" AS DECIMAL(18,0)) AS VARCHAR(20)), 'NULL')
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
        """)
        print("-- explain virtual --")
        print(explain_sql)
    finally:
        con.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path

from nano_support import ROOT, bundle_adapter, connect, install_preprocessor, install_virtual_schema_fixture


RESULT_PATH = ROOT / "dist" / "preprocessor_only_feasibility_results.json"


def fetchall(con, sql: str):
    return con.execute(sql).fetchall()


def capture_query(con, sql: str) -> dict:
    try:
        return {"ok": True, "rows": fetchall(con, sql)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def install_wrapper_views(con) -> None:
    statements = [
        "DROP SCHEMA IF EXISTS JVS_VIEW CASCADE",
        "CREATE SCHEMA JVS_VIEW",
        "OPEN SCHEMA JVS_VIEW",
        """
        CREATE OR REPLACE VIEW SAMPLE AS
        SELECT
          "_id",
          "id",
          "name",
          "note",
          "child|object",
          "meta|object",
          "value",
          "shape|object",
          "shape|array",
          "tags|array",
          "items|array"
        FROM JVS_SRC.SAMPLE
        """,
        'CREATE OR REPLACE VIEW "SAMPLE_child" AS SELECT "_id", "value" FROM JVS_SRC."SAMPLE_child"',
        """
        CREATE OR REPLACE VIEW "SAMPLE_meta" AS
        SELECT "_id", "info|object", "flag", "items|array"
        FROM JVS_SRC."SAMPLE_meta"
        """,
        """
        CREATE OR REPLACE VIEW "SAMPLE_meta_info" AS
        SELECT "_id", "note"
        FROM JVS_SRC."SAMPLE_meta_info"
        """,
        'CREATE OR REPLACE VIEW "SAMPLE_tags_arr" AS SELECT "_parent", "_pos", "_value" FROM JVS_SRC."SAMPLE_tags_arr"',
        'CREATE OR REPLACE VIEW "SAMPLE_items_arr" AS SELECT "_id", "_parent", "_pos", "value", "label" FROM JVS_SRC."SAMPLE_items_arr"',
        'CREATE OR REPLACE VIEW "SAMPLE_meta_items_arr" AS SELECT "_parent", "_pos", "value" FROM JVS_SRC."SAMPLE_meta_items_arr"',
    ]
    for stmt in statements:
        con.execute(stmt)


def install_probe_preprocessor(con) -> None:
    con.execute("DROP SCHEMA IF EXISTS JVS_PP_STUDY CASCADE")
    con.execute("CREATE SCHEMA JVS_PP_STUDY")
    con.execute(
        """CREATE OR REPLACE LUA PREPROCESSOR SCRIPT JVS_PP_STUDY.META_PROBE AS
local sqltext = sqlparsing.getsqltext()
if string.find(sqltext, "JVS_META_PROBE", 1, true) ~= nil then
    local result = query([[
        SELECT COUNT(*)
        FROM SYS.EXA_ALL_COLUMNS
        WHERE COLUMN_SCHEMA = 'JVS_SRC' AND COLUMN_TABLE = 'SAMPLE'
    ]])
    sqlparsing.setsqltext("SELECT " .. tostring(result[1][1]) .. " AS column_count")
end
/"""
    )


def install_explicit_null_view_preprocessor(con) -> None:
    con.execute("DROP SCHEMA IF EXISTS JVS_PP_VIEW CASCADE")
    con.execute("CREATE SCHEMA JVS_PP_VIEW")
    con.execute(
        """CREATE OR REPLACE LUA PREPROCESSOR SCRIPT JVS_PP_VIEW.VIEW_JSON_HELPERS AS
local sqltext = sqlparsing.getsqltext()
local rewritten = string.gsub(
    sqltext,
    'JSON_IS_EXPLICIT_NULL%("note"%)',
    [[EXISTS (
        SELECT 1
        FROM JVS_SRC.SAMPLE "__base"
        WHERE "__base"."_id" = "SAMPLE"."_id"
          AND "__base"."note|n" = TRUE
    )]]
)
sqlparsing.setsqltext(rewritten)
/"""
    )


def install_explicit_null_view_join_preprocessor(con) -> None:
    con.execute("DROP SCHEMA IF EXISTS JVS_PP_VIEW_JOIN CASCADE")
    con.execute("CREATE SCHEMA JVS_PP_VIEW_JOIN")
    con.execute(
        """CREATE OR REPLACE LUA PREPROCESSOR SCRIPT JVS_PP_VIEW_JOIN.VIEW_JSON_HELPERS AS
local sqltext = sqlparsing.getsqltext()
if string.find(sqltext, 'JSON_IS_EXPLICIT_NULL("note")', 1, true) ~= nil then
    sqltext = string.gsub(
        sqltext,
        'FROM%s+JVS_VIEW%.SAMPLE',
        'FROM JVS_VIEW.SAMPLE LEFT JOIN (SELECT "_id", "note|n" FROM JVS_SRC.SAMPLE) "__base" ON "__base"."_id" = "SAMPLE"."_id"',
        1
    )
    sqltext = string.gsub(
        sqltext,
        'JSON_IS_EXPLICIT_NULL%("note"%)',
        '("__base"."note|n" = TRUE)'
    )
end
sqlparsing.setsqltext(sqltext)
/"""
    )


def install_identity_udf(con) -> None:
    con.execute("DROP SCHEMA IF EXISTS JVS_UDF CASCADE")
    con.execute("CREATE SCHEMA JVS_UDF")
    con.execute(
        """CREATE OR REPLACE LUA SCALAR SCRIPT JVS_UDF.IDENTITY_VARCHAR(x VARCHAR(2000))
RETURNS VARCHAR(2000) AS
function run(ctx)
    return ctx.x
end
/"""
    )


def main() -> None:
    con = connect()
    try:
        install_virtual_schema_fixture(con, bundle_adapter())
        install_wrapper_views(con)
        install_identity_udf(con)

        base_columns = fetchall(
            con,
            """
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JVS_SRC' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
            """,
        )
        wrapper_columns = fetchall(
            con,
            """
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JVS_VIEW' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
            """,
        )
        virtual_columns = fetchall(
            con,
            """
            SELECT COLUMN_NAME
            FROM SYS.EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = 'JSON_VS' AND COLUMN_TABLE = 'SAMPLE'
            ORDER BY COLUMN_ORDINAL_POSITION
            """,
        )
        base_select_star = fetchall(con, 'SELECT * FROM JVS_SRC.SAMPLE ORDER BY "id"')
        wrapper_select_star = fetchall(con, 'SELECT * FROM JVS_VIEW.SAMPLE ORDER BY "id"')
        virtual_select_star = fetchall(con, 'SELECT * FROM JSON_VS.SAMPLE ORDER BY "id"')

        install_preprocessor(
            con,
            ["JSON_IS_EXPLICIT_NULL"],
            rewrite_path_identifiers=True,
            virtual_schemas=["JVS_VIEW"],
        )

        wrapper_path_rows = capture_query(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              COALESCE("child.value", 'NULL'),
              COALESCE("meta.info.note", 'NULL'),
              COALESCE("tags[LAST]", 'NULL')
            FROM JVS_VIEW.SAMPLE
            ORDER BY "id"
            """,
        )
        wrapper_rowset_rows = capture_query(
            con,
            """
            SELECT
              CAST(s."id" AS VARCHAR(10)),
              CAST(item._index AS VARCHAR(10)),
              item.value,
              item.label,
              JVS_UDF.IDENTITY_VARCHAR(item.value)
            FROM JVS_VIEW.SAMPLE s
            JOIN item IN s."items"
            ORDER BY s."id", item._index
            """,
        )
        wrapper_scalar_udf_rows = capture_query(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              JVS_UDF.IDENTITY_VARCHAR("name"),
              JVS_UDF.IDENTITY_VARCHAR("child.value")
            FROM JVS_VIEW.SAMPLE
            ORDER BY "id"
            """,
        )
        wrapper_builtin_typeof_rows = capture_query(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              TYPEOF("value"),
              COALESCE(CAST("value" AS VARCHAR(100)), 'NULL')
            FROM JVS_VIEW.SAMPLE
            ORDER BY "id"
            """,
        )
        virtual_builtin_typeof_rows = capture_query(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              TYPEOF("value"),
              COALESCE(CAST("value" AS VARCHAR(100)), 'NULL')
            FROM JSON_VS.SAMPLE
            ORDER BY "id"
            """,
        )

        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        install_probe_preprocessor(con)
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_PP_STUDY.META_PROBE")
        preprocessor_query_probe = capture_query(con, "SELECT 1 /*JVS_META_PROBE*/")

        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        install_explicit_null_view_preprocessor(con)
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_PP_VIEW.VIEW_JSON_HELPERS")
        explicit_null_rows = capture_query(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
              CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END
            FROM JVS_VIEW.SAMPLE
            ORDER BY "id"
            """,
        )

        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        install_explicit_null_view_join_preprocessor(con)
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_PP_VIEW_JOIN.VIEW_JSON_HELPERS")
        explicit_null_join_rows = capture_query(
            con,
            """
            SELECT
              CAST("id" AS VARCHAR(10)),
              CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END,
              CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END
            FROM JVS_VIEW.SAMPLE
            ORDER BY "id"
            """,
        )
        explicit_null_join_filter_rows = capture_query(
            con,
            """
            SELECT CAST("id" AS VARCHAR(10))
            FROM JVS_VIEW.SAMPLE
            WHERE JSON_IS_EXPLICIT_NULL("note")
            ORDER BY "id"
            """,
        )

        results = {
            "base_columns": base_columns,
            "wrapper_columns": wrapper_columns,
            "virtual_columns": virtual_columns,
            "base_select_star": base_select_star,
            "wrapper_select_star": wrapper_select_star,
            "virtual_select_star": virtual_select_star,
            "wrapper_path_rows": wrapper_path_rows,
            "wrapper_rowset_rows": wrapper_rowset_rows,
            "wrapper_scalar_udf_rows": wrapper_scalar_udf_rows,
            "wrapper_builtin_typeof_rows": wrapper_builtin_typeof_rows,
            "virtual_builtin_typeof_rows": virtual_builtin_typeof_rows,
            "preprocessor_query_probe": preprocessor_query_probe,
            "explicit_null_rows": explicit_null_rows,
            "explicit_null_join_rows": explicit_null_join_rows,
            "explicit_null_join_filter_rows": explicit_null_join_filter_rows,
        }
        RESULT_PATH.write_text(json.dumps(results, indent=2))

        print(json.dumps(results, indent=2))
        print(f"Wrote {RESULT_PATH}")
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

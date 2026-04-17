#!/usr/bin/env python3

from __future__ import annotations

import json
import time
from pathlib import Path

from generate_wrapper_preprocessor_sql import generate_wrapper_preprocessor_sql_text
from nano_support import ROOT, bundle_adapter, connect, install_preprocessor
from wrapper_package_tool import execute_generated_preprocessor_sql, execute_plain_sql_file
from wrapper_schema_support import generate_wrapper_artifacts


ROW_COUNT = 200000
RESULT_PATH = ROOT / "dist" / "preprocessor_only_performance_results.json"
SOURCE_SCHEMA = "JVS_PERF_SRC"
WRAPPER_SCHEMA = "JVS_PERF_VIEW"
HELPER_SCHEMA = "JVS_PERF_VIEW_INTERNAL"
WRAPPER_PP_SCHEMA = "JVS_PP_PERF_WRAP"
WRAPPER_PP_SCRIPT = "JSON_WRAPPER_PERF"
VIRTUAL_SCHEMA = "JSON_PERF_VS"
VIRTUAL_PP_SCHEMA = "JVS_PP"
VIRTUAL_PP_SCRIPT = "JSON_NULL_PREPROCESSOR"


def q(sql: str) -> str:
    return "\n".join(line.rstrip() for line in sql.strip().splitlines())


def fetchall(con, sql: str):
    return con.execute(sql).fetchall()


def install_perf_fixture(con) -> None:
    adapter_code = bundle_adapter()
    try:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
    except Exception:
        pass
    statements = [
        f"DROP FORCE VIRTUAL SCHEMA IF EXISTS {VIRTUAL_SCHEMA} CASCADE",
        "DROP SCHEMA IF EXISTS JVS_PERF_VS CASCADE",
        f"DROP SCHEMA IF EXISTS {WRAPPER_SCHEMA} CASCADE",
        f"DROP SCHEMA IF EXISTS {HELPER_SCHEMA} CASCADE",
        f"DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE",
        f"DROP SCHEMA IF EXISTS {WRAPPER_PP_SCHEMA} CASCADE",
        f"DROP SCHEMA IF EXISTS {VIRTUAL_PP_SCHEMA} CASCADE",
        f"CREATE SCHEMA {SOURCE_SCHEMA}",
        f"OPEN SCHEMA {SOURCE_SCHEMA}",
        q(
            f"""
            CREATE OR REPLACE TABLE BENCH_SAMPLE AS
            SELECT
              CAST(i AS DECIMAL(18,0)) AS "_id",
              CAST(i AS DECIMAL(18,0)) AS "id",
              CASE
                WHEN MOD(i, 10) = 0 THEN NULL
                WHEN MOD(i, 10) = 1 THEN NULL
                ELSE 'note-' || CAST(i AS VARCHAR(20))
              END AS "note",
              CASE WHEN MOD(i, 10) = 0 THEN TRUE ELSE FALSE END AS "note|n",
              CAST(i AS DECIMAL(18,0)) AS "child|object",
              CAST(i AS DECIMAL(18,0)) AS "meta|object",
              CASE WHEN MOD(i, 3) = 1 THEN CAST(i AS DECIMAL(18,0)) ELSE NULL END AS "value",
              CASE WHEN MOD(i, 3) = 2 THEN CAST(i AS VARCHAR(20)) ELSE NULL END AS "value|string",
              CASE WHEN MOD(i, 3) = 0 THEN TRUE ELSE FALSE END AS "value|n",
              CAST(2 AS DECIMAL(18,0)) AS "items|array"
            FROM VALUES BETWEEN 1 AND {ROW_COUNT} AS gen(i)
            """
        ),
        q(
            f"""
            CREATE OR REPLACE TABLE "BENCH_SAMPLE_child" AS
            SELECT
              CAST(i AS DECIMAL(18,0)) AS "_id",
              'child-' || CAST(i AS VARCHAR(20)) AS "value"
            FROM VALUES BETWEEN 1 AND {ROW_COUNT} AS gen(i)
            """
        ),
        q(
            f"""
            CREATE OR REPLACE TABLE "BENCH_SAMPLE_meta" AS
            SELECT
              CAST(i AS DECIMAL(18,0)) AS "_id",
              CAST(i AS DECIMAL(18,0)) AS "info|object"
            FROM VALUES BETWEEN 1 AND {ROW_COUNT} AS gen(i)
            """
        ),
        q(
            f"""
            CREATE OR REPLACE TABLE "BENCH_SAMPLE_meta_info" AS
            SELECT
              CAST(i AS DECIMAL(18,0)) AS "_id",
              CASE
                WHEN MOD(i, 5) = 0 THEN NULL
                ELSE 'deep-' || CAST(i AS VARCHAR(20))
              END AS "note"
            FROM VALUES BETWEEN 1 AND {ROW_COUNT} AS gen(i)
            """
        ),
        q(
            f"""
            CREATE OR REPLACE TABLE "BENCH_SAMPLE_items_arr" AS
            SELECT
              CAST(i * 10 + p AS DECIMAL(18,0)) AS "_id",
              CAST(i AS DECIMAL(18,0)) AS "_parent",
              CAST(p - 1 AS DECIMAL(18,0)) AS "_pos",
              CASE
                WHEN p = 1 THEN 'first-' || CAST(i AS VARCHAR(20))
                ELSE 'second-' || CAST(i AS VARCHAR(20))
              END AS "value",
              CASE
                WHEN p = 1 THEN 'A'
                ELSE 'B'
              END AS "label"
            FROM VALUES BETWEEN 1 AND {ROW_COUNT} AS gen(i)
            CROSS JOIN VALUES BETWEEN 1 AND 2 AS pos(p)
            """
        ),
        "COMMIT",
        "CREATE SCHEMA JVS_PERF_VS",
        "OPEN SCHEMA JVS_PERF_VS",
        "CREATE OR REPLACE LUA ADAPTER SCRIPT JSON_PERF_VS_ADAPTER AS\n" + adapter_code + "\n/",
        f'CREATE VIRTUAL SCHEMA {VIRTUAL_SCHEMA} USING "JVS_PERF_VS"."JSON_PERF_VS_ADAPTER" WITH SCHEMA_NAME=\'{SOURCE_SCHEMA}\'',
        "COMMIT",
    ]
    for stmt in statements:
        con.execute(stmt)

    wrapper_artifacts = generate_wrapper_artifacts(con, SOURCE_SCHEMA, WRAPPER_SCHEMA, HELPER_SCHEMA)
    execute_plain_sql_file(con, wrapper_artifacts.sql)
    wrapper_preprocessor_sql = generate_wrapper_preprocessor_sql_text(
        schema=WRAPPER_PP_SCHEMA,
        script=WRAPPER_PP_SCRIPT,
        wrapper_schemas=[WRAPPER_SCHEMA],
        helper_schemas=[HELPER_SCHEMA],
        manifests=[wrapper_artifacts.manifest],
    )
    execute_generated_preprocessor_sql(con, wrapper_preprocessor_sql)


def install_virtual_perf_preprocessor(con) -> None:
    install_preprocessor(
        con,
        ["JSON_IS_EXPLICIT_NULL"],
        rewrite_path_identifiers=True,
        virtual_schemas=[VIRTUAL_SCHEMA],
    )
    con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")


def set_preprocessor(con, mode: str | None) -> None:
    if mode is None:
        con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
    elif mode == "wrapper":
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {WRAPPER_PP_SCHEMA}.{WRAPPER_PP_SCRIPT}")
    elif mode == "virtual":
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {VIRTUAL_PP_SCHEMA}.{VIRTUAL_PP_SCRIPT}")
    else:
        raise ValueError(f"Unknown preprocessor mode: {mode}")


def explain_virtual_query(con, sql: str) -> dict:
    try:
        rows = fetchall(con, "EXPLAIN VIRTUAL " + sql)
        return {"ok": True, "rows": rows}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def summarize_profile_rows(rows) -> dict:
    parts = []
    totals = {
        "duration_sum": 0.0,
        "compile_execute_duration": 0.0,
        "index_create_duration": 0.0,
        "join_duration": 0.0,
        "outer_join_duration": 0.0,
        "scan_duration": 0.0,
        "pushdown_duration": 0.0,
        "part_names": [],
    }
    for row in rows:
        part = {
            "part_name": row[0],
            "part_info": row[1],
            "object_schema": row[2],
            "object_name": row[3],
            "object_rows": row[4],
            "out_rows": row[5],
            "duration": float(row[6]) if row[6] is not None else 0.0,
            "remarks": row[7],
        }
        parts.append(part)
        totals["duration_sum"] += part["duration"]
        totals["part_names"].append(part["part_name"])
        if part["part_name"] == "COMPILE / EXECUTE":
            totals["compile_execute_duration"] += part["duration"]
        if part["part_name"] == "INDEX CREATE":
            totals["index_create_duration"] += part["duration"]
        if part["part_name"] == "JOIN":
            totals["join_duration"] += part["duration"]
        if part["part_name"] == "OUTER JOIN":
            totals["outer_join_duration"] += part["duration"]
        if part["part_name"] == "SCAN":
            totals["scan_duration"] += part["duration"]
        if part["part_name"] == "PUSHDOWN":
            totals["pushdown_duration"] += part["duration"]
    totals["has_index_create"] = totals["index_create_duration"] > 0
    return {"parts": parts, "totals": totals}


def profile_query(con, label: str, sql: str) -> dict:
    tagged_sql = f"/* PERF:{label} */\n" + sql.strip()
    con.execute("ALTER SESSION SET PROFILE='ON'")
    started = time.perf_counter()
    rows = fetchall(con, tagged_sql)
    wall_time = time.perf_counter() - started
    con.execute("ALTER SESSION SET PROFILE='OFF'")
    con.execute("FLUSH STATISTICS")
    stmt_row = fetchall(
        con,
        q(
            f"""
            SELECT MAX(STMT_ID)
            FROM EXA_USER_PROFILE_LAST_DAY
            WHERE SESSION_ID = CURRENT_SESSION
              AND SQL_TEXT LIKE '%PERF:{label}%'
            """
        ),
    )[0]
    stmt_id = stmt_row[0]
    if stmt_id is None:
        raise RuntimeError(f"Could not find profile rows for {label}")
    profile_rows = fetchall(
        con,
        q(
            f"""
            SELECT
              PART_NAME,
              PART_INFO,
              OBJECT_SCHEMA,
              OBJECT_NAME,
              OBJECT_ROWS,
              OUT_ROWS,
              DURATION,
              REMARKS
            FROM EXA_USER_PROFILE_LAST_DAY
            WHERE SESSION_ID = CURRENT_SESSION
              AND STMT_ID = {stmt_id}
            ORDER BY PART_ID
            """
        ),
    )
    con.execute("COMMIT")
    return {
        "label": label,
        "row_count": len(rows),
        "rows": rows,
        "wall_time_seconds": wall_time,
        "stmt_id": stmt_id,
        "profile": summarize_profile_rows(profile_rows),
    }


def run_cold_warm(con, name: str, sql: str, *, preprocessor_mode: str | None) -> dict:
    set_preprocessor(con, preprocessor_mode)
    cold = profile_query(con, f"{name}_cold", sql)
    set_preprocessor(con, preprocessor_mode)
    warm = profile_query(con, f"{name}_warm", sql)
    return {"cold": cold, "warm": warm}


def extract_summary_metric(result: dict) -> dict[str, float]:
    return {
        "cold_wall": result["cold"]["wall_time_seconds"],
        "warm_wall": result["warm"]["wall_time_seconds"],
        "cold_profile_total": result["cold"]["profile"]["totals"]["duration_sum"],
        "warm_profile_total": result["warm"]["profile"]["totals"]["duration_sum"],
        "cold_index_create": result["cold"]["profile"]["totals"]["index_create_duration"],
        "warm_index_create": result["warm"]["profile"]["totals"]["index_create_duration"],
        "cold_pushdown": result["cold"]["profile"]["totals"]["pushdown_duration"],
        "warm_pushdown": result["warm"]["profile"]["totals"]["pushdown_duration"],
    }


def main() -> None:
    con = connect()
    try:
        install_perf_fixture(con)
        con.execute("ALTER SESSION SET QUERY_CACHE='OFF'")
        install_virtual_perf_preprocessor(con)

        path_raw_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_SRC.BENCH_SAMPLE s
            LEFT JOIN JVS_PERF_SRC."BENCH_SAMPLE_child" c
              ON s."child|object" = c."_id"
            LEFT JOIN JVS_PERF_SRC."BENCH_SAMPLE_meta" m
              ON s."meta|object" = m."_id"
            LEFT JOIN JVS_PERF_SRC."BENCH_SAMPLE_meta_info" mi
              ON m."info|object" = mi."_id"
            WHERE c."value" IS NOT NULL
              AND mi."note" IS NOT NULL
            """
        )
        path_wrapper_sugar_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_VIEW.BENCH_SAMPLE
            WHERE "child.value" IS NOT NULL
              AND "meta.info.note" IS NOT NULL
            """
        )
        path_virtual_sugar_sql = q(
            """
            SELECT COUNT(*)
            FROM JSON_PERF_VS.BENCH_SAMPLE
            WHERE "child.value" IS NOT NULL
              AND "meta.info.note" IS NOT NULL
            """
        )
        rowset_raw_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_SRC.BENCH_SAMPLE s
            JOIN JVS_PERF_SRC."BENCH_SAMPLE_items_arr" item
              ON s."_id" = item."_parent"
            WHERE item."_pos" >= 0
            """
        )
        rowset_wrapper_sugar_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_VIEW.BENCH_SAMPLE s
            JOIN item IN s."items"
            WHERE item._index >= 0
            """
        )
        rowset_virtual_sugar_sql = q(
            """
            SELECT COUNT(*)
            FROM JSON_PERF_VS.BENCH_SAMPLE s
            JOIN item IN s."items"
            WHERE item._index >= 0
            """
        )
        explicit_null_raw_direct_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_SRC.BENCH_SAMPLE s
            WHERE s."note|n" = TRUE
            """
        )
        explicit_null_wrapper_helper_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_VIEW.BENCH_SAMPLE
            WHERE JSON_IS_EXPLICIT_NULL("note")
            """
        )
        explicit_null_virtual_helper_sql = q(
            """
            SELECT COUNT(*)
            FROM JSON_PERF_VS.BENCH_SAMPLE
            WHERE JSON_IS_EXPLICIT_NULL("note")
            """
        )
        variant_type_raw_direct_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_SRC.BENCH_SAMPLE s
            WHERE CASE
              WHEN s."value" IS NOT NULL THEN 'NUMBER'
              WHEN s."value|string" IS NOT NULL THEN 'STRING'
              WHEN s."value|n" = TRUE THEN 'NULL'
              ELSE NULL
            END = 'STRING'
            """
        )
        variant_type_wrapper_helper_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_VIEW.BENCH_SAMPLE
            WHERE JSON_TYPEOF("value") = 'STRING'
            """
        )
        variant_type_virtual_builtin_sql = q(
            """
            SELECT COUNT(*)
            FROM JSON_PERF_VS.BENCH_SAMPLE
            WHERE TYPEOF("value") = 'STRING'
            """
        )
        variant_extract_raw_direct_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_SRC.BENCH_SAMPLE s
            WHERE COALESCE(CAST(s."value" AS VARCHAR(20)), s."value|string") IS NOT NULL
            """
        )
        variant_extract_wrapper_helper_sql = q(
            """
            SELECT COUNT(*)
            FROM JVS_PERF_VIEW.BENCH_SAMPLE
            WHERE JSON_AS_VARCHAR("value") IS NOT NULL
            """
        )
        variant_extract_virtual_builtin_sql = q(
            """
            SELECT COUNT(*)
            FROM JSON_PERF_VS.BENCH_SAMPLE
            WHERE CAST("value" AS VARCHAR(100)) IS NOT NULL
            """
        )

        set_preprocessor(con, "virtual")

        explain = {
            "path_virtual_sugar": explain_virtual_query(con, path_virtual_sugar_sql),
            "rowset_virtual_sugar": explain_virtual_query(con, rowset_virtual_sugar_sql),
            "explicit_null_virtual_helper": explain_virtual_query(con, explicit_null_virtual_helper_sql),
            "variant_type_virtual_builtin": explain_virtual_query(con, variant_type_virtual_builtin_sql),
            "variant_extract_virtual_builtin": explain_virtual_query(con, variant_extract_virtual_builtin_sql),
        }

        benchmarks = {
            "path_raw_manual": run_cold_warm(con, "path_raw_manual", path_raw_sql, preprocessor_mode=None),
            "path_wrapper_sugar": run_cold_warm(con, "path_wrapper_sugar", path_wrapper_sugar_sql, preprocessor_mode="wrapper"),
            "path_virtual_sugar": run_cold_warm(con, "path_virtual_sugar", path_virtual_sugar_sql, preprocessor_mode="virtual"),
            "rowset_raw_manual": run_cold_warm(con, "rowset_raw_manual", rowset_raw_sql, preprocessor_mode=None),
            "rowset_wrapper_sugar": run_cold_warm(con, "rowset_wrapper_sugar", rowset_wrapper_sugar_sql, preprocessor_mode="wrapper"),
            "rowset_virtual_sugar": run_cold_warm(con, "rowset_virtual_sugar", rowset_virtual_sugar_sql, preprocessor_mode="virtual"),
            "explicit_null_raw_direct": run_cold_warm(con, "explicit_null_raw_direct", explicit_null_raw_direct_sql, preprocessor_mode=None),
            "explicit_null_wrapper_helper": run_cold_warm(con, "explicit_null_wrapper_helper", explicit_null_wrapper_helper_sql, preprocessor_mode="wrapper"),
            "explicit_null_virtual_helper": run_cold_warm(con, "explicit_null_virtual_helper", explicit_null_virtual_helper_sql, preprocessor_mode="virtual"),
            "variant_type_raw_direct": run_cold_warm(con, "variant_type_raw_direct", variant_type_raw_direct_sql, preprocessor_mode=None),
            "variant_type_wrapper_helper": run_cold_warm(con, "variant_type_wrapper_helper", variant_type_wrapper_helper_sql, preprocessor_mode="wrapper"),
            "variant_type_virtual_builtin": run_cold_warm(con, "variant_type_virtual_builtin", variant_type_virtual_builtin_sql, preprocessor_mode="virtual"),
            "variant_extract_raw_direct": run_cold_warm(con, "variant_extract_raw_direct", variant_extract_raw_direct_sql, preprocessor_mode=None),
            "variant_extract_wrapper_helper": run_cold_warm(con, "variant_extract_wrapper_helper", variant_extract_wrapper_helper_sql, preprocessor_mode="wrapper"),
            "variant_extract_virtual_builtin": run_cold_warm(con, "variant_extract_virtual_builtin", variant_extract_virtual_builtin_sql, preprocessor_mode="virtual"),
        }

        isolated_cold = {}
        isolated_cases = [
            ("path_wrapper_sugar", "wrapper"),
            ("path_virtual_sugar", "virtual"),
            ("rowset_wrapper_sugar", "wrapper"),
            ("rowset_virtual_sugar", "virtual"),
            ("explicit_null_wrapper_helper", "wrapper"),
            ("explicit_null_virtual_helper", "virtual"),
            ("variant_type_wrapper_helper", "wrapper"),
            ("variant_type_virtual_builtin", "virtual"),
            ("variant_extract_wrapper_helper", "wrapper"),
            ("variant_extract_virtual_builtin", "virtual"),
        ]
        case_sql = {
            "path_wrapper_sugar": path_wrapper_sugar_sql,
            "path_virtual_sugar": path_virtual_sugar_sql,
            "rowset_wrapper_sugar": rowset_wrapper_sugar_sql,
            "rowset_virtual_sugar": rowset_virtual_sugar_sql,
            "explicit_null_wrapper_helper": explicit_null_wrapper_helper_sql,
            "explicit_null_virtual_helper": explicit_null_virtual_helper_sql,
            "variant_type_wrapper_helper": variant_type_wrapper_helper_sql,
            "variant_type_virtual_builtin": variant_type_virtual_builtin_sql,
            "variant_extract_wrapper_helper": variant_extract_wrapper_helper_sql,
            "variant_extract_virtual_builtin": variant_extract_virtual_builtin_sql,
        }
        for name, mode in isolated_cases:
            install_perf_fixture(con)
            con.execute("ALTER SESSION SET QUERY_CACHE='OFF'")
            install_virtual_perf_preprocessor(con)
            set_preprocessor(con, mode)
            isolated_cold[name] = profile_query(con, f"{name}_isolated_cold", case_sql[name])

        summary = {name: extract_summary_metric(result) for name, result in benchmarks.items()}

        results = {
            "row_count": ROW_COUNT,
            "explain": explain,
            "benchmarks": benchmarks,
            "isolated_cold": isolated_cold,
            "summary": summary,
        }
        RESULT_PATH.write_text(json.dumps(results, indent=2))
        print(json.dumps(results, indent=2))
        print(f"Wrote {RESULT_PATH}")
    finally:
        try:
            con.execute("ALTER SESSION SET PROFILE='OFF'")
        except Exception:
            pass
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

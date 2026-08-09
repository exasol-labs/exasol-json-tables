#!/usr/bin/env python3

from __future__ import annotations

import json
import _bootstrap  # noqa: F401

from nano_support import ROOT, connect, install_source_fixture, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_ACCESS"
HELPER_SCHEMA = "JSON_VIEW_ACCESS_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_ACCESS_PP"
PREPROCESSOR_SCRIPT = "JSON_ACCESS_PREPROCESSOR"
PUBLISHED_SCHEMA = "JVS_PUBLISHED"
SECOND_SOURCE_SCHEMA = "JVS_SRC_ORDERS"
SECOND_WRAPPER_SCHEMA = "JSON_VIEW_ORDERS"
SECOND_HELPER_SCHEMA = "JSON_VIEW_ORDERS_INTERNAL"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def parse_json_rows(rows) -> list[object]:
    return [json.loads(row[0]) for row in rows]


def main() -> None:
    con = connect()
    try:
        con.execute(f"DROP SCHEMA IF EXISTS {PUBLISHED_SCHEMA} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {SECOND_SOURCE_SCHEMA} CASCADE")
        install_source_fixture(con, include_deep_fixture=False)
        first_manifest = install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
            generate_preprocessor=True,
            preprocessor_schema=PREPROCESSOR_SCHEMA,
            preprocessor_script=PREPROCESSOR_SCRIPT,
        )
        first_manifest_path = ROOT / "dist" / "json_wrapper_manifest_access_primary.json"
        first_manifest_path.write_text(json.dumps(first_manifest))

        con.execute(f"CREATE SCHEMA {SECOND_SOURCE_SCHEMA}")
        con.execute(
            f'''
            CREATE TABLE {SECOND_SOURCE_SCHEMA}.ORDERS (
              "_id" DECIMAL(18,0) NOT NULL,
              "customer_id" DECIMAL(18,0),
              "status" VARCHAR(100),
              "line_items|array" DECIMAL(18,0)
            )
            '''
        )
        con.execute(
            f'''
            CREATE TABLE {SECOND_SOURCE_SCHEMA}."ORDERS_line_items_arr" (
              "_id" DECIMAL(18,0) NOT NULL,
              "_parent" DECIMAL(18,0) NOT NULL,
              "_pos" DECIMAL(18,0) NOT NULL,
              "sku" VARCHAR(100)
            )
            '''
        )
        con.execute(f"INSERT INTO {SECOND_SOURCE_SCHEMA}.ORDERS VALUES (10, 1, 'open', 2), (20, 2, 'closed', 1)")
        con.execute(
            f'''
            INSERT INTO {SECOND_SOURCE_SCHEMA}."ORDERS_line_items_arr" VALUES
              (101, 10, 0, 'A'), (102, 10, 1, 'B'), (201, 20, 0, 'C')
            '''
        )
        second_manifest = install_wrapper_views(
            con,
            source_schema=SECOND_SOURCE_SCHEMA,
            wrapper_schema=SECOND_WRAPPER_SCHEMA,
            helper_schema=SECOND_HELPER_SCHEMA,
        )
        second_manifest_path = ROOT / "dist" / "json_wrapper_manifest_access_orders.json"
        second_manifest_path.write_text(json.dumps(second_manifest))
        install_wrapper_preprocessor(
            con,
            [WRAPPER_SCHEMA, SECOND_WRAPPER_SCHEMA],
            [HELPER_SCHEMA, SECOND_HELPER_SCHEMA],
            [first_manifest_path, second_manifest_path],
            schema_name=PREPROCESSOR_SCHEMA,
            script_name=PREPROCESSOR_SCRIPT,
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    activation_sql = f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}"

    bootstrap_con = connect()
    try:
        bootstrap_con.execute(activation_sql)
        bootstrap_rows = bootstrap_con.execute(
            f"""
            SELECT
              CAST("id" AS VARCHAR(10)),
              COALESCE("meta.info.note", 'NULL'),
              COALESCE("tags[LAST]", 'NULL')
            FROM {WRAPPER_SCHEMA}.SAMPLE
            ORDER BY "id"
            """
        ).fetchall()
    finally:
        try:
            bootstrap_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        bootstrap_con.close()
    assert_equal(
        bootstrap_rows,
        [("1", "deep", "blue"), ("2", "NULL", "green"), ("3", "NULL", "NULL")],
        "connection-bootstrap wrapper query",
    )

    multi_collection_con = connect()
    try:
        multi_collection_con.execute(activation_sql)
        multi_collection_rows = multi_collection_con.execute(
            f'''
            SELECT
              CAST(customer."id" AS VARCHAR(10)),
              COALESCE(customer."meta.info.note", 'NULL'),
              JSON_TYPEOF(customer."value"),
              JSON_TYPEOF(orders."status"),
              JSON_AS_VARCHAR(orders."status"),
              CAST(orders."line_items[SIZE]" AS VARCHAR(10)),
              orders."line_items[LAST].sku"
            FROM {WRAPPER_SCHEMA}.SAMPLE customer
            JOIN {SECOND_WRAPPER_SCHEMA}.ORDERS orders
              ON customer."id" = orders."customer_id"
            ORDER BY customer."id"
            ''',
        ).fetchall()
    finally:
        try:
            multi_collection_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        multi_collection_con.close()
    assert_equal(
        multi_collection_rows,
        [
            ("1", "deep", "NUMBER", "STRING", "open", "2", "B"),
            ("2", "NULL", "STRING", "STRING", "closed", "1", "C"),
        ],
        "BUG-065 combined-preprocessor helpers on both join sides",
    )

    authoring_con = connect()
    try:
        authoring_con.execute(activation_sql)
        authoring_con.execute(f"DROP SCHEMA IF EXISTS {PUBLISHED_SCHEMA} CASCADE")
        authoring_con.execute(f"CREATE SCHEMA {PUBLISHED_SCHEMA}")
        authoring_con.execute(
            f"""
            CREATE OR REPLACE VIEW {PUBLISHED_SCHEMA}.SAMPLE_PUBLISHED AS
            SELECT
              CAST("id" AS VARCHAR(10)) AS doc_id,
              COALESCE("meta.info.note", 'NULL') AS deep_note,
              TO_JSON("id", "meta", "tags") AS doc_json
            FROM {WRAPPER_SCHEMA}.SAMPLE
            """
        )
    finally:
        try:
            authoring_con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        authoring_con.close()

    consumer_con = connect()
    try:
        published_rows = consumer_con.execute(
            f"""
            SELECT
              doc_id,
              deep_note,
              doc_json
            FROM {PUBLISHED_SCHEMA}.SAMPLE_PUBLISHED
            ORDER BY doc_id
            """
        ).fetchall()
    finally:
        consumer_con.close()

    assert_equal(
        [(row[0], row[1]) for row in published_rows],
        [("1", "deep"), ("2", "NULL"), ("3", "NULL")],
        "published view scalar rows",
    )
    assert_equal(
        [row["id"] for row in parse_json_rows([(row[2],) for row in published_rows])],
        [1, 2, 3],
        "published view JSON rows",
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal

import _bootstrap  # noqa: F401

from in_session_wrapper_installer import install_wrapper_surface_in_session
from nano_support import connect, install_wrapper_preprocessor, install_wrapper_views
from result_family_json_export import export_root_family_to_json
from result_family_materializer import materialize_result_family, result_family_spec_from_dict


SOURCE_SCHEMA = "JVS_MONGO_R3_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_MONGO_R3"
HELPER_SCHEMA = "JSON_VIEW_MONGO_R3_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_WRAP_MONGO_R3_PP"
PREPROCESSOR_SCRIPT = "JSON_WRAPPER_MONGO_R3_PREPROCESSOR"


def normalize_value(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value


def normalize_data(value):
    if isinstance(value, tuple):
        return [normalize_data(item) for item in value]
    if isinstance(value, list):
        return [normalize_data(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_data(item) for key, item in value.items()}
    return normalize_value(value)


def run_sql(con, sql: str):
    return normalize_data(con.execute(sql).fetchall())


def run_sql_or_error(con, sql: str) -> dict[str, object]:
    try:
        return {"status": "rows", "rows": run_sql(con, sql)}
    except Exception as exc:  # pragma: no cover - study harness
        return {"status": "error", "error": str(exc)}


def activate_source_preprocessor(con) -> None:
    con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}")


def install_source_fixture(con) -> None:
    statements = [
        f"DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE",
        f"CREATE SCHEMA {SOURCE_SCHEMA}",
        f"OPEN SCHEMA {SOURCE_SCHEMA}",
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.PRODUCTS (
          "_id" DECIMAL(18,0) NOT NULL,
          "name" VARCHAR(100),
          "variation" VARCHAR(100),
          "category" VARCHAR(100),
          "description" VARCHAR(200)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.ORDERS (
          "_id" DECIMAL(18,0) NOT NULL,
          "order_id" DECIMAL(18,0),
          "customer_id" VARCHAR(100),
          "orderdate" TIMESTAMP,
          "region" VARCHAR(50),
          "status" VARCHAR(50),
          "channel" VARCHAR(50),
          "products|array" DECIMAL(18,0)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}."ORDERS_products_arr" (
          "_id" DECIMAL(18,0) NOT NULL,
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "prod_id" VARCHAR(50),
          "name" VARCHAR(100),
          "variation" VARCHAR(100),
          "category" VARCHAR(100),
          "price" DECIMAL(18,2),
          "quantity" DECIMAL(18,0)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.ARTWORK (
          "_id" DECIMAL(18,0) NOT NULL,
          "title" VARCHAR(200),
          "artist" VARCHAR(100),
          "year" DECIMAL(18,0),
          "price" DECIMAL(18,2),
          "tags|array" DECIMAL(18,0)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}."ARTWORK_tags_arr" (
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "_value" VARCHAR(100)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.SALES (
          "_id" DECIMAL(18,0) NOT NULL,
          "sale_id" DECIMAL(18,0),
          "store" VARCHAR(50),
          "items|array" DECIMAL(18,0)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}."SALES_items_arr" (
          "_id" DECIMAL(18,0) NOT NULL,
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "item_id" DECIMAL(18,0),
          "quantity" DECIMAL(18,0),
          "price" DECIMAL(18,2),
          "name" VARCHAR(100)
        )
        """,
        f"""
        CREATE OR REPLACE TABLE {SOURCE_SCHEMA}.CAKESALES (
          "_id" DECIMAL(18,0) NOT NULL,
          "type" VARCHAR(50),
          "orderDate" TIMESTAMP,
          "state" VARCHAR(10),
          "price" DECIMAL(18,2),
          "quantity" DECIMAL(18,0)
        )
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}.PRODUCTS VALUES
          (1, 'Asus Laptop', 'Ultra HD', 'ELECTRONICS', 'Great for watching movies'),
          (2, 'Asus Laptop', 'Standard Display', 'ELECTRONICS', 'Budget display'),
          (3, 'Morphy Richards Food Mixer', 'Deluxe', 'KITCHENWARE', 'Luxury mixer turning good cakes into great'),
          (4, 'Russell Hobbs Chrome Kettle', 'Standard', 'KITCHENWARE', 'Stylish kettle'),
          (5, 'Karcher Hose Set', 'Standard', 'GARDEN', 'Garden hose kit'),
          (6, 'The Day Of The Triffids', '1st Edition', 'BOOKS', 'Classic post-apocalyptic novel')
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}.ORDERS VALUES
          (1, 1001, 'alice@example.com', TIMESTAMP '2020-01-03 10:15:00', 'EMEA', 'shipped', 'web', 2),
          (2, 1002, 'bob@example.com', TIMESTAMP '2020-02-10 11:00:00', 'EMEA', 'shipped', 'web', 1),
          (3, 1003, 'alice@example.com', TIMESTAMP '2020-03-21 12:20:00', 'AMER', 'returned', 'mobile', 1),
          (4, 1004, 'cara@example.com', TIMESTAMP '2021-01-05 09:00:00', 'APAC', 'shipped', 'web', 2),
          (5, 1005, 'dan@example.com', TIMESTAMP '2020-06-01 14:45:00', 'EMEA', 'pending', 'partner', 1),
          (6, 1006, 'bob@example.com', TIMESTAMP '2020-11-07 16:30:00', 'EMEA', 'shipped', 'web', 2)
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}."ORDERS_products_arr" VALUES
          (101, 1, 0, 'abc12345', 'Asus Laptop', 'Ultra HD', 'ELECTRONICS', 430.00, 1),
          (102, 1, 1, 'def45678', 'Karcher Hose Set', 'Standard', 'GARDEN', 22.00, 2),
          (103, 2, 0, 'mrf88223', 'Morphy Richards Food Mixer', 'Deluxe', 'KITCHENWARE', 215.00, 1),
          (104, 3, 0, 'xyz11228', 'Russell Hobbs Chrome Kettle', 'Standard', 'KITCHENWARE', 80.00, 1),
          (105, 4, 0, 'abc12345', 'Asus Laptop', 'Ultra HD', 'ELECTRONICS', 430.00, 1),
          (106, 4, 1, 'xyz11228', 'Russell Hobbs Chrome Kettle', 'Standard', 'KITCHENWARE', 80.00, 1),
          (107, 5, 0, 'abc12346', 'Asus Laptop', 'Standard Display', 'ELECTRONICS', 350.00, 1),
          (108, 6, 0, 'def45678', 'Karcher Hose Set', 'Standard', 'GARDEN', 22.00, 1),
          (109, 6, 1, 'xyz11228', 'Russell Hobbs Chrome Kettle', 'Standard', 'KITCHENWARE', 80.00, 1)
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}.ARTWORK VALUES
          (1, 'The Pillars of Society', 'Grosz', 1926, 199.99, 4),
          (2, 'Melancholy III', 'Munch', 1902, 280.00, 2),
          (3, 'Dancer', 'Miro', 1925, 76.04, 3),
          (4, 'The Great Wave off Kanagawa', 'Hokusai', NULL, 167.30, 2),
          (5, 'The Persistence of Memory', 'Dali', 1931, 483.00, 3),
          (6, 'Composition VII', 'Kandinsky', 1913, 385.00, 3),
          (7, 'The Scream', 'Munch', 1893, NULL, 3),
          (8, 'Blue Flower', 'O''Keefe', 1918, 118.42, 2)
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}."ARTWORK_tags_arr" VALUES
          (1, 0, 'painting'),
          (1, 1, 'satire'),
          (1, 2, 'Expressionism'),
          (1, 3, 'caricature'),
          (2, 0, 'woodcut'),
          (2, 1, 'Expressionism'),
          (3, 0, 'oil'),
          (3, 1, 'Surrealism'),
          (3, 2, 'painting'),
          (4, 0, 'woodblock'),
          (4, 1, 'ukiyo-e'),
          (5, 0, 'Surrealism'),
          (5, 1, 'painting'),
          (5, 2, 'oil'),
          (6, 0, 'oil'),
          (6, 1, 'painting'),
          (6, 2, 'abstract'),
          (7, 0, 'Expressionism'),
          (7, 1, 'painting'),
          (7, 2, 'oil'),
          (8, 0, 'abstract'),
          (8, 1, 'painting')
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}.SALES VALUES
          (1, 2001, 'north', 2),
          (2, 2002, 'south', 3),
          (3, 2003, 'west', 0)
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}."SALES_items_arr" VALUES
          (201, 1, 0, 43, 2, 10.00, 'pen'),
          (202, 1, 1, 2, 1, 240.00, 'briefcase'),
          (203, 2, 0, 23, 3, 110.00, 'notebook'),
          (204, 2, 1, 103, 4, 5.00, 'pen'),
          (205, 2, 2, 38, 1, 300.00, 'printer')
        """,
        f"""
        INSERT INTO {SOURCE_SCHEMA}.CAKESALES VALUES
          (0, 'chocolate', TIMESTAMP '2020-05-18 14:10:30', 'CA', 13.00, 120),
          (1, 'chocolate', TIMESTAMP '2021-03-20 11:30:05', 'WA', 14.00, 140),
          (2, 'vanilla', TIMESTAMP '2021-01-11 06:31:15', 'CA', 12.00, 145),
          (3, 'vanilla', TIMESTAMP '2020-02-08 13:13:23', 'WA', 13.00, 104),
          (4, 'strawberry', TIMESTAMP '2019-05-18 16:09:01', 'CA', 41.00, 162),
          (5, 'strawberry', TIMESTAMP '2019-01-08 06:12:03', 'WA', 43.00, 134)
        """,
    ]
    for statement in statements:
        con.execute(statement)


def build_customer_history_spec() -> dict[str, object]:
    order_totals_sql = f"""
    SELECT
      o."customer_id" AS customer_id,
      o."order_id" AS order_id,
      o."orderdate" AS orderdate,
      SUM(item.price * item.quantity) AS order_value
    FROM {WRAPPER_SCHEMA}.ORDERS o
    JOIN item IN o."products"
    WHERE o."orderdate" >= TIMESTAMP '2020-01-01 00:00:00'
      AND o."orderdate" < TIMESTAMP '2021-01-01 00:00:00'
    GROUP BY o."customer_id", o."order_id", o."orderdate"
    """.strip()
    return {
        "kind": "structured_shape",
        "rootTable": "CUSTOMER_HISTORY",
        "root": {
            "fromSql": f"""
            FROM (
              SELECT
                DENSE_RANK() OVER (ORDER BY customer_id) AS customer_key,
                customer_id,
                MIN(orderdate) AS first_purchase_date,
                SUM(order_value) AS total_value,
                COUNT(*) AS total_orders
              FROM ({order_totals_sql}) order_totals
              GROUP BY customer_id
            ) customer_summary
            """.strip(),
            "idSql": "customer_summary.customer_key",
            "fields": [
                {"name": "customer_id", "sql": "customer_summary.customer_id"},
                {"name": "first_purchase_date", "sql": "customer_summary.first_purchase_date"},
                {"name": "total_value", "sql": "customer_summary.total_value"},
                {"name": "total_orders", "sql": "customer_summary.total_orders"},
                {"name": "orders", "kind": "array_ref", "sql": "customer_summary.total_orders"},
            ],
            "arrays": [
                {
                    "name": "orders",
                    "fromSql": f"""
                    FROM (
                      SELECT
                        DENSE_RANK() OVER (ORDER BY customer_id) AS customer_key,
                        ROW_NUMBER() OVER (ORDER BY customer_id, orderdate, order_id) AS order_row_key,
                        ROW_NUMBER() OVER (
                          PARTITION BY customer_id
                          ORDER BY orderdate, order_id
                        ) - 1 AS order_pos,
                        order_id,
                        orderdate,
                        order_value
                      FROM ({order_totals_sql}) order_totals
                    ) order_rows
                    """.strip(),
                    "rowIdSql": "order_rows.order_row_key",
                    "parentIdSql": "order_rows.customer_key",
                    "positionSql": "order_rows.order_pos",
                    "fields": [
                        {"name": "order_id", "sql": "order_rows.order_id"},
                        {"name": "orderdate", "sql": "order_rows.orderdate"},
                        {"name": "value", "sql": "order_rows.order_value"},
                    ],
                }
            ],
        },
    }


def build_product_lookup_spec() -> dict[str, object]:
    product_order_lines_sql = f"""
    SELECT
      p."name" AS name,
      p."variation" AS variation,
      p."category" AS category,
      p."description" AS description,
      o."order_id" AS order_id,
      o."customer_id" AS customer_id,
      o."orderdate" AS orderdate,
      item.quantity AS quantity,
      item.price * item.quantity AS line_value
    FROM {WRAPPER_SCHEMA}.PRODUCTS p
    JOIN {WRAPPER_SCHEMA}.ORDERS o
      ON o."orderdate" >= TIMESTAMP '2020-01-01 00:00:00'
     AND o."orderdate" < TIMESTAMP '2021-01-01 00:00:00'
    JOIN item IN o."products"
    WHERE p."name" = item.name
      AND p."variation" = item.variation
    """.strip()
    return {
        "kind": "structured_shape",
        "rootTable": "PRODUCT_LOOKUP_RESULT",
        "root": {
            "fromSql": f"""
            FROM (
              SELECT
                DENSE_RANK() OVER (ORDER BY name, variation) AS product_key,
                name,
                variation,
                category,
                description,
                COUNT(*) AS matched_orders,
                SUM(quantity) AS total_units
              FROM ({product_order_lines_sql}) product_order_lines
              GROUP BY name, variation, category, description
            ) product_summary
            """.strip(),
            "idSql": "product_summary.product_key",
            "fields": [
                {"name": "name", "sql": "product_summary.name"},
                {"name": "variation", "sql": "product_summary.variation"},
                {"name": "category", "sql": "product_summary.category"},
                {"name": "description", "sql": "product_summary.description"},
                {"name": "matched_orders", "sql": "product_summary.matched_orders"},
                {"name": "total_units", "sql": "product_summary.total_units"},
                {"name": "orders", "kind": "array_ref", "sql": "product_summary.matched_orders"},
            ],
            "arrays": [
                {
                    "name": "orders",
                    "fromSql": f"""
                    FROM (
                      SELECT
                        DENSE_RANK() OVER (ORDER BY name, variation) AS product_key,
                        ROW_NUMBER() OVER (
                          ORDER BY name, variation, orderdate, order_id, customer_id
                        ) AS order_row_key,
                        ROW_NUMBER() OVER (
                          PARTITION BY name, variation
                          ORDER BY orderdate, order_id, customer_id
                        ) - 1 AS order_pos,
                        order_id,
                        customer_id,
                        orderdate,
                        quantity,
                        line_value
                      FROM ({product_order_lines_sql}) product_order_lines
                    ) order_rows
                    """.strip(),
                    "rowIdSql": "order_rows.order_row_key",
                    "parentIdSql": "order_rows.product_key",
                    "positionSql": "order_rows.order_pos",
                    "fields": [
                        {"name": "order_id", "sql": "order_rows.order_id"},
                        {"name": "customer_id", "sql": "order_rows.customer_id"},
                        {"name": "orderdate", "sql": "order_rows.orderdate"},
                        {"name": "quantity", "sql": "order_rows.quantity"},
                        {"name": "line_value", "sql": "order_rows.line_value"},
                    ],
                }
            ],
        },
    }


def build_artwork_facet_spec() -> dict[str, object]:
    tag_summary_sql = f"""
    SELECT
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, tag) AS row_key,
      ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, tag) - 1 AS row_pos,
      tag,
      COUNT(*) AS tag_count
    FROM (
      SELECT tag
      FROM {WRAPPER_SCHEMA}.ARTWORK a
      JOIN VALUE tag IN a."tags"
    ) exploded_tags
    GROUP BY tag
    """.strip()

    price_docs_sql = f"""
    SELECT
      CASE
        WHEN "price" < 150 THEN '[0,150)'
        WHEN "price" < 200 THEN '[150,200)'
        WHEN "price" < 300 THEN '[200,300)'
        WHEN "price" < 400 THEN '[300,400)'
        ELSE 'Other'
      END AS bucket,
      CASE
        WHEN "price" < 150 THEN 0
        WHEN "price" < 200 THEN 1
        WHEN "price" < 300 THEN 2
        WHEN "price" < 400 THEN 3
        ELSE 4
      END AS bucket_order,
      "title" AS title
    FROM {WRAPPER_SCHEMA}.ARTWORK
    WHERE "price" IS NOT NULL
    """.strip()

    price_summary_sql = f"""
    SELECT
      bucket_order + 1 AS row_key,
      bucket_order AS row_pos,
      bucket,
      COUNT(*) AS bucket_count,
      COUNT(*) AS title_count
    FROM ({price_docs_sql}) price_docs
    GROUP BY bucket_order, bucket
    """.strip()

    price_titles_sql = f"""
    SELECT
      bucket_order + 1 AS bucket_key,
      ROW_NUMBER() OVER (PARTITION BY bucket_order ORDER BY title) - 1 AS title_pos,
      title
    FROM ({price_docs_sql}) price_docs
    """.strip()

    year_summary_sql = f"""
    SELECT
      bucket_order + 1 AS row_key,
      bucket_order AS row_pos,
      bucket,
      COUNT(*) AS bucket_count
    FROM (
      SELECT
        CASE
          WHEN "year" < 1900 THEN '[<1900)'
          WHEN "year" < 1920 THEN '[1900,1920)'
          WHEN "year" < 1930 THEN '[1920,1930)'
          ELSE '[1930,+)'
        END AS bucket,
        CASE
          WHEN "year" < 1900 THEN 0
          WHEN "year" < 1920 THEN 1
          WHEN "year" < 1930 THEN 2
          ELSE 3
        END AS bucket_order
      FROM {WRAPPER_SCHEMA}.ARTWORK
      WHERE "year" IS NOT NULL
    ) year_docs
    GROUP BY bucket_order, bucket
    """.strip()

    return {
        "kind": "structured_shape",
        "rootTable": "ARTWORK_FACETS",
        "root": {
            "fromSql": f"""
            FROM (
              SELECT
                1 AS facet_doc_id,
                (SELECT COUNT(*) FROM ({tag_summary_sql}) tag_summary) AS categorized_by_tags_count,
                (SELECT COUNT(*) FROM ({price_summary_sql}) price_summary) AS categorized_by_price_count,
                (SELECT COUNT(*) FROM ({year_summary_sql}) year_summary) AS categorized_by_year_count
            ) facet_root
            """.strip(),
            "idSql": "facet_root.facet_doc_id",
            "fields": [
                {"name": "categorizedByTags", "kind": "array_ref", "sql": "facet_root.categorized_by_tags_count"},
                {
                    "name": "categorizedByPrice",
                    "kind": "array_ref",
                    "sql": "facet_root.categorized_by_price_count",
                },
                {"name": "categorizedByYear", "kind": "array_ref", "sql": "facet_root.categorized_by_year_count"},
            ],
            "arrays": [
                {
                    "name": "categorizedByTags",
                    "fromSql": f"FROM ({tag_summary_sql}) tag_summary",
                    "rowIdSql": "tag_summary.row_key",
                    "parentIdSql": "1",
                    "positionSql": "tag_summary.row_pos",
                    "fields": [
                        {"name": "tag", "sql": "tag_summary.tag"},
                        {"name": "count", "sql": "tag_summary.tag_count"},
                    ],
                },
                {
                    "name": "categorizedByPrice",
                    "fromSql": f"FROM ({price_summary_sql}) price_summary",
                    "rowIdSql": "price_summary.row_key",
                    "parentIdSql": "1",
                    "positionSql": "price_summary.row_pos",
                    "fields": [
                        {"name": "bucket", "sql": "price_summary.bucket"},
                        {"name": "count", "sql": "price_summary.bucket_count"},
                        {"name": "titles", "kind": "array_ref", "sql": "price_summary.title_count"},
                    ],
                    "arrays": [
                        {
                            "name": "titles",
                            "fromSql": f"FROM ({price_titles_sql}) price_titles",
                            "parentIdSql": "price_titles.bucket_key",
                            "positionSql": "price_titles.title_pos",
                            "valueSql": "price_titles.title",
                        }
                    ],
                },
                {
                    "name": "categorizedByYear",
                    "fromSql": f"FROM ({year_summary_sql}) year_summary",
                    "rowIdSql": "year_summary.row_key",
                    "parentIdSql": "1",
                    "positionSql": "year_summary.row_pos",
                    "fields": [
                        {"name": "bucket", "sql": "year_summary.bucket"},
                        {"name": "count", "sql": "year_summary.bucket_count"},
                    ],
                },
            ],
        },
    }


def build_filtered_sales_spec() -> dict[str, object]:
    expensive_items_sql = f"""
    SELECT
      s."sale_id" AS sale_id,
      item._index AS item_index,
      item.item_id AS item_id,
      item.name AS name,
      item.quantity AS quantity,
      item.price AS price,
      item.price * item.quantity AS line_value
    FROM {WRAPPER_SCHEMA}.SALES s
    JOIN item IN s."items"
    WHERE item.price >= 100
    """.strip()
    return {
        "kind": "structured_shape",
        "rootTable": "FILTERED_SALES",
        "root": {
            "fromSql": f"""
            FROM (
              SELECT
                s."sale_id" AS sale_id,
                COALESCE(expensive_counts.expensive_count, 0) AS expensive_count
              FROM {WRAPPER_SCHEMA}.SALES s
              LEFT JOIN (
                SELECT sale_id, COUNT(*) AS expensive_count
                FROM ({expensive_items_sql}) expensive_items
                GROUP BY sale_id
              ) expensive_counts
                ON expensive_counts.sale_id = s."sale_id"
            ) sales_root
            """.strip(),
            "idSql": "sales_root.sale_id",
            "fields": [
                {"name": "sale_id", "sql": "sales_root.sale_id"},
                {"name": "expensiveItems", "kind": "array_ref", "sql": "sales_root.expensive_count"},
            ],
            "arrays": [
                {
                    "name": "expensiveItems",
                    "fromSql": f"""
                    FROM (
                      SELECT
                        sale_id,
                        (sale_id * 100) + item_index + 1 AS row_key,
                        ROW_NUMBER() OVER (
                          PARTITION BY sale_id
                          ORDER BY item_index
                        ) - 1 AS row_pos,
                        item_id,
                        name,
                        quantity,
                        price,
                        line_value
                      FROM ({expensive_items_sql}) expensive_items
                    ) expensive_rows
                    """.strip(),
                    "rowIdSql": "expensive_rows.row_key",
                    "parentIdSql": "expensive_rows.sale_id",
                    "positionSql": "expensive_rows.row_pos",
                    "fields": [
                        {"name": "item_id", "sql": "expensive_rows.item_id"},
                        {"name": "name", "sql": "expensive_rows.name"},
                        {"name": "quantity", "sql": "expensive_rows.quantity"},
                        {"name": "price", "sql": "expensive_rows.price"},
                        {"name": "line_value", "sql": "expensive_rows.line_value"},
                    ],
                }
            ],
        },
    }


def materialize_and_export(
    con,
    *,
    spec_dict: dict[str, object],
    target_schema: str,
    result_wrapper_schema: str | None = None,
    result_helper_schema: str | None = None,
    result_preprocessor_schema: str | None = None,
    result_preprocessor_script: str | None = None,
    wrapper_query_sql: str | None = None,
) -> dict[str, object]:
    spec = result_family_spec_from_dict(spec_dict)
    materialized = materialize_result_family(
        con,
        target_schema=target_schema,
        spec=spec,
        table_kind="table",
        reset_schema=True,
    )
    exported = normalize_data(export_root_family_to_json(con, materialized_family=materialized))
    result: dict[str, object] = {"exported": exported}
    if wrapper_query_sql is not None:
        if not all(
            value is not None
            for value in (
                result_wrapper_schema,
                result_helper_schema,
                result_preprocessor_schema,
                result_preprocessor_script,
            )
        ):
            raise ValueError("Wrapper schema, helper schema, and preprocessor names are required for wrapper_query_sql.")
        install_wrapper_surface_in_session(
            con,
            materialized_family=materialized,
            wrapper_schema=result_wrapper_schema,
            helper_schema=result_helper_schema,
            preprocessor_schema=result_preprocessor_schema,
            preprocessor_script=result_preprocessor_script,
            activate_preprocessor_session=True,
        )
        result["wrapperQuery"] = run_sql(con, wrapper_query_sql.format(wrapper_schema=result_wrapper_schema))
        activate_source_preprocessor(con)
    return result


def main() -> None:
    con = connect()
    try:
        install_source_fixture(con)
        install_wrapper_views(
            con,
            source_schema=SOURCE_SCHEMA,
            wrapper_schema=WRAPPER_SCHEMA,
            helper_schema=HELPER_SCHEMA,
            generate_preprocessor=True,
            preprocessor_schema=PREPROCESSOR_SCHEMA,
            preprocessor_script=PREPROCESSOR_SCRIPT,
        )
        install_wrapper_preprocessor(
            con,
            [WRAPPER_SCHEMA],
            [HELPER_SCHEMA],
            schema_name=PREPROCESSOR_SCHEMA,
            script_name=PREPROCESSOR_SCRIPT,
        )

        report = {
            "study": "mongodb-migration-round-3",
            "sourceSchema": SOURCE_SCHEMA,
            "wrapperSchema": WRAPPER_SCHEMA,
            "helperSchema": HELPER_SCHEMA,
            "preprocessor": f"{PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}",
            "scenarios": {},
        }

        report["scenarios"]["natural_dot_attempt"] = {
            "description": "Natural Mongo dot traversal across an array property should fail with guidance.",
            "result": run_sql_or_error(
                con,
                f'''
                SELECT CAST("order_id" AS VARCHAR(10))
                FROM {WRAPPER_SCHEMA}.ORDERS
                WHERE "products.name" = 'Asus Laptop'
                ORDER BY "order_id"
                ''',
            ),
        }

        report["scenarios"]["elem_match_translation"] = {
            "description": "Same-element array semantics via correlated EXISTS over rowset expansion.",
            "result": run_sql_or_error(
                con,
                f'''
                SELECT CAST(o."order_id" AS VARCHAR(10))
                FROM {WRAPPER_SCHEMA}.ORDERS o
                WHERE EXISTS (
                  SELECT 1
                  FROM item IN o."products"
                  WHERE item.name = 'Asus Laptop'
                    AND item.variation = 'Ultra HD'
                    AND item.quantity >= 1
                )
                ORDER BY o."order_id"
                ''',
            ),
        }

        report["scenarios"]["unwind_group_analytics"] = {
            "description": "Official-style $unwind + $match + $group order analytics.",
            "result": run_sql_or_error(
                con,
                f'''
                SELECT
                  item.prod_id,
                  item.name,
                  item.variation,
                  SUM(item.quantity) AS units_sold,
                  SUM(item.price * item.quantity) AS total_sales
                FROM {WRAPPER_SCHEMA}.ORDERS o
                JOIN item IN o."products"
                WHERE EXTRACT(YEAR FROM o."orderdate") = 2020
                  AND item.price > 15
                GROUP BY item.prod_id, item.name, item.variation
                ORDER BY total_sales DESC, item.name, item.variation
                ''',
            ),
        }

        report["scenarios"]["customer_history_structured_result"] = {
            "description": "Group-and-total migration using structured results to preserve nested order history output.",
            "result": materialize_and_export(
                con,
                spec_dict=build_customer_history_spec(),
                target_schema="JVS_MONGO_R3_CUSTOMER_HISTORY_SRC",
            ),
        }

        report["scenarios"]["multi_field_lookup_structured_result"] = {
            "description": "Multi-field join migration producing nested orders arrays per product.",
            "result": materialize_and_export(
                con,
                spec_dict=build_product_lookup_spec(),
                target_schema="JVS_MONGO_R3_PRODUCT_LOOKUP_SRC",
                result_wrapper_schema="JSON_VIEW_MONGO_R3_PRODUCT_LOOKUP",
                result_helper_schema="JSON_VIEW_MONGO_R3_PRODUCT_LOOKUP_INTERNAL",
                result_preprocessor_schema="JVS_WRAP_MONGO_R3_PRODUCT_LOOKUP_PP",
                result_preprocessor_script="JSON_WRAPPER_MONGO_R3_PRODUCT_LOOKUP_PREPROCESSOR",
                wrapper_query_sql="""
                SELECT
                  p."name",
                  p."variation",
                  order_row."customer_id",
                  order_row."line_value"
                FROM {wrapper_schema}.PRODUCT_LOOKUP_RESULT p
                JOIN order_row IN p."orders"
                WHERE p."name" = 'Asus Laptop'
                ORDER BY p."variation", order_row._index
                """,
            ),
        }

        report["scenarios"]["facet_structured_result"] = {
            "description": "Facet-style browse output materialized as one nested document with parallel arrays.",
            "result": materialize_and_export(
                con,
                spec_dict=build_artwork_facet_spec(),
                target_schema="JVS_MONGO_R3_FACET_SRC",
            ),
        }

        report["scenarios"]["filter_map_structured_result"] = {
            "description": "Filter-style nested output with transformed expensive item rows preserved as arrays.",
            "result": materialize_and_export(
                con,
                spec_dict=build_filtered_sales_spec(),
                target_schema="JVS_MONGO_R3_FILTERED_SALES_SRC",
            ),
        }

        report["scenarios"]["window_analytics"] = {
            "description": "SetWindowFields-style cumulative and moving-window analytics over flat documents.",
            "result": run_sql_or_error(
                con,
                f'''
                SELECT
                  "state",
                  "type",
                  "orderDate",
                  "quantity",
                  SUM("quantity") OVER (
                    PARTITION BY "state"
                    ORDER BY "orderDate"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) AS cumulative_quantity_for_state,
                  AVG("quantity") OVER (
                    PARTITION BY EXTRACT(YEAR FROM "orderDate")
                    ORDER BY "orderDate"
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                  ) AS moving_average_within_year
                FROM {WRAPPER_SCHEMA}.CAKESALES
                ORDER BY "state", "orderDate"
                ''',
            ),
        }

        print(json.dumps(normalize_data(report), indent=2, sort_keys=True))
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

from __future__ import annotations

import _bootstrap  # noqa: F401

from nano_support import connect, install_wrapper_preprocessor, install_wrapper_views


SOURCE_SCHEMA = "JVS_MONGO_FOCUS_SRC"
WRAPPER_SCHEMA = "JSON_VIEW_MONGO_FOCUS"
HELPER_SCHEMA = "JSON_VIEW_MONGO_FOCUS_INTERNAL"
PREPROCESSOR_SCHEMA = "JVS_WRAP_MONGO_FOCUS_PP"
PREPROCESSOR_SCRIPT = "JSON_WRAPPER_MONGO_FOCUS_PREPROCESSOR"


def run_sql(con, sql: str):
    return con.execute(sql).fetchall()


def run_sql_or_error(con, sql: str) -> tuple[str, list[tuple] | str]:
    try:
        return ("rows", con.execute(sql).fetchall())
    except Exception as exc:  # pragma: no cover - used for study capture
        return ("error", str(exc))


def install_mongo_focus_source(con) -> None:
    statements = [
        f"DROP SCHEMA IF EXISTS {SOURCE_SCHEMA} CASCADE",
        f"CREATE SCHEMA {SOURCE_SCHEMA}",
        f"OPEN SCHEMA {SOURCE_SCHEMA}",
        """
        CREATE OR REPLACE TABLE PRODUCTS (
          "_id" DECIMAL(18,0) NOT NULL,
          "sku" VARCHAR(50),
          "name" VARCHAR(100),
          "variation" VARCHAR(100),
          "category" VARCHAR(100),
          "description" VARCHAR(200),
          "price" DECIMAL(18,2),
          "year" DECIMAL(18,0),
          "tags|array" DECIMAL(18,0)
        )
        """,
        """
        CREATE OR REPLACE TABLE "PRODUCTS_tags_arr" (
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "_value" VARCHAR(100)
        )
        """,
        """
        CREATE OR REPLACE TABLE ORDERS (
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
        """
        CREATE OR REPLACE TABLE "ORDERS_products_arr" (
          "_id" DECIMAL(18,0) NOT NULL,
          "_parent" DECIMAL(18,0) NOT NULL,
          "_pos" DECIMAL(18,0) NOT NULL,
          "sku" VARCHAR(50),
          "name" VARCHAR(100),
          "variation" VARCHAR(100),
          "category" VARCHAR(100),
          "price" DECIMAL(18,2),
          "quantity" DECIMAL(18,0)
        )
        """,
        """
        INSERT INTO PRODUCTS VALUES
          (1, 'abc12345', 'Asus Laptop', 'Ultra HD', 'ELECTRONICS', 'Great for watching movies', 430.00, 2020, 3),
          (2, 'abc12346', 'Asus Laptop', 'Standard Display', 'ELECTRONICS', 'Budget display', 350.00, 2020, 2),
          (3, 'mrf88223', 'Morphy Richards Food Mixer', 'Deluxe', 'KITCHENWARE', 'Luxury mixer', 215.00, 2019, 3),
          (4, 'xyz11228', 'Russell Hobbs Chrome Kettle', 'Standard', 'KITCHENWARE', 'Stylish kettle', 80.00, 2018, 2),
          (5, 'def45678', 'Karcher Hose Set', 'Standard', 'GARDEN', 'Garden hose kit', 22.00, 2017, 2),
          (6, 'book1000', 'The Day Of The Triffids', '1st Edition', 'BOOKS', 'Classic novel', 18.00, 1951, 2)
        """,
        """
        INSERT INTO "PRODUCTS_tags_arr" VALUES
          (1, 0, 'electronics'),
          (1, 1, 'laptop'),
          (1, 2, 'premium'),
          (2, 0, 'electronics'),
          (2, 1, 'laptop'),
          (3, 0, 'kitchen'),
          (3, 1, 'appliance'),
          (3, 2, 'premium'),
          (4, 0, 'kitchen'),
          (4, 1, 'appliance'),
          (5, 0, 'garden'),
          (5, 1, 'outdoor'),
          (6, 0, 'book'),
          (6, 1, 'fiction')
        """,
        """
        INSERT INTO ORDERS VALUES
          (1, 1001, 'alice@example.com', TIMESTAMP '2020-01-03 10:15:00', 'EMEA', 'shipped', 'web', 2),
          (2, 1002, 'bob@example.com', TIMESTAMP '2020-02-10 11:00:00', 'EMEA', 'shipped', 'web', 1),
          (3, 1003, 'alice@example.com', TIMESTAMP '2020-03-21 12:20:00', 'AMER', 'returned', 'mobile', 1),
          (4, 1004, 'cara@example.com', TIMESTAMP '2021-01-05 09:00:00', 'APAC', 'shipped', 'web', 2),
          (5, 1005, 'dan@example.com', TIMESTAMP '2020-06-01 14:45:00', 'EMEA', 'pending', 'partner', 1),
          (6, 1006, 'bob@example.com', TIMESTAMP '2020-11-07 16:30:00', 'EMEA', 'shipped', 'web', 2)
        """,
        """
        INSERT INTO "ORDERS_products_arr" VALUES
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
    ]
    for statement in statements:
        con.execute(statement)


def main() -> None:
    con = connect()
    try:
        install_mongo_focus_source(con)
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

        scenarios: list[tuple[str, str, str]] = [
            (
                "mongo-dot-array-natural-attempt",
                "Natural Mongo-style dot traversal across arrays should teach the rowset rule.",
                f'''
                SELECT CAST("order_id" AS VARCHAR(10))
                FROM {WRAPPER_SCHEMA}.ORDERS
                WHERE "products.name" = 'Asus Laptop'
                ORDER BY "order_id"
                ''',
            ),
            (
                "mongo-dot-array-ported",
                "Port the any-element Mongo dot match into a correlated EXISTS over rowset expansion.",
                f'''
                SELECT CAST(o."order_id" AS VARCHAR(10))
                FROM {WRAPPER_SCHEMA}.ORDERS o
                WHERE EXISTS (
                  SELECT 1
                  FROM item IN o."products"
                  WHERE item.name = 'Asus Laptop'
                )
                ORDER BY o."order_id"
                ''',
            ),
            (
                "elemMatch-ported",
                "Port a same-element $elemMatch predicate into correlated rowset filtering.",
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
            (
                "size-query",
                "Port a Mongo $size filter into bracket SIZE syntax.",
                f'''
                SELECT CAST("order_id" AS VARCHAR(10))
                FROM {WRAPPER_SCHEMA}.ORDERS
                WHERE "products[SIZE]" = 2
                ORDER BY "order_id"
                ''',
            ),
            (
                "unwind-group",
                "Port $unwind + $match + $group for line-item sales analytics.",
                f'''
                SELECT
                  item.name,
                  item.variation,
                  SUM(item.quantity) AS units_sold,
                  SUM(item.price * item.quantity) AS total_sales
                FROM {WRAPPER_SCHEMA}.ORDERS o
                JOIN item IN o."products"
                WHERE item.price > 15
                GROUP BY item.name, item.variation
                ORDER BY total_sales DESC, item.name, item.variation
                ''',
            ),
            (
                "group-total-history",
                "Port the Group and Total Data pattern into grouped SQL with history rendering.",
                f'''
                WITH line_items AS (
                  SELECT
                    o."customer_id" AS customer_id,
                    o."order_id" AS order_id,
                    o."orderdate" AS orderdate,
                    item.name AS product_name,
                    item.quantity AS quantity,
                    item.price AS price,
                    item.price * item.quantity AS line_value
                  FROM {WRAPPER_SCHEMA}.ORDERS o
                  JOIN item IN o."products"
                  WHERE EXTRACT(YEAR FROM o."orderdate") = 2020
                )
                SELECT
                  customer_id,
                  COUNT(DISTINCT order_id) AS orders_2020,
                  SUM(line_value) AS total_value_2020,
                  LISTAGG(CAST(order_id AS VARCHAR(20)) || ':' || product_name, ', ')
                    WITHIN GROUP (ORDER BY orderdate, order_id, product_name) AS order_history
                FROM line_items
                GROUP BY customer_id
                ORDER BY total_value_2020 DESC, customer_id
                ''',
            ),
            (
                "lookup-multifield-join",
                "Port the Mongo multi-field $lookup example into ordinary SQL joins plus rowset expansion.",
                f'''
                SELECT
                  p."name",
                  p."variation",
                  p."category",
                  COUNT(*) AS matched_order_lines,
                  SUM(item.quantity) AS total_units
                FROM {WRAPPER_SCHEMA}.PRODUCTS p
                JOIN {WRAPPER_SCHEMA}.ORDERS o
                  ON EXTRACT(YEAR FROM o."orderdate") = 2020
                JOIN item IN o."products"
                WHERE p."name" = item.name
                  AND p."variation" = item.variation
                GROUP BY p."name", p."variation", p."category"
                ORDER BY matched_order_lines DESC, p."name", p."variation"
                ''',
            ),
            (
                "facet-like-dashboard",
                "Approximate a Mongo $facet dashboard using CTEs plus UNION ALL.",
                f'''
                WITH tag_counts AS (
                  SELECT
                    'tag' AS facet,
                    tag AS bucket,
                    COUNT(*) AS metric
                  FROM {WRAPPER_SCHEMA}.PRODUCTS p
                  JOIN VALUE tag IN p."tags"
                  GROUP BY tag
                ),
                price_buckets AS (
                  SELECT
                    'price' AS facet,
                    CASE
                      WHEN "price" < 50 THEN '[0,50)'
                      WHEN "price" < 200 THEN '[50,200)'
                      ELSE '[200,+)'
                    END AS bucket,
                    COUNT(*) AS metric
                  FROM {WRAPPER_SCHEMA}.PRODUCTS
                  GROUP BY 2
                ),
                year_buckets AS (
                  SELECT
                    'year' AS facet,
                    CASE
                      WHEN "year" < 2019 THEN '[<2019)'
                      WHEN "year" < 2021 THEN '[2019,2021)'
                      ELSE '[2021,+)'
                    END AS bucket,
                    COUNT(*) AS metric
                  FROM {WRAPPER_SCHEMA}.PRODUCTS
                  GROUP BY 2
                )
                SELECT facet, bucket, metric
                FROM tag_counts
                UNION ALL
                SELECT facet, bucket, metric FROM price_buckets
                UNION ALL
                SELECT facet, bucket, metric FROM year_buckets
                ORDER BY facet, metric DESC, bucket
                ''',
            ),
            (
                "filter-like-normalized",
                "Port a $filter-style expensive-items projection into normalized SQL rows.",
                f'''
                SELECT
                  CAST(o."order_id" AS VARCHAR(10)) AS order_id,
                  item.name,
                  item.price,
                  item.quantity
                FROM {WRAPPER_SCHEMA}.ORDERS o
                JOIN item IN o."products"
                WHERE item.price >= 100
                ORDER BY o."order_id", item.name
                ''',
            ),
            (
                "windowed-rollup",
                "Port a $setWindowFields-style cumulative customer spend analysis into SQL windows.",
                f'''
                WITH line_items AS (
                  SELECT
                    o."customer_id" AS customer_id,
                    o."order_id" AS order_id,
                    o."orderdate" AS orderdate,
                    item.name AS product_name,
                    item.price * item.quantity AS line_value
                  FROM {WRAPPER_SCHEMA}.ORDERS o
                  JOIN item IN o."products"
                )
                SELECT
                  customer_id,
                  CAST(order_id AS VARCHAR(10)) AS order_id,
                  product_name,
                  line_value,
                  SUM(line_value) OVER (
                    PARTITION BY customer_id
                    ORDER BY orderdate, order_id, product_name
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                  ) AS cumulative_customer_value
                FROM line_items
                ORDER BY customer_id, order_id, product_name
                ''',
            ),
        ]

        print("-- mongodb migration focus study --")
        print(f"source schema: {SOURCE_SCHEMA}")
        print(f"wrapper schema: {WRAPPER_SCHEMA}")
        print(f"helper schema: {HELPER_SCHEMA}")
        print(f"preprocessor: {PREPROCESSOR_SCHEMA}.{PREPROCESSOR_SCRIPT}")
        for scenario_id, summary, sql in scenarios:
            status, payload = run_sql_or_error(con, sql)
            print(f"\n## {scenario_id}")
            print(summary)
            if status == "rows":
                print(payload)
            else:
                print(payload)
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()


if __name__ == "__main__":
    main()

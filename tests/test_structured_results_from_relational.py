#!/usr/bin/env python3

import json
import subprocess

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect


UPSTREAM_SCHEMA = "JVS_RELATIONAL_UPSTREAM"
RESULT_SOURCE_SCHEMA = "JVS_RELATIONAL_RESULT_SRC"
RESULT_WRAPPER_SCHEMA = "JSON_VIEW_RELATIONAL_RESULT"
RESULT_HELPER_SCHEMA = "JSON_VIEW_RELATIONAL_RESULT_INTERNAL"
RESULT_PP_SCHEMA = "JVS_RELATIONAL_RESULT_PP"
RESULT_PP_SCRIPT = "JSON_RELATIONAL_RESULT_PREPROCESSOR"

PACKAGE_DIR = ROOT / "dist" / "relational_result_package_test"
PACKAGE_NAME = "json_relational_result"
PACKAGE_CONFIG_PATH = PACKAGE_DIR / f"{PACKAGE_NAME}_package.json"
RESULT_FAMILY_CONFIG_PATH = PACKAGE_DIR / "relational_result_family_input.json"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def project_top_level(rows: list[dict[str, object]], keys: list[str]) -> list[dict[str, object]]:
    projected: list[dict[str, object]] = []
    for row in rows:
        projected_row: dict[str, object] = {}
        for key in keys:
            if key in row:
                projected_row[key] = row[key]
        projected.append(projected_row)
    return projected


def install_relational_fixture(con) -> None:
    statements = [
        f"DROP SCHEMA IF EXISTS {UPSTREAM_SCHEMA} CASCADE",
        f"CREATE SCHEMA {UPSTREAM_SCHEMA}",
        f"""
        CREATE TABLE {UPSTREAM_SCHEMA}.CUSTOMERS (
          CUSTOMER_ID DECIMAL(18,0) NOT NULL,
          NAME VARCHAR(100),
          TIER VARCHAR(40)
        )
        """,
        f"""
        CREATE TABLE {UPSTREAM_SCHEMA}.ORDERS (
          ORDER_ID DECIMAL(18,0) NOT NULL,
          CUSTOMER_ID DECIMAL(18,0) NOT NULL,
          STATUS VARCHAR(40)
        )
        """,
        f"""
        CREATE TABLE {UPSTREAM_SCHEMA}.ORDER_ITEMS (
          ORDER_ID DECIMAL(18,0) NOT NULL,
          LINE_NO DECIMAL(18,0) NOT NULL,
          SKU VARCHAR(40),
          QTY DECIMAL(18,0)
        )
        """,
        f"""
        CREATE TABLE {UPSTREAM_SCHEMA}.PRODUCTS (
          SKU VARCHAR(40) NOT NULL,
          TITLE VARCHAR(100),
          CATEGORY VARCHAR(80)
        )
        """,
        f"""
        CREATE TABLE {UPSTREAM_SCHEMA}.ORDER_TAGS (
          ORDER_ID DECIMAL(18,0) NOT NULL,
          POS DECIMAL(18,0) NOT NULL,
          TAG VARCHAR(80)
        )
        """,
        f"""
        INSERT INTO {UPSTREAM_SCHEMA}.CUSTOMERS VALUES
          (1, 'Alice', 'gold'),
          (2, 'Bob', 'silver')
        """,
        f"""
        INSERT INTO {UPSTREAM_SCHEMA}.ORDERS VALUES
          (100, 1, 'paid'),
          (101, 2, 'pending'),
          (102, 1, 'cancelled')
        """,
        f"""
        INSERT INTO {UPSTREAM_SCHEMA}.ORDER_ITEMS VALUES
          (100, 1, 'sku-1', 2),
          (100, 2, 'sku-2', 1),
          (101, 1, 'sku-3', 4)
        """,
        f"""
        INSERT INTO {UPSTREAM_SCHEMA}.PRODUCTS VALUES
          ('sku-1', 'Widget', 'hardware'),
          ('sku-2', 'Gizmo', 'accessory'),
          ('sku-3', 'Cable', 'accessory')
        """,
        f"""
        INSERT INTO {UPSTREAM_SCHEMA}.ORDER_TAGS VALUES
          (100, 0, 'priority'),
          (100, 1, 'gift'),
          (101, 0, 'review')
        """,
    ]
    for statement in statements:
        con.execute(statement)


def main() -> None:
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_FAMILY_CONFIG_PATH.write_text(
        json.dumps(
            {
                "kind": "synthesized_family",
                "rootTable": "ORDER_REPORT",
                "tableSpecs": [
                    {
                        "tableName": "ORDER_REPORT",
                        "selectSql": f"""
                        SELECT
                          o.ORDER_ID AS "_id",
                          o.ORDER_ID AS "order_id",
                          o.STATUS AS "status",
                          CAST(100000 + o.CUSTOMER_ID AS DECIMAL(18,0)) AS "customer|object",
                          COALESCE(item_counts.ITEM_COUNT, 0) AS "items|array",
                          COALESCE(tag_counts.TAG_COUNT, 0) AS "tags|array"
                        FROM {UPSTREAM_SCHEMA}.ORDERS o
                        LEFT JOIN (
                          SELECT ORDER_ID, COUNT(*) AS ITEM_COUNT
                          FROM {UPSTREAM_SCHEMA}.ORDER_ITEMS
                          GROUP BY ORDER_ID
                        ) item_counts ON item_counts.ORDER_ID = o.ORDER_ID
                        LEFT JOIN (
                          SELECT ORDER_ID, COUNT(*) AS TAG_COUNT
                          FROM {UPSTREAM_SCHEMA}.ORDER_TAGS
                          GROUP BY ORDER_ID
                        ) tag_counts ON tag_counts.ORDER_ID = o.ORDER_ID
                        """.strip(),
                    },
                    {
                        "tableName": "ORDER_REPORT_customer",
                        "selectSql": f"""
                        SELECT
                          CAST(100000 + c.CUSTOMER_ID AS DECIMAL(18,0)) AS "_id",
                          c.NAME AS "name",
                          c.TIER AS "tier"
                        FROM {UPSTREAM_SCHEMA}.CUSTOMERS c
                        """.strip(),
                    },
                    {
                        "tableName": "ORDER_REPORT_items_arr",
                        "selectSql": f"""
                        SELECT
                          CAST((i.ORDER_ID * 100) + i.LINE_NO AS DECIMAL(18,0)) AS "_id",
                          i.ORDER_ID AS "_parent",
                          i.LINE_NO - 1 AS "_pos",
                          i.SKU AS "sku",
                          i.QTY AS "qty",
                          CAST((i.ORDER_ID * 100) + i.LINE_NO AS DECIMAL(18,0)) AS "product|object"
                        FROM {UPSTREAM_SCHEMA}.ORDER_ITEMS i
                        """.strip(),
                    },
                    {
                        "tableName": "ORDER_REPORT_items_arr_product",
                        "selectSql": f"""
                        SELECT
                          CAST((i.ORDER_ID * 100) + i.LINE_NO AS DECIMAL(18,0)) AS "_id",
                          p.TITLE AS "title",
                          p.CATEGORY AS "category"
                        FROM {UPSTREAM_SCHEMA}.ORDER_ITEMS i
                        JOIN {UPSTREAM_SCHEMA}.PRODUCTS p ON p.SKU = i.SKU
                        """.strip(),
                    },
                    {
                        "tableName": "ORDER_REPORT_tags_arr",
                        "selectSql": f"""
                        SELECT
                          t.ORDER_ID AS "_parent",
                          t.POS AS "_pos",
                          t.TAG AS "_value"
                        FROM {UPSTREAM_SCHEMA}.ORDER_TAGS t
                        """.strip(),
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    con = connect()
    try:
        install_relational_fixture(con)
    finally:
        con.close()

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "generate-result-family-package",
            "--source-schema",
            RESULT_SOURCE_SCHEMA,
            "--wrapper-schema",
            RESULT_WRAPPER_SCHEMA,
            "--helper-schema",
            RESULT_HELPER_SCHEMA,
            "--preprocessor-schema",
            RESULT_PP_SCHEMA,
            "--preprocessor-script",
            RESULT_PP_SCRIPT,
            "--output-dir",
            str(PACKAGE_DIR),
            "--package-name",
            PACKAGE_NAME,
            "--result-family-config",
            str(RESULT_FAMILY_CONFIG_PATH),
        ],
        check=True,
    )

    install_result = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    if f"Installed durable result family into source schema {RESULT_SOURCE_SCHEMA}" not in install_result.stdout:
        raise AssertionError("install output should confirm durable relational-source result-family materialization")

    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {RESULT_PP_SCHEMA}.{RESULT_PP_SCRIPT}")
        wrapper_rows = con.execute(
            f"""
            SELECT
              CAST("order_id" AS VARCHAR(10)),
              "customer.name",
              COALESCE("items[FIRST].product.title", 'NULL'),
              COALESCE("items[LAST].sku", 'NULL'),
              COALESCE("tags[LAST]", 'NULL'),
              CAST("tags[SIZE]" AS VARCHAR(10))
            FROM {RESULT_WRAPPER_SCHEMA}.ORDER_REPORT
            ORDER BY "order_id"
            """
        ).fetchall()
        assert_equal(
            wrapper_rows,
            [
                ("100", "Alice", "Widget", "sku-2", "gift", "2"),
                ("101", "Bob", "Cable", "sku-3", "review", "1"),
                ("102", "Alice", "NULL", "NULL", "NULL", "0"),
            ],
            "relational-source wrapper rows",
        )

        rowset_rows = con.execute(
            f"""
            SELECT
              CAST(r."order_id" AS VARCHAR(10)),
              CAST(item._index AS VARCHAR(10)),
              item.sku,
              item."product.category"
            FROM {RESULT_WRAPPER_SCHEMA}.ORDER_REPORT r
            JOIN item IN r."items"
            ORDER BY r."order_id", item._index
            """
        ).fetchall()
        assert_equal(
            rowset_rows,
            [
                ("100", "0", "sku-1", "hardware"),
                ("100", "1", "sku-2", "accessory"),
                ("101", "0", "sku-3", "accessory"),
            ],
            "relational-source rowset rows",
        )

        expected_exported = [
            {
                "customer": {"name": "Alice", "tier": "gold"},
                "items": [
                    {"product": {"category": "hardware", "title": "Widget"}, "qty": 2, "sku": "sku-1"},
                    {"product": {"category": "accessory", "title": "Gizmo"}, "qty": 1, "sku": "sku-2"},
                ],
                "order_id": 100,
                "status": "paid",
                "tags": ["priority", "gift"],
            },
            {
                "customer": {"name": "Bob", "tier": "silver"},
                "items": [
                    {"product": {"category": "accessory", "title": "Cable"}, "qty": 4, "sku": "sku-3"},
                ],
                "order_id": 101,
                "status": "pending",
                "tags": ["review"],
            },
            {
                "customer": {"name": "Alice", "tier": "gold"},
                "items": [],
                "order_id": 102,
                "status": "cancelled",
                "tags": [],
            },
        ]
        to_json_rows = [
            (json.loads(row[0]), json.loads(row[1]), json.loads(row[2]), json.loads(row[3]))
            for row in con.execute(
                f"""
                SELECT
                  TO_JSON(*) AS full_json,
                  TO_JSON("customer", "items") AS nested_subset_json,
                  TO_JSON("order_id", "status") AS scalar_subset_json,
                  TO_JSON("customer") AS customer_json
                FROM {RESULT_WRAPPER_SCHEMA}.ORDER_REPORT
                ORDER BY "order_id"
                """
            ).fetchall()
        ]
        assert_equal(
            [row[0] for row in to_json_rows],
            expected_exported,
            "relational-source TO_JSON(*) export",
        )
        assert_equal(
            [row[1] for row in to_json_rows],
            project_top_level(expected_exported, ["customer", "items"]),
            "relational-source TO_JSON nested subset",
        )
        assert_equal(
            [row[2] for row in to_json_rows],
            project_top_level(expected_exported, ["order_id", "status"]),
            "relational-source TO_JSON scalar subset",
        )
        assert_equal(
            [row[3] for row in to_json_rows],
            project_top_level(expected_exported, ["customer"]),
            "relational-source TO_JSON customer subset",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    print("-- structured results from relational tables regression --")
    print("generated, installed, queried, and exported relational-source structured results:", PACKAGE_CONFIG_PATH)


if __name__ == "__main__":
    main()

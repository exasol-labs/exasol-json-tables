#!/usr/bin/env python3

import json
import subprocess

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect, install_source_fixture, install_wrapper_views


UPSTREAM_SCHEMA = "JVS_ERGO_UPSTREAM"
PACKAGE_DIR = ROOT / "dist" / "structured_result_ergonomics_test"
SHAPE_CONFIG_PATH = PACKAGE_DIR / "order_report_shape.json"
PACKAGE_NAME = "json_order_report_shape"
PACKAGE_CONFIG_PATH = PACKAGE_DIR / f"{PACKAGE_NAME}_package.json"

RESULT_SOURCE_SCHEMA = "JVS_ERGO_RESULT_SRC"
RESULT_WRAPPER_SCHEMA = "JSON_VIEW_ERGO_RESULT"
RESULT_HELPER_SCHEMA = "JSON_VIEW_ERGO_RESULT_INTERNAL"
RESULT_PP_SCHEMA = "JVS_ERGO_RESULT_PP"
RESULT_PP_SCRIPT = "JSON_ERGO_RESULT_PREPROCESSOR"

BASE_SOURCE_SCHEMA = "JVS_SRC"
BASE_WRAPPER_SCHEMA = "JSON_VIEW"
BASE_HELPER_SCHEMA = "JSON_VIEW_INTERNAL"

WRAPPER_PACKAGE_NAME = "json_wrapper_shape"
WRAPPER_SHAPE_CONFIG_PATH = PACKAGE_DIR / "wrapper_shape.json"
WRAPPER_PACKAGE_CONFIG_PATH = PACKAGE_DIR / f"{WRAPPER_PACKAGE_NAME}_package.json"
WRAPPER_RESULT_SOURCE_SCHEMA = "JVS_ERGO_WRAPPER_RESULT_SRC"
WRAPPER_RESULT_WRAPPER_SCHEMA = "JSON_VIEW_ERGO_WRAPPER_RESULT"
WRAPPER_RESULT_HELPER_SCHEMA = "JSON_VIEW_ERGO_WRAPPER_RESULT_INTERNAL"
WRAPPER_RESULT_PP_SCHEMA = "JVS_ERGO_WRAPPER_RESULT_PP"
WRAPPER_RESULT_PP_SCRIPT = "JSON_ERGO_WRAPPER_RESULT_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


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


def write_shape_config() -> None:
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    SHAPE_CONFIG_PATH.write_text(
        json.dumps(
            {
                "kind": "structured_shape",
                "rootTable": "ORDER_REPORT",
                "root": {
                    "fromSql": f"""
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
                    "idSql": "o.ORDER_ID",
                    "fields": [
                        {"name": "order_id", "sql": "o.ORDER_ID"},
                        {"name": "status", "sql": "o.STATUS"},
                        {"name": "customer", "kind": "object_ref", "sql": "CAST(100000 + o.CUSTOMER_ID AS DECIMAL(18,0))"},
                        {"name": "items", "kind": "array_ref", "sql": "COALESCE(item_counts.ITEM_COUNT, 0)"},
                        {"name": "tags", "kind": "array_ref", "sql": "COALESCE(tag_counts.TAG_COUNT, 0)"},
                    ],
                    "objects": [
                        {
                            "name": "customer",
                            "fromSql": f"FROM {UPSTREAM_SCHEMA}.CUSTOMERS c",
                            "idSql": "CAST(100000 + c.CUSTOMER_ID AS DECIMAL(18,0))",
                            "fields": [
                                {"name": "name", "sql": "c.NAME"},
                                {"name": "tier", "sql": "c.TIER"},
                            ],
                        }
                    ],
                    "arrays": [
                        {
                            "name": "items",
                            "fromSql": f"FROM {UPSTREAM_SCHEMA}.ORDER_ITEMS i",
                            "rowIdSql": "CAST((i.ORDER_ID * 100) + i.LINE_NO AS DECIMAL(18,0))",
                            "parentIdSql": "i.ORDER_ID",
                            "positionSql": "i.LINE_NO - 1",
                            "fields": [
                                {"name": "sku", "sql": "i.SKU"},
                                {"name": "qty", "sql": "i.QTY"},
                                {
                                    "name": "product",
                                    "kind": "object_ref",
                                    "sql": "CAST((i.ORDER_ID * 100) + i.LINE_NO AS DECIMAL(18,0))",
                                },
                            ],
                            "objects": [
                                {
                                    "name": "product",
                                    "fromSql": f"""
                                    FROM {UPSTREAM_SCHEMA}.ORDER_ITEMS i
                                    JOIN {UPSTREAM_SCHEMA}.PRODUCTS p ON p.SKU = i.SKU
                                    """.strip(),
                                    "idSql": "CAST((i.ORDER_ID * 100) + i.LINE_NO AS DECIMAL(18,0))",
                                    "fields": [
                                        {"name": "title", "sql": "p.TITLE"},
                                        {"name": "category", "sql": "p.CATEGORY"},
                                    ],
                                }
                            ],
                        },
                        {
                            "name": "tags",
                            "fromSql": f"FROM {UPSTREAM_SCHEMA}.ORDER_TAGS t",
                            "parentIdSql": "t.ORDER_ID",
                            "positionSql": "t.POS",
                            "valueSql": "t.TAG",
                        },
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def write_wrapper_shape_config() -> None:
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    WRAPPER_SHAPE_CONFIG_PATH.write_text(
        json.dumps(
            {
                "kind": "structured_shape",
                "rootTable": "SAMPLE_REPORT",
                "root": {
                    "fromSql": f"FROM {BASE_WRAPPER_SCHEMA}.SAMPLE s",
                    "idSql": 's."id"',
                    "fields": [
                        {"name": "sample_id", "sql": 's."id"'},
                        {"name": "name", "sql": 'JSON_AS_VARCHAR(s."name")'},
                        {
                            "name": "note_state",
                            "sql": """CASE
                                WHEN JSON_IS_EXPLICIT_NULL(s."note") THEN 'explicit-null'
                                WHEN s."note" IS NULL THEN 'missing'
                                ELSE 'value'
                              END""",
                        },
                        {"name": "deep_note", "sql": """COALESCE(s."meta.info.note", 'NULL')"""},
                        {"name": "tags", "kind": "array_ref", "sql": 'COALESCE(s."tags[SIZE]", 0)'},
                    ],
                    "arrays": [
                        {
                            "name": "tags",
                            "fromSql": f'FROM {BASE_WRAPPER_SCHEMA}.SAMPLE s JOIN VALUE tag IN s."tags"',
                            "parentIdSql": 's."id"',
                            "positionSql": "tag._index",
                            "valueSql": "tag",
                        }
                    ],
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main() -> None:
    write_shape_config()
    write_wrapper_shape_config()

    con = connect()
    try:
        install_relational_fixture(con)
        install_source_fixture(con, include_deep_fixture=False)
        install_wrapper_views(
            con,
            source_schema=BASE_SOURCE_SCHEMA,
            wrapper_schema=BASE_WRAPPER_SCHEMA,
            helper_schema=BASE_HELPER_SCHEMA,
            generate_preprocessor=True,
        )
    finally:
        con.close()

    preview = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "structured_result_tool.py"),
            "preview-json",
            "--result-family-config",
            str(SHAPE_CONFIG_PATH),
            "--target-schema",
            "JVS_ERGO_PREVIEW",
            "--table-kind",
            "local_temporary",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    preview_rows = json.loads(preview.stdout)
    assert_equal(
        preview_rows,
        [
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
        ],
        "preview-json output",
    )

    wrapper_preview = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "structured_result_tool.py"),
            "preview-json",
            "--result-family-config",
            str(WRAPPER_SHAPE_CONFIG_PATH),
            "--target-schema",
            "JVS_ERGO_WRAPPER_PREVIEW",
            "--table-kind",
            "local_temporary",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    wrapper_preview_rows = json.loads(wrapper_preview.stdout)
    assert_equal(
        wrapper_preview_rows,
        [
            {
                "deep_note": "deep",
                "name": "alpha",
                "note_state": "value",
                "sample_id": 1,
                "tags": ["red", "blue"],
            },
            {
                "deep_note": "NULL",
                "name": "beta",
                "note_state": "explicit-null",
                "sample_id": 2,
                "tags": ["green"],
            },
            {
                "deep_note": "NULL",
                "name": "gamma",
                "note_state": "missing",
                "sample_id": 3,
                "tags": [],
            },
        ],
        "wrapper-based preview-json output",
    )

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
            str(SHAPE_CONFIG_PATH),
        ],
        check=True,
    )

    package_config = json.loads(PACKAGE_CONFIG_PATH.read_text())
    assert_equal(
        package_config["resultFamily"]["kind"],
        "structured_shape",
        "result-family package config kind",
    )

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
        ],
        check=True,
    )

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
              COALESCE("tags[LAST]", 'NULL')
            FROM {RESULT_WRAPPER_SCHEMA}.ORDER_REPORT
            ORDER BY "order_id"
            """
        ).fetchall()
        assert_equal(
            wrapper_rows,
            [
                ("100", "Alice", "Widget", "sku-2", "gift"),
                ("101", "Bob", "Cable", "sku-3", "review"),
                ("102", "Alice", "NULL", "NULL", "NULL"),
            ],
            "structured-shape wrapper rows",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "generate-result-family-package",
            "--source-schema",
            WRAPPER_RESULT_SOURCE_SCHEMA,
            "--wrapper-schema",
            WRAPPER_RESULT_WRAPPER_SCHEMA,
            "--helper-schema",
            WRAPPER_RESULT_HELPER_SCHEMA,
            "--preprocessor-schema",
            WRAPPER_RESULT_PP_SCHEMA,
            "--preprocessor-script",
            WRAPPER_RESULT_PP_SCRIPT,
            "--output-dir",
            str(PACKAGE_DIR),
            "--package-name",
            WRAPPER_PACKAGE_NAME,
            "--result-family-config",
            str(WRAPPER_SHAPE_CONFIG_PATH),
        ],
        check=True,
    )

    subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "install",
            "--package-config",
            str(WRAPPER_PACKAGE_CONFIG_PATH),
        ],
        check=True,
    )

    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {WRAPPER_RESULT_PP_SCHEMA}.{WRAPPER_RESULT_PP_SCRIPT}")
        wrapper_shape_rows = con.execute(
            f"""
            SELECT
              CAST("sample_id" AS VARCHAR(10)),
              "name",
              "note_state",
              COALESCE("deep_note", 'NULL'),
              COALESCE("tags[LAST]", 'NULL')
            FROM {WRAPPER_RESULT_WRAPPER_SCHEMA}.SAMPLE_REPORT
            ORDER BY "sample_id"
            """
        ).fetchall()
        assert_equal(
            wrapper_shape_rows,
            [
                ("1", "alpha", "value", "deep", "blue"),
                ("2", "beta", "explicit-null", "NULL", "green"),
                ("3", "gamma", "missing", "NULL", "NULL"),
            ],
            "wrapper-based structured-shape wrapper rows",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    print("-- structured result ergonomics regression --")
    print("previewed and packaged structured-shape config:", PACKAGE_CONFIG_PATH)


if __name__ == "__main__":
    main()

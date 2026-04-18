#!/usr/bin/env python3

import json
import subprocess

import _bootstrap  # noqa: F401

from nano_support import ROOT, connect, install_source_fixture, install_wrapper_views


PACKAGE_DIR = ROOT / "dist" / "result_family_package_tool_test"
PACKAGE_NAME = "json_result_family_pkg"
PACKAGE_CONFIG_PATH = PACKAGE_DIR / f"{PACKAGE_NAME}_package.json"
RESULT_FAMILY_CONFIG_PATH = PACKAGE_DIR / "subset_result_family_input.json"

BASE_SOURCE_SCHEMA = "JVS_SRC"
BASE_WRAPPER_SCHEMA = "JSON_VIEW"
BASE_HELPER_SCHEMA = "JSON_VIEW_INTERNAL"

RESULT_SOURCE_SCHEMA = "JVS_RESULT_PKG_SRC"
RESULT_WRAPPER_SCHEMA = "JSON_VIEW_RESULT_PKG"
RESULT_HELPER_SCHEMA = "JSON_VIEW_RESULT_PKG_INTERNAL"
RESULT_PP_SCHEMA = "JVS_RESULT_PKG_PP"
RESULT_PP_SCRIPT = "JSON_RESULT_PKG_PREPROCESSOR"


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def main() -> None:
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_FAMILY_CONFIG_PATH.write_text(
        json.dumps(
            {
                "kind": "family_preserving_subset",
                "sourceHelperSchema": BASE_HELPER_SCHEMA,
                "rootTable": "SAMPLE",
                "rootFilterSql": '"id" IN (1, 2)',
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    con = connect()
    try:
        install_source_fixture(con, include_deep_fixture=False)
        install_wrapper_views(
            con,
            source_schema=BASE_SOURCE_SCHEMA,
            wrapper_schema=BASE_WRAPPER_SCHEMA,
            helper_schema=BASE_HELPER_SCHEMA,
            generate_preprocessor=False,
        )
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

    package_config = json.loads(PACKAGE_CONFIG_PATH.read_text())
    assert_equal(package_config["sourceSchema"], RESULT_SOURCE_SCHEMA, "result package source schema")
    assert_equal(
        package_config["resultFamily"]["kind"],
        "family_preserving_subset",
        "result package materialization kind",
    )
    result_manifest_path = PACKAGE_DIR / package_config["resultFamily"]["materializedFamilyManifest"]
    result_manifest = json.loads(result_manifest_path.read_text())
    assert_equal(result_manifest["rootTable"], "SAMPLE", "result family manifest root table")
    assert_equal(result_manifest["tableKind"], "table", "result family manifest table kind")
    assert_equal(
        result_manifest["familyDescription"]["rootTables"],
        ["SAMPLE"],
        "result family manifest roots",
    )

    con = connect()
    try:
        con.execute(f"DROP SCHEMA IF EXISTS {RESULT_SOURCE_SCHEMA} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {RESULT_WRAPPER_SCHEMA} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {RESULT_HELPER_SCHEMA} CASCADE")
        con.execute(f"DROP SCHEMA IF EXISTS {RESULT_PP_SCHEMA} CASCADE")
    finally:
        con.close()

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
        raise AssertionError("install output should confirm durable result-family materialization")

    validate_result = subprocess.run(
        [
            "python3",
            str(ROOT / "tools" / "wrapper_package_tool.py"),
            "validate",
            "--package-config",
            str(PACKAGE_CONFIG_PATH),
            "--check-installed",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    if "Validated installed package" not in validate_result.stdout:
        raise AssertionError("validate output should confirm the installed durable result-family package")

    con = connect()
    try:
        con.execute(f"ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = {RESULT_PP_SCHEMA}.{RESULT_PP_SCRIPT}")
        rows = con.execute(
            f"""
            SELECT
              CAST("id" AS VARCHAR(10)),
              COALESCE("child.value", 'NULL'),
              COALESCE("tags[LAST]", 'NULL'),
              COALESCE("items[LAST].value", 'NULL')
            FROM {RESULT_WRAPPER_SCHEMA}.SAMPLE
            ORDER BY "id"
            """
        ).fetchall()
        assert_equal(
            rows,
            [("1", "child-1", "blue", "second"), ("2", "NULL", "green", "only")],
            "installed durable result-family package query",
        )
    finally:
        try:
            con.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = NULL")
        except Exception:
            pass
        con.close()

    print("-- result-family package tool regression --")
    print("generated, installed, and validated durable result-family package:", PACKAGE_CONFIG_PATH)


if __name__ == "__main__":
    main()

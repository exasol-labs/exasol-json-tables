#!/usr/bin/env python3

import json
import subprocess

from nano_support import ROOT, connect


DIST = ROOT / "dist" / "udf_pushdown_mre_adapter.lua"


def bundle_adapter() -> str:
    subprocess.run(["python3", str(ROOT / "tools" / "bundle_udf_pushdown_mre.py")], check=True)
    return DIST.read_text()


def assert_equal(actual, expected, label: str) -> None:
    if actual != expected:
        raise AssertionError(f"{label} mismatch.\nExpected: {expected}\nActual:   {actual}")


def extract_pushdown_json(explain_row) -> dict:
    trace = json.loads(explain_row[2])
    return next(item for item in trace if item.get("type") == "pushdown")


def main() -> None:
    adapter_code = bundle_adapter()
    con = connect()
    try:
        for stmt in [
            "DROP FORCE VIRTUAL SCHEMA IF EXISTS MRE_UDF_VS CASCADE",
            "DROP SCHEMA IF EXISTS MRE_UDF_ADAPTER CASCADE",
            "DROP SCHEMA IF EXISTS MRE_UDF_SRC CASCADE",
            "CREATE SCHEMA MRE_UDF_SRC",
            "OPEN SCHEMA MRE_UDF_SRC",
            'CREATE OR REPLACE TABLE T ("value" DECIMAL(18,0))',
            "INSERT INTO T VALUES (-3)",
            "INSERT INTO T VALUES (4)",
            "INSERT INTO T VALUES (NULL)",
            "CREATE SCHEMA MRE_UDF_ADAPTER",
            "OPEN SCHEMA MRE_UDF_ADAPTER",
            "CREATE OR REPLACE LUA ADAPTER SCRIPT PASSTHROUGH_ADAPTER AS\n" + adapter_code + "\n/",
            """CREATE OR REPLACE LUA SCALAR SCRIPT IDENTITY_UDF(x DECIMAL(18,0))
RETURNS DECIMAL(18,0) AS
function run(ctx)
    return ctx.x
end
/""",
            'CREATE VIRTUAL SCHEMA MRE_UDF_VS USING "MRE_UDF_ADAPTER"."PASSTHROUGH_ADAPTER" WITH SCHEMA_NAME=\'MRE_UDF_SRC\'',
            "OPEN SCHEMA MRE_UDF_ADAPTER",
        ]:
            con.execute(stmt)

        built_in_row = con.execute('EXPLAIN VIRTUAL SELECT ABS("value") FROM MRE_UDF_VS.T').fetchall()[0]
        udf_row = con.execute('EXPLAIN VIRTUAL SELECT IDENTITY_UDF("value") FROM MRE_UDF_VS.T').fetchall()[0]

        built_in_pushdown = extract_pushdown_json(built_in_row)
        udf_pushdown = extract_pushdown_json(udf_row)

        built_in_select = built_in_pushdown["pushdownRequest"]["selectList"][0]
        udf_request = udf_pushdown["pushdownRequest"]

        assert_equal(built_in_select["type"], "function_scalar", "built-in select expression type")
        assert_equal(built_in_select["name"], "ABS", "built-in function name")
        if "selectList" in udf_request:
            raise AssertionError(f"Expected UDF pushdown request to omit selectList entirely, got: {udf_request}")

        built_in_sql = built_in_row[1]
        udf_sql = udf_row[1]
        if 'ABS("T"."value")' not in built_in_sql:
            raise AssertionError(f'Expected built-in pushdown SQL to contain ABS("T"."value"), got: {built_in_sql}')
        if 'IDENTITY_UDF' in udf_sql:
            raise AssertionError(f"Expected UDF name to be absent from pushdown SQL, got: {udf_sql}")
        if 'SELECT * FROM "MRE_UDF_SRC"."T"' not in udf_sql:
            raise AssertionError(f'Expected stripped UDF pushdown SQL to fall back to SELECT *, got: {udf_sql}')

        print("-- udf pushdown stripping mre --")
        print("built-in pushdown sql:", built_in_sql)
        print("built-in pushdown selectList[0]:", json.dumps(built_in_select, indent=2, sort_keys=True))
        print("udf pushdown sql:", udf_sql)
        print("udf pushdown request:", json.dumps(udf_request, indent=2, sort_keys=True))
    finally:
        con.close()


if __name__ == "__main__":
    main()

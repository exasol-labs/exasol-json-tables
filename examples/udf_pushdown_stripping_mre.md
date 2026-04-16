# UDF Pushdown Stripping MRE

This is a minimal reproducible example showing that an Exasol Lua Virtual Schema receives
built-in scalar functions in the pushdown request, but user-defined scalar functions are
removed before the adapter sees the query.

## Files

- [tools/test_nano_udf_pushdown_mre.py](/Users/alexander.stigsen/Dev/json-virtual-schema/tools/test_nano_udf_pushdown_mre.py)
- [tools/bundle_udf_pushdown_mre.py](/Users/alexander.stigsen/Dev/json-virtual-schema/tools/bundle_udf_pushdown_mre.py)
- [mre/udf-pushdown-stripping/src/entry.lua](/Users/alexander.stigsen/Dev/json-virtual-schema/mre/udf-pushdown-stripping/src/entry.lua)
- [mre/udf-pushdown-stripping/src/mre_adapter/PassthroughAdapter.lua](/Users/alexander.stigsen/Dev/json-virtual-schema/mre/udf-pushdown-stripping/src/mre_adapter/PassthroughAdapter.lua)

## What It Proves

The adapter is a plain passthrough wrapper over a local Exasol source schema. It does not
implement any custom rewrite logic beyond rendering the pushdown request back to SQL.

Against that adapter:

```sql
EXPLAIN VIRTUAL SELECT ABS("value") FROM MRE_UDF_VS.T;
```

pushes `ABS(...)` into the adapter request and into `PUSHDOWN_SQL`.

But:

```sql
EXPLAIN VIRTUAL SELECT IDENTITY_UDF("value") FROM MRE_UDF_VS.T;
```

does not push the UDF call. In the reproduced Nano trace, the adapter receives no `selectList`
at all and the generated `PUSHDOWN_SQL` falls back to:

```sql
SELECT * FROM "MRE_UDF_SRC"."T"
```

## Why This Matters

This prevents Virtual Schemas from using user-defined function syntax as part of their
semantic contract. The adapter cannot inspect or rewrite the UDF call because the function
is stripped before `push_down(...)` is invoked.

That limitation blocks use cases such as:

- adapter-defined helper syntax,
- semantic null / type introspection helpers,
- explicit opt-in rewrite functions implemented at the virtual-schema layer.

## Expected vs Actual

Expected:

- the adapter should receive the UDF call in `pushdownRequest`, just like it receives built-in
  scalar functions such as `ABS(...)`
- if Exasol still chooses to evaluate the UDF outside the adapter later, that decision should
  happen after the adapter has had the opportunity to inspect the original expression tree

Actual:

- built-in `ABS("value")` appears in `pushdownRequest.selectList[0]`
- `IDENTITY_UDF("value")` is absent from `pushdownRequest`
- the adapter receives no `selectList` at all for the UDF case and renders `SELECT *`

Observed Nano output from the included runner:

```text
built-in pushdown sql: SELECT ABS("T"."value") FROM "MRE_UDF_SRC"."T"
udf pushdown sql: SELECT * FROM "MRE_UDF_SRC"."T"
```

## How To Run

```bash
python3 tools/test_nano_udf_pushdown_mre.py
```

The script connects to the local Exasol Nano instance, installs a minimal passthrough adapter,
creates a one-argument identity UDF, and prints the contrasting `EXPLAIN VIRTUAL` output.

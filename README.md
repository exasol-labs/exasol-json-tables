# JSON Wrapper Views for Exasol

This repo packages a JSON-friendly SQL surface on top of `json-to-parquet` tables in Exasol.

The supported architecture is:

- generated public root views such as `JSON_VIEW.SAMPLE`
- a generated internal helper schema such as `JSON_VIEW_INTERNAL`
- a companion SQL preprocessor that adds JSON-oriented syntax on top of those views
- a package tool that generates, installs, regenerates, and validates the whole surface

## What This Feels Like

```sql
SELECT
  "id",
  CASE
    WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null'
    WHEN "note" IS NULL THEN 'missing'
    ELSE 'value'
  END AS note_state,
  JSON_TYPEOF("value") AS value_type,
  JSON_AS_VARCHAR("value") AS value_text,
  "child.value" AS child_value,
  "meta.info.note" AS deep_note,
  "tags[FIRST]" AS first_tag,
  "tags[LAST]" AS last_tag,
  "tags[SIZE]" AS tag_count,
  "meta.items[LAST].value" AS last_meta_item
FROM JSON_VIEW.SAMPLE
WHERE "tags[SIZE]" > 0
  AND (
    "tags[LAST]" = 'blue'
    OR JSON_IS_EXPLICIT_NULL("note")
  )
ORDER BY "id";
```

And when an array should behave like rows instead of a scalar:

```sql
SELECT
  s."id",
  item._index,
  item.value,
  item.label
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items"
WHERE EXISTS (
  SELECT 1
  FROM VALUE tag IN s."tags"
  WHERE tag = 'blue'
)
ORDER BY s."id", item._index;
```

## Why This Exists

`json-to-parquet` stores JSON in a relational layout:

- explicit-null provenance is stored in mask columns such as `note|n`
- mixed-type values are split across sibling columns such as `value` and `value|string`
- nested objects live in child tables linked by columns such as `child|object`
- arrays live in child tables with `_parent` and `_pos`

That layout is efficient, but not pleasant to query directly.

This repo generates a cleaner SQL surface so users can stay on the root document view and still:

- distinguish missing from explicit JSON `null`
- traverse nested objects with dot syntax
- inspect and extract mixed-type values from one logical column
- access arrays positionally with brackets
- expand arrays into rows with `JOIN ... IN ...`

## Supported Surface

The maintained user-facing surface is the wrapper package:

- public root/document views in a wrapper schema, for example `JSON_VIEW`
- hidden implementation details in a helper schema, for example `JSON_VIEW_INTERNAL`
- a scoped preprocessor that only activates JSON syntax on those wrapper schemas

Supported helper functions:

- `JSON_IS_EXPLICIT_NULL(expr)`
- `JSON_TYPEOF(expr)`
- `JSON_AS_VARCHAR(expr)`
- `JSON_AS_DECIMAL(expr)`
- `JSON_AS_BOOLEAN(expr)`

Supported syntax sugar:

- dotted paths such as `"child.value"` or `"meta.info.note"`
- bracket access such as `"tags[0]"`, `"tags[FIRST]"`, `"tags[LAST]"`, `"tags[SIZE]"`, `"tags[id]"`, or `"tags[?]"`
- mixed deep access such as `"meta.items[LAST].value"`
- array rowset syntax such as `JOIN item IN s."items"` and `JOIN VALUE tag IN s."tags"`
- iterator-row path and bracket access such as `item."nested.note"` and `entry."extras[LAST]"`

Important contract note:

- Use `JSON_TYPEOF(...)` and `JSON_AS_*` for JSON-aware variant semantics.
- Built-in `TYPEOF(...)` and plain SQL `CAST(...)` on the wrapper views reflect the projected SQL type of the view column, not the original JSON runtime type contract.
- Those helper functions also work on object-array iterator rows such as `JSON_TYPEOF(item."value")`, `JSON_AS_DECIMAL(item."amount")`, and `JSON_AS_BOOLEAN(item."enabled")` after `JOIN item IN s."items"`.
- Object-array iterator rows also support JSON path and bracket traversal such as `item."nested.note"` and `item."nested.items[LAST].value"`.
- Helper functions and path/bracket traversal are still not supported on scalar `VALUE` iterators such as `JOIN VALUE tag IN s."tags"`.
- Path/helper syntax does not resolve through derived-table roots yet. Move the JSON expression into the inner `SELECT` or query the wrapper view directly.

## Quickstart

Generate the package:

```bash
python3 tools/wrapper_package_tool.py generate \
  --source-schema JVS_SRC \
  --wrapper-schema JSON_VIEW \
  --helper-schema JSON_VIEW_INTERNAL \
  --preprocessor-schema JVS_WRAP_PP \
  --preprocessor-script JSON_WRAPPER_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_wrapper
```

This writes:

- `json_wrapper_views.sql`
- `json_wrapper_manifest.json`
- `json_wrapper_preprocessor.sql`
- `json_wrapper_package.json`

Install the generated package:

```bash
python3 tools/wrapper_package_tool.py install \
  --package-config ./dist/json_wrapper_package.json
```

The install command now prints an immediate next-step snippet:

- `ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = ...`
- a copy/paste smoke-test query against one of the generated root views
  The package tool now prefers a helper-based query over a likely populated field such as `title` or `name`,
  includes an id column for context, and orders non-`NULL` rows first so the result is visibly confirmatory.

For local/dev verification, you can also ask the installer to activate the preprocessor in its own session and run that smoke test immediately:

```bash
python3 tools/wrapper_package_tool.py install \
  --package-config ./dist/json_wrapper_package.json \
  --activate-session
```

That convenience activation is session-local to the installer process. For normal interactive work, run the printed `ALTER SESSION ...` in your own SQL session.

Validate the package:

```bash
python3 tools/wrapper_package_tool.py validate \
  --package-config ./dist/json_wrapper_package.json \
  --check-installed
```

`validate --check-installed` also prints an activation reminder plus the same high-signal smoke-test query, so operators can immediately apply the session-level step in the SQL client where they plan to use wrapper syntax.

If helper behavior changes but the wrapper views do not, regenerate only the preprocessor:

```bash
python3 tools/wrapper_package_tool.py regenerate-preprocessor \
  --package-config ./dist/json_wrapper_package.json
```

Enable the preprocessor for the current session:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

Recommended operator flow:

1. `generate`
2. `install`
3. copy/paste the printed activation + smoke-test snippet into your SQL session
4. `validate --check-installed`
5. after source-schema changes, regenerate and reinstall the package

## Structured Results

The same mapping is also useful in the other direction.

Instead of only querying existing JSON documents, you can materialize a query result back into a
source-like table family and then use it as:

- a nested result that can be queried again through the wrapper surface
- a durable scratch-layer intermediate for downstream SQL
- a shape that can be exported back to JSON-like rows later

The easiest mental model is:

- ordinary wrapper views make JSON-shaped source data easy to query
- structured results let you take SQL output and put it back into that same JSON-shaped contract

You do not need JSON-shaped input for this.

Structured results also work well when your source is plain relational tables such as:

- `orders`
- `order_items`
- `customers`
- `products`

In that case, the structured-result family becomes the layer that turns relational joins back into
a nested document-shaped output.

If the result is nested, it will usually become:

- one root table
- plus child tables for nested objects and arrays

That makes this useful for two different workflows:

- reshape JSON-derived tables into a new nested result
- produce nested document-style output from ordinary relational analytics models

There are now two authoring levels for structured results:

- `structured_shape`: a higher-level nested shape config for common cases
- `synthesized_family`: the lower-level table-family config when you need exact control

### Structured Results Quickstart

For a durable structured result, the easiest starting point is now a `structured_shape` config.
This describes the nested output shape directly and compiles down to the same source-like family contract.

Example:

```json
{
  "kind": "structured_shape",
  "rootTable": "DOC_REPORT",
  "root": {
    "fromSql": "FROM JSON_VIEW.SAMPLE",
    "idSql": "\"id\"",
    "fields": [
      {"name": "doc_id", "sql": "\"id\""},
      {"name": "items", "kind": "array_ref", "sql": "CASE WHEN \"items[SIZE]\" IS NULL THEN 0 ELSE \"items[SIZE]\" END"}
    ],
    "arrays": [
      {
        "name": "items",
        "fromSql": "FROM JSON_VIEW.SAMPLE s JOIN item IN s.\"items\"",
        "rowIdSql": "CAST((s.\"id\" * 100) + item._index + 1 AS DECIMAL(18,0))",
        "parentIdSql": "s.\"id\"",
        "positionSql": "item._index",
        "fields": [
          {"name": "label", "sql": "item.label"},
          {"name": "value", "sql": "item.value"}
        ]
      }
    ]
  }
}
```

If you want the nested result immediately, without first creating a durable package, use the one-shot
preview command:

```bash
python3 tools/structured_result_tool.py preview-json \
  --result-family-config ./dist/result_family_input.json \
  --target-schema JVS_RESULT_PREVIEW \
  --table-kind local_temporary
```

That materializes the family in the current command session and prints the nested JSON-like rows directly.

Then package that durable result family:

```bash
python3 tools/wrapper_package_tool.py generate-result-family-package \
  --source-schema JVS_RESULT_SRC \
  --wrapper-schema JSON_VIEW_RESULT \
  --helper-schema JSON_VIEW_RESULT_INTERNAL \
  --preprocessor-schema JVS_RESULT_PP \
  --preprocessor-script JSON_RESULT_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_result \
  --result-family-config ./dist/result_family_input.json
```

This writes both the normal wrapper package files and two result-family-specific artifacts:

- the persisted materialization config
- the materialized family manifest

Install it the same way as a normal wrapper package:

```bash
python3 tools/wrapper_package_tool.py install \
  --package-config ./dist/json_result_package.json
```

For result-family packages, `install` also recreates the durable source-like family before it
installs the wrapper views and preprocessor.

If you need exact control over the physical table family, you can still use the lower-level
`synthesized_family` format with explicit `tableSpecs`.

After activation, the structured result behaves like any other wrapped JSON document surface:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RESULT_PP.JSON_RESULT_PREPROCESSOR;

SELECT
  CAST("doc_id" AS VARCHAR(10)),
  COALESCE("items[FIRST].label", 'NULL'),
  COALESCE("items[LAST].value", 'NULL')
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "doc_id";
```

And if you want to turn that source-like family back into nested JSON-like rows, use the export helper
programmatically:

```python
from nano_support import connect
from result_family_json_export import export_root_family_to_json

con = connect()
rows = export_root_family_to_json(con, source_schema="JVS_RESULT_SRC", root_table="DOC_REPORT")
print(rows)
con.close()
```

Use durable structured results when you want reproducibility, operator handoff, or further SQL modeling.
Use the lower-level in-session installer when the result family only needs to live inside the current session,
for example on top of `LOCAL TEMPORARY` tables.

### Structured Results From Regular Tables

This is a very common pattern: start from ordinary relational tables, then materialize a nested result family
that is easier to query, export, or hand off as document-shaped output.

For example, suppose you have upstream tables like:

- `JVS_RELATIONAL_UPSTREAM.ORDERS`
- `JVS_RELATIONAL_UPSTREAM.ORDER_ITEMS`
- `JVS_RELATIONAL_UPSTREAM.CUSTOMERS`
- `JVS_RELATIONAL_UPSTREAM.PRODUCTS`
- `JVS_RELATIONAL_UPSTREAM.ORDER_TAGS`

You can materialize them into one nested result family with either:

- a `structured_shape` config, which is easier to author
- or a lower-level `synthesized_family` config, if you want to control every generated table

For example, here is a higher-level `structured_shape` version:

```json
{
  "kind": "structured_shape",
  "rootTable": "ORDER_REPORT",
  "root": {
    "fromSql": "FROM JVS_RELATIONAL_UPSTREAM.ORDERS o LEFT JOIN (SELECT ORDER_ID, COUNT(*) AS ITEM_COUNT FROM JVS_RELATIONAL_UPSTREAM.ORDER_ITEMS GROUP BY ORDER_ID) item_counts ON item_counts.ORDER_ID = o.ORDER_ID LEFT JOIN (SELECT ORDER_ID, COUNT(*) AS TAG_COUNT FROM JVS_RELATIONAL_UPSTREAM.ORDER_TAGS GROUP BY ORDER_ID) tag_counts ON tag_counts.ORDER_ID = o.ORDER_ID",
    "idSql": "o.ORDER_ID",
    "fields": [
      {"name": "order_id", "sql": "o.ORDER_ID"},
      {"name": "status", "sql": "o.STATUS"},
      {"name": "customer", "kind": "object_ref", "sql": "CAST(100000 + o.CUSTOMER_ID AS DECIMAL(18,0))"},
      {"name": "items", "kind": "array_ref", "sql": "COALESCE(item_counts.ITEM_COUNT, 0)"},
      {"name": "tags", "kind": "array_ref", "sql": "COALESCE(tag_counts.TAG_COUNT, 0)"}
    ]
  }
}
```

The full shape config also defines the nested `customer`, `items`, and `tags` child nodes. The compact example
above is only showing the root-level part of the shape. For a complete working example, see
[tests/test_structured_result_ergonomics.py](tests/test_structured_result_ergonomics.py).

Once packaged and installed, that relationally sourced result behaves just like any other structured result:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RELATIONAL_RESULT_PP.JSON_RELATIONAL_RESULT_PREPROCESSOR;

SELECT
  CAST("order_id" AS VARCHAR(10)),
  "customer.name",
  "items[FIRST].product.title",
  "items[LAST].sku",
  "tags[LAST]"
FROM JSON_VIEW_RELATIONAL_RESULT.ORDER_REPORT
ORDER BY "order_id";
```

And it exports back to nested JSON-like rows through the same export helper:

```python
from nano_support import connect
from result_family_json_export import export_root_family_to_json

con = connect()
rows = export_root_family_to_json(con, source_schema="JVS_RELATIONAL_RESULT_SRC", root_table="ORDER_REPORT")
print(rows)
con.close()
```

Durable structured-result families can use the same package tool. Provide a materialization config and the tool will:

- materialize the durable source-like result family into the target source schema
- write the materialization recipe and materialized family manifest into the package
- generate the wrapper/helper/preprocessor artifacts on top of that durable result family

Example:

```bash
python3 tools/wrapper_package_tool.py generate-result-family-package \
  --source-schema JVS_RESULT_SRC \
  --wrapper-schema JSON_VIEW_RESULT \
  --helper-schema JSON_VIEW_RESULT_INTERNAL \
  --preprocessor-schema JVS_RESULT_PP \
  --preprocessor-script JSON_RESULT_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_result \
  --result-family-config ./dist/result_family_input.json
```

The generated package can then be installed and validated with the same `install` and `validate` commands. For durable result-family packages, `install` recreates the packaged source family before installing the wrapper artifacts.

## Generated Artifacts

The package generator produces four artifacts:

- wrapper SQL: public root views plus helper schema objects
- manifest JSON: machine-readable description of roots, tables, relationships, and folded column families
- preprocessor SQL: the scoped wrapper preprocessor
- package config JSON: the reproducible control-plane artifact for generation/install/validate/regenerate

Checked-in examples:

- [examples/json_wrapper_views.sql](examples/json_wrapper_views.sql)
- [examples/json_wrapper_manifest.json](examples/json_wrapper_manifest.json)
- [examples/json_wrapper_preprocessor.sql](examples/json_wrapper_preprocessor.sql)
- [examples/json_wrapper_package.json](examples/json_wrapper_package.json)

## Semantics

### Missing vs explicit `null`

```sql
SELECT
  "id",
  CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS explicit_null,
  CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS missing
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

### Variant values

```sql
SELECT
  "id",
  JSON_TYPEOF("value") AS value_type,
  JSON_AS_VARCHAR("value") AS value_text,
  JSON_AS_DECIMAL("value") AS value_decimal
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

### Nested paths

```sql
SELECT
  "id",
  "child.value",
  "meta.info.note"
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

### Array access

```sql
SELECT
  "id",
  "tags[FIRST]",
  "tags[LAST]",
  "tags[SIZE]",
  "items[LAST].value",
  "tags[id]" AS tag_selected_by_id
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

### Rowset expansion

```sql
SELECT
  s."id",
  item._index,
  item.value,
  item.label
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items"
ORDER BY s."id", item._index;
```

Iterator rows can now use JSON helpers too:

```sql
SELECT
  s."id",
  item._index,
  JSON_TYPEOF(item."value") AS item_value_type,
  JSON_AS_VARCHAR(item."value") AS item_value_text,
  JSON_AS_DECIMAL(item."amount") AS item_amount_decimal,
  JSON_AS_BOOLEAN(item."enabled") AS item_enabled_boolean,
  item."nested.note" AS nested_note,
  item."nested.items[LAST].value" AS nested_last_item,
  CASE WHEN JSON_IS_EXPLICIT_NULL(item."optional") THEN '1' ELSE '0' END AS item_optional_explicit_null
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items"
ORDER BY s."id", item._index;
```

### Modeling-Friendly SQL

The same surface is intended to work in the shapes people actually use for silver/gold models:

```sql
WITH item_features AS (
  SELECT
    CAST(s."id" AS VARCHAR(10)) AS doc_id,
    CAST(item._index AS VARCHAR(10)) AS item_index,
    COALESCE(JSON_TYPEOF(item."value"), 'MISSING') AS item_value_type,
    COALESCE(JSON_AS_VARCHAR(item."nested.items[LAST].value"), 'NULL') AS nested_last_value,
    CASE WHEN JSON_IS_EXPLICIT_NULL(s."note") THEN '1' ELSE '0' END AS root_note_explicit_null
  FROM JSON_VIEW.SAMPLE s
  JOIN item IN s."items"
)
SELECT *
FROM item_features
ORDER BY doc_id, item_index;
```

In joined queries, qualify root-document helper arguments with the root alias:

```sql
JSON_IS_EXPLICIT_NULL(s."note")
JSON_TYPEOF(s."value")
```

Persisted modeling flows are supported too:

```sql
CREATE OR REPLACE VIEW JVS_ANALYTICS.ITEM_MODEL AS
SELECT
  CAST(s."id" AS VARCHAR(10)) AS doc_id,
  CAST(item._index AS VARCHAR(10)) AS item_index,
  JSON_TYPEOF(item."value") AS item_value_type,
  item."nested.items[LAST].value" AS nested_last_value
FROM JSON_VIEW.SAMPLE s
JOIN item IN s."items";
```

## Known Boundaries

- The preprocessor is session-local. Run `ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = ...` in the SQL session where you want wrapper syntax.
- In joined queries, qualify root-document helper arguments with the root alias, for example `JSON_IS_EXPLICIT_NULL(s."note")`.
- `VALUE` iterators support plain SQL on the scalar value, but JSON helper/path syntax is intentionally not supported on them.
- Path/helper syntax does not start from derived-table roots. Move the JSON expression into the inner `SELECT` or query the wrapper view directly.
- Dynamic bracket selectors support `?` and direct field names on the current row, for example `"tags[id]"` or `item."nested.items[pick].value"`. Arbitrary SQL expressions such as `"tags[id + 1]"` are still intentionally rejected.
- Use `JSON_TYPEOF(...)` and `JSON_AS_*` for JSON-aware variant semantics. Built-in `TYPEOF(...)` and plain `CAST(...)` reflect the wrapper view’s SQL types, not the original per-row JSON type contract.

## Testing

Install Python test dependencies first:

```bash
python3 -m pip install -r requirements-dev.txt
```

The Nano tests expect a local Exasol Nano instance on `127.0.0.1:8563` with the default `sys` / `exasol` credentials used by the helpers.

Primary wrapper-surface regression:

```bash
python3 tests/test_wrapper_surface.py
```

This verifies:

- public wrapper metadata and helper-schema shape
- dotted path and bracket access
- rowset expansion
- explicit-null helpers
- helper-based variant semantics
- deep recursive traversal

Wrapper package lifecycle regression:

```bash
python3 tests/test_wrapper_package_tool.py
```

This verifies:

- package generation
- targeted preprocessor regeneration
- installation
- installed-package validation
- end-to-end wrapper queries through the installed preprocessor

Wrapper preprocessor error regression:

```bash
python3 tests/test_wrapper_errors.py
```

This verifies:

- malformed path and bracket syntax errors
- iterator misuse errors
- helper arity and scope errors
- generator validation errors

Modeling and BI regression:

```bash
python3 tests/test_wrapper_modeling.py
```

This verifies:

- nested CTE stacks with mixed helper, rowset, and deep-path logic
- stacked derived tables over projected wrapper expressions
- `UNION ALL` across multiple wrapper roots with branch-local helper semantics
- `GROUP BY` / `ORDER BY` over projected wrapper expressions
- persisted `CREATE VIEW ... AS SELECT` and `CREATE TABLE ... AS SELECT` flows
- UDF usage on iterator-local helper expressions

Final wrapper-package evaluation:

```bash
python3 tests/test_wrapper_evaluation.py
```

This verifies:

- wrapper helper semantics on the installed package
- built-in SQL typing behavior on wrapper views
- UDF interoperability on the wrapper surface
- additive source-DDL refresh through package regeneration, install, and validation

Performance study:

```bash
python3 tests/study_wrapper_performance.py
```

This benchmarks the final wrapper package on Nano for:

- path traversal
- rowset iteration
- explicit-null helper queries
- helper-based variant type and extraction queries
- warm steady-state and isolated cold-start behavior

Structured result materialization study:

```bash
python3 tests/study_structured_result_materialization.py
```

This investigates whether the existing JSON table mapping can also serve as a structured-result interchange format for:

- family-preserving filtered copies of JSON documents
- synthesized nested analytical result families
- generic JSON reconstruction from source-like result tables
- local temporary result families versus durable scratch schemas

See also [structured-result-materialization-study.md](structured-result-materialization-study.md).

Result-family materializer regression:

```bash
python3 tests/test_result_family_materializer.py
```

This verifies the extracted Phase 1 materialization library for:

- family-preserving subset materialization from helper metadata
- synthesized nested result-family materialization from declarative table specs
- re-wrapping both materialized families through the normal wrapper interface

In-session wrapper installer regression:

```bash
python3 tests/test_in_session_wrapper_installer.py
```

This verifies the Phase 2 runtime install flow for:

- generating wrapper/helper objects from the current database session
- installing the companion preprocessor in the same session
- wrapping and querying a `LOCAL TEMPORARY` result family
- confirming the observed cross-session query behavior while the creating session remains alive

Result-family JSON export regression:

```bash
python3 tests/test_result_family_json_export.py
```

This verifies the Phase 3 export helper for:

- exporting a family-preserving subset back to nested JSON-like rows
- exporting durable synthesized result families back to nested JSON-like rows
- exporting in-session wrapped local-temporary result families back to nested JSON-like rows
- preserving scalar-array versus object-array reconstruction

Structured results from relational tables regression:

```bash
python3 tests/test_structured_results_from_relational.py
```

This verifies that structured results can also be built from ordinary relational upstream tables by:

- materializing a synthesized source-like family from plain relational SQL
- packaging and installing that family through the durable package workflow
- querying it through the wrapper surface with path, bracket, and rowset syntax
- exporting it back to nested JSON-like rows

Structured result ergonomics regression:

```bash
python3 tests/test_structured_result_ergonomics.py
```

This verifies the ergonomic layer for:

- authoring structured results with the higher-level `structured_shape` config
- one-shot `preview-json` materialize-and-export workflows
- durable packaging from the same higher-level shape config

Durable result-family package regression:

```bash
python3 tests/test_result_family_package_tool.py
```

This verifies the Phase 4 package flow for:

- generating a durable result-family package from a materialization config
- persisting both the materialization recipe and the materialized family manifest
- recreating the durable source family during `install`
- validating the installed source family plus wrapper/preprocessor package together

## Repo Guide

Main implementation files:

- wrapper package tool: [tools/wrapper_package_tool.py](tools/wrapper_package_tool.py)
- wrapper SQL generator: [tools/generate_wrapper_views_sql.py](tools/generate_wrapper_views_sql.py)
- wrapper preprocessor generator: [tools/generate_wrapper_preprocessor_sql.py](tools/generate_wrapper_preprocessor_sql.py)
- shared wrapper manifest/generation logic: [tools/wrapper_schema_support.py](tools/wrapper_schema_support.py)
- structured result-family materializer: [tools/result_family_materializer.py](tools/result_family_materializer.py)
- structured result preview/export CLI: [tools/structured_result_tool.py](tools/structured_result_tool.py)
- in-session wrapper installer: [tools/in_session_wrapper_installer.py](tools/in_session_wrapper_installer.py)
- result-family JSON exporter: [tools/result_family_json_export.py](tools/result_family_json_export.py)
- shared preprocessor engine: [tools/generate_preprocessor_sql.py](tools/generate_preprocessor_sql.py)
- Nano fixture helpers: [tools/nano_support.py](tools/nano_support.py)
- executable regression and benchmark entrypoints: [tests](tests)

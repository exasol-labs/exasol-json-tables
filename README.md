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
- bracket access such as `"tags[0]"`, `"tags[FIRST]"`, `"tags[LAST]"`, `"tags[SIZE]"`
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
  "items[LAST].value"
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

## Repo Guide

Main implementation files:

- wrapper package tool: [tools/wrapper_package_tool.py](tools/wrapper_package_tool.py)
- wrapper SQL generator: [tools/generate_wrapper_views_sql.py](tools/generate_wrapper_views_sql.py)
- wrapper preprocessor generator: [tools/generate_wrapper_preprocessor_sql.py](tools/generate_wrapper_preprocessor_sql.py)
- shared wrapper manifest/generation logic: [tools/wrapper_schema_support.py](tools/wrapper_schema_support.py)
- shared preprocessor engine: [tools/generate_preprocessor_sql.py](tools/generate_preprocessor_sql.py)
- Nano fixture helpers: [tools/nano_support.py](tools/nano_support.py)
- executable regression and benchmark entrypoints: [tests](tests)

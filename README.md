# JSON Virtual Schema

Lua virtual schema adapter for Exasol that wraps tables produced by [json-to-parquet](https://github.com/exasol/json-to-parquet) and makes them feel much closer to working with the original JSON.

The goal is not just to hide storage details. It is to make JSON queryable in a way that feels natural in SQL:

- scalar properties behave like ordinary columns
- nested objects can be followed with dot syntax
- mixed-type values can be inspected and cast from one logical column
- arrays can be accessed either by position or as row sources in `JOIN` and `EXISTS`

## What This Feels Like

This is the kind of SQL the interface is designed to make possible:

```sql
SELECT
  "id",
  CASE
    WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null'
    WHEN "note" IS NULL THEN 'missing'
    ELSE 'value'
  END AS note_state,
  TYPEOF("value") AS value_type,
  CAST("value" AS VARCHAR(100)) AS value_text,
  "child.value" AS child_value,
  "meta.info.note" AS deep_note,
  "tags[FIRST]" AS first_tag,
  "tags[LAST]" AS last_tag,
  "tags[SIZE]" AS tag_count,
  "meta.items[LAST].value" AS last_meta_item
FROM JSON_VS.SAMPLE
WHERE "tags[SIZE]" > 0
  AND (
    "tags[LAST]" = 'blue'
    OR JSON_IS_EXPLICIT_NULL("note")
  )
ORDER BY "id";
```

And when you want to treat an array relationally instead of positionally:

```sql
SELECT
  s."id",
  item._index,
  item.value,
  item.label
FROM JSON_VS.SAMPLE s
JOIN item IN s."items"
WHERE EXISTS (
  SELECT 1
  FROM VALUE tag IN s."tags"
  WHERE tag = 'blue'
)
ORDER BY s."id", item._index;
```

Together, those queries show the intended shape of the interface:

- JSON null semantics
- variant-type introspection and casting
- nested object traversal
- positional array access
- array size introspection
- array expansion into rows for filtering, correlation, and aggregation

Under the hood, the physical data may actually be spread across mask columns, variant columns, object-link columns, child tables, and array tables. The virtual schema and companion preprocessor let you query it in a much more JSON-shaped way.

## Why This Exists

`json-to-parquet` stores JSON in a relational layout:

- missing-vs-explicit-null information is pushed into hidden mask columns such as `note|n`
- mixed-type properties are spread across multiple columns such as `value`, `value|string`, `value|object`, `value|array`
- nested objects live in separate child tables linked through columns such as `child|object`
- arrays live in child tables with parent links and ordinality

That layout is efficient, but awkward to query directly.

This virtual schema puts a friendlier surface on top so you can:

- query one visible column for a JSON property even when the underlying storage uses multiple columns
- distinguish `{"note": null}` from a document where `note` did not exist
- follow nested object paths with dot syntax
- inspect variant values and cast them to the type you need
- work with arrays either as positional values or as rowsets using ordinary SQL constructs

## What You Get

- Null-mask columns named `<name>|n` are hidden from the virtual schema surface.
- Mixed-type properties are folded back into one visible column.
- `TYPEOF(col)` reports the actual JSON-oriented runtime type for folded columns.
- `CAST(col AS ...)` reads from the active physical variant column.
- Nested object paths can be queried with dotted identifiers such as `"meta.info.note"`.
- Arrays support two complementary styles:
  - positional access with bracket syntax such as `"tags[0]"`, `"tags[FIRST]"`, `"tags[LAST]"`, `"tags[SIZE]"`, and `"items[LAST].value"`
  - relational expansion with `JOIN item IN row."items"`, `LEFT JOIN`, and `JOIN VALUE tag IN row."tags"`
- The adapter keeps normal SQL null semantics intact.

## Recommended Setup

The best user experience is to install both:

- the virtual schema adapter
- the companion SQL preprocessor

The preprocessor enables the user-facing JSON syntax Exasol does not pass through to the adapter on its own:

- `JSON_IS_EXPLICIT_NULL(col)`
- dotted path identifiers such as `"child.value"`
- bracket access such as `"tags[0]"` and `"meta.items[1].value"`
- array rowset syntax such as `JOIN item IN row."items"` and `JOIN VALUE tag IN row."tags"`

Build the adapter:

```bash
python3 tools/bundle.py
```

Generate a preprocessor that enables both features:

```bash
python3 tools/generate_preprocessor_sql.py \
  --function-name JSON_IS_EXPLICIT_NULL \
  --virtual-schema JSON_VS \
  --rewrite-path-identifiers \
  --output ./dist/json_user_preprocessor.sql
```

The generated installer is safe by default: it creates the schema and script, but it does not change the current schema or enable preprocessing for the session unless you ask for that explicitly.

The generated preprocessor is also scope-gated by default: the JSON helper/path syntax only activates for queries over the configured JSON virtual schemas. On regular tables it fails fast with `JVS-SCOPE-ERROR` instead of silently rewriting.

Path rewriting is join-based. Both dotted paths and bracket access are expanded to explicit `LEFT OUTER JOIN`s instead of synthetic `__path__...` columns or scalar subqueries.

If you want the generated SQL to enable the preprocessor immediately for the current session too, add `--activate-session`.

Install the adapter:

```sql
CREATE OR REPLACE LUA ADAPTER SCRIPT MY_SCHEMA.JSON_VS_ADAPTER AS
-- paste dist/adapter.lua here
/

CREATE VIRTUAL SCHEMA JSON_VS
USING MY_SCHEMA.JSON_VS_ADAPTER
WITH SCHEMA_NAME='JVS_SRC';
```

Install the preprocessor:

```sql
-- paste dist/json_user_preprocessor.sql here
```

Enable it for the current session:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_PP.JSON_NULL_PREPROCESSOR;
```

That activation step is intentionally separate from installation. It avoids surprising session-level behavior during rollout, and it keeps later DDL in the same session from being preprocessed accidentally.

Recommended usage pattern:

- schema-qualify JSON virtual schema tables, for example `FROM JSON_VS.SAMPLE`
- if you install multiple JSON virtual schemas, repeat `--virtual-schema` for each one when generating the preprocessor
- in mixed-table joins, qualify JSON helper arguments such as `JSON_IS_EXPLICIT_NULL("SAMPLE"."note")` so ordinary SQL name resolution stays unambiguous

## Phase 4 Wrapper Semantic Helper Package

This repo now also includes the next migration steps toward a simpler architecture based on:

- generated public root views over the raw `json-to-parquet` tables
- a generated internal helper schema for descendant structure and hidden semantics
- a manifest-driven wrapper preprocessor scoped to the wrapper schema pair

Phase 1 established the public root-view plus helper-schema split. Phase 2 made that split usable for navigation. Phase 3 added explicit-null semantics. Phase 4 adds helper-based variant semantics on top of the same packaged wrapper/runtime contract.

What this wrapper package gives you today:

- one public view per root JSON table
- no user-visible child/object/array tables in the public wrapper schema
- an internal helper schema that mirrors the full table family for preprocessor/runtime use
- a machine-readable manifest describing root families and column groups
- a clean public wrapper schema that hides `<name>|n` mask columns
- folded visible columns for variant families, so users query one column name instead of sibling storage columns
- the same dotted-path syntax, bracket access, and array rowset syntax on top of the public root views
- `JSON_IS_EXPLICIT_NULL(...)` on the wrapper surface, including deep paths
- `JSON_TYPEOF(...)`, `JSON_AS_VARCHAR(...)`, `JSON_AS_DECIMAL(...)`, and `JSON_AS_BOOLEAN(...)` on the wrapper surface, including deep paths
- ordinary Exasol execution semantics on that surface, including normal UDF usage

What the wrapper package intentionally does not try to preserve yet:

- built-in `TYPEOF(...)` / `CAST(...)` compatibility rewriting on the wrapper surface
- full one-for-one emulation of every virtual-schema semantic rewrite outside the explicit helper surface

The recommended Phase 5 workflow is the package tool. It generates a complete package config plus the wrapper SQL artifacts in one step:

```bash
python3 tools/wrapper_package_tool.py generate \
  --source-schema JVS_SRC \
  --wrapper-schema JSON_VIEW \
  --helper-schema JSON_VIEW_INTERNAL \
  --output-dir ./dist \
  --package-name json_wrapper
```

That writes:

- `json_wrapper_views.sql`
- `json_wrapper_manifest.json`
- `json_wrapper_preprocessor.sql`
- `json_wrapper_package.json`

Install the generated package:

```bash
python3 tools/wrapper_package_tool.py install \
  --package-config ./dist/json_wrapper_package.json
```

Regenerate only the preprocessor after helper-profile changes:

```bash
python3 tools/wrapper_package_tool.py regenerate-preprocessor \
  --package-config ./dist/json_wrapper_package.json
```

Validate the package files, or validate the installed package too:

```bash
python3 tools/wrapper_package_tool.py validate \
  --package-config ./dist/json_wrapper_package.json

python3 tools/wrapper_package_tool.py validate \
  --package-config ./dist/json_wrapper_package.json \
  --check-installed
```

The lower-level generators are still available if you want to work with the artifacts directly.

Generate the wrapper SQL, manifest, and companion preprocessor in one pass:

```bash
python3 tools/generate_wrapper_views_sql.py \
  --source-schema JVS_SRC \
  --wrapper-schema JSON_VIEW \
  --helper-schema JSON_VIEW_INTERNAL \
  --output ./dist/json_wrapper_views.sql \
  --manifest-output ./dist/json_wrapper_manifest.json \
  --preprocessor-output ./dist/json_wrapper_preprocessor.sql
```

Install the generated public views + helper schema, then install and enable the preprocessor:

```sql
-- paste dist/json_wrapper_views.sql here
-- paste dist/json_wrapper_preprocessor.sql here

ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

If you only need to regenerate the preprocessor for an existing wrapper/helper schema pair, you can still use:

```bash
python3 tools/generate_wrapper_preprocessor_sql.py \
  --wrapper-schema JSON_VIEW \
  --helper-schema JSON_VIEW_INTERNAL \
  --manifest ./dist/json_wrapper_manifest.json \
  --output ./dist/json_wrapper_preprocessor.sql
```

The wrapper preprocessor is scope-gated to the configured public wrapper schemas. Path, bracket, and rowset rewrites target the generated internal helper schema, so users still query only the root/document views. `JSON_IS_EXPLICIT_NULL(...)` is now recovered through hidden projected joins against the helper contract, while malformed or unsupported helper uses still fail with clear wrapper-specific `JVS-FUNCTION-ERROR` messages. Because Exasol only allows one active preprocessor per session, compare the wrapper surface and the virtual-schema surface in separate sessions.

Variant semantics on the wrapper surface are helper-first in Phase 4:

```sql
SELECT
  CAST("id" AS VARCHAR(10)) AS doc_id,
  COALESCE(JSON_TYPEOF("value"), 'MISSING') AS value_type,
  COALESCE(JSON_AS_VARCHAR("value"), 'NULL') AS value_text,
  COALESCE(CAST(JSON_AS_DECIMAL("value") AS VARCHAR(60)), 'NULL') AS value_number,
  COALESCE(JSON_TYPEOF("shape"), 'MISSING') AS shape_type,
  COALESCE(CAST(JSON_AS_BOOLEAN("meta.flag") AS VARCHAR(10)), 'NULL') AS flag_value
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

The Nano parity suite for this prototype is [tools/test_nano_wrapper_phase0.py](tools/test_nano_wrapper_phase0.py). It compares the public wrapper surface against the current virtual schema for visible metadata, root-table queries, deep path traversal, bracket access, nested rowset iteration, explicit-null semantics, and helper-based variant semantics, while also checking that descendant/helper artifacts moved out of the public schema.

## Mental Model

Think of the virtual schema as giving you a JSON-shaped query surface over relational storage:

- plain columns behave like normal SQL columns
- `JSON_IS_EXPLICIT_NULL(col)` tells you whether the property existed and was `null`
- `TYPEOF(col)` tells you which JSON variant is active on the current row
- `CAST(col AS ...)` extracts the current scalar value from the right physical backing column
- `"a.b.c"` follows object links into child tables
- arrays can be used in two equally first-class ways:
  - `"arr[0]"`, `"arr[FIRST]"`, `"arr[LAST]"`, and `"arr[SIZE]"` for direct positional access
  - `JOIN item IN row."items"` or `JOIN VALUE tag IN row."tags"` when you want one row per element
- `"obj.items[LAST].value"` combines object traversal with positional array access
- rowset array expansion composes with ordinary SQL: `WHERE`, `EXISTS`, `GROUP BY`, `COUNT(*)`, and correlated subqueries

## Example Source Shape

Assume `json-to-parquet` produced something like this:

Root table `SAMPLE`

- `_id`
- `id`
- `name`
- `note`
- `note|n`
- `value`
- `value|string`
- `value|n`
- `child|object`
- `meta|object`
- `shape|object`
- `shape|array`

Child tables

- `SAMPLE_child.value`
- `SAMPLE_meta.flag`
- `SAMPLE_meta.info|object`
- `SAMPLE_meta_info.note`

The virtual schema lets you query that data as if it were much closer to the original JSON document.

## Query Examples

### Ordinary Projection

You query visible JSON properties directly:

```sql
SELECT
  "id",
  "name",
  "note"
FROM JSON_VS.SAMPLE
ORDER BY "id";
```

### Differentiate Explicit `null` vs Missing Property

This is the most important distinction when working with deconstructed JSON.

```sql
SELECT
  "id",
  CASE WHEN "note" IS NULL THEN 'sql-null' ELSE 'value' END AS sql_view,
  CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null' ELSE 'not-explicit-null' END AS explicit_null,
  CASE
    WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note")
    THEN 'missing'
    ELSE 'present'
  END AS presence
FROM JSON_VS.SAMPLE
ORDER BY "id";
```

Meaning:

- `"note" IS NULL` means the visible SQL value is null, regardless of why
- `JSON_IS_EXPLICIT_NULL("note")` means the property existed and its JSON value was `null`
- `"note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note")` means the property did not exist in the original JSON

### Work with Variant Properties as One Column

If the original JSON property can be a number on one row and a string on another, `json-to-parquet` stores that across multiple physical columns. The virtual schema exposes one visible column:

```sql
SELECT
  "id",
  TYPEOF("value") AS actual_type,
  CAST("value" AS VARCHAR(100)) AS as_text,
  CAST("value" AS DECIMAL(18,0)) AS as_number
FROM JSON_VS.SAMPLE
ORDER BY "id";
```

Typical result shape:

- row 1: `actual_type = NUMBER`, value comes from `value`
- row 2: `actual_type = STRING`, value comes from `value|string`
- row 3: `actual_type = NULL`, the visible value is null

This lets you keep one logical column in user queries while still recovering the original runtime type when needed.

### Explore Nested Objects with Dot Syntax

With the preprocessor enabled, you can follow object links using dotted identifiers:

```sql
SELECT
  "id",
  "child.value",
  "meta.flag",
  "meta.info.note"
FROM JSON_VS.SAMPLE
ORDER BY "id";
```

The preprocessor rewrites those references into the matching `LEFT OUTER JOIN` chain behind the scenes.

### Access Arrays by Position or Symbolic Selector

With the preprocessor enabled, arrays feel natural both as values and as row sources.

For direct positional reads:

```sql
SELECT
  "id",
  "tags[FIRST]" AS first_tag,
  "tags[LAST]" AS last_tag,
  "tags[SIZE]" AS tag_count,
  "items[LAST].value" AS last_item_value,
  "meta.items[LAST].value" AS last_meta_item_value
FROM JSON_VS.SAMPLE
ORDER BY "id";
```

This lets you combine:

- root array access such as `"tags[0]"`
- symbolic selectors such as `"tags[FIRST]"`, `"tags[LAST]"`, and `"tags[SIZE]"`
- object-array access such as `"items[1].value"` and `"items[LAST].value"`
- object path plus array access such as `"meta.items[1].value"` and `"meta.items[LAST].value"`
- filtering directly on array elements, for example `WHERE "tags[0]" = 'red'`
- filtering on symbolic selectors, for example `WHERE "tags[SIZE]" = 2` or `WHERE "metrics[LAST]" = 30`
- typed numeric access, for example `WHERE "metrics[1]" = 20`

### Iterate Arrays as Rows

When you want relational behavior instead of positional lookup, expand the array into rows:

```sql
SELECT
  s."id",
  item._index,
  item.value,
  item.label
FROM JSON_VS.SAMPLE s
JOIN item IN s."items"
ORDER BY s."id", item._index;
```

For scalar arrays:

```sql
SELECT
  s."id",
  tag._index,
  tag
FROM JSON_VS.SAMPLE s
JOIN VALUE tag IN s."tags"
ORDER BY s."id", tag._index;
```

And because arrays become ordinary row sources, they work naturally inside correlated subqueries:

```sql
SELECT s."id"
FROM JSON_VS.SAMPLE s
WHERE EXISTS (
  SELECT 1
  FROM item IN s."items"
  WHERE item.value = 'second'
    AND item.label = 'B'
);
```

This is the key complement to bracket access:

- bracket syntax is best when you want one known position such as the first, last, or nth value
- `JOIN ... IN ...` is best when you want filtering, `EXISTS`, aggregation, same-element binding, or one row per element

Both styles are part of the intended JSON query surface, and they can be mixed freely in the same query.

### Combine Flat, Variant, and Nested JSON Logic

This is the kind of query the adapter is intended to make straightforward:

```sql
SELECT
  "id",
  "name",
  CASE
    WHEN JSON_IS_EXPLICIT_NULL("note") THEN 'explicit-null'
    WHEN "note" IS NULL THEN 'missing'
    ELSE 'value'
  END AS note_status,
  TYPEOF("value") AS value_type,
  CAST("value" AS VARCHAR(100)) AS value_as_text,
  "meta.flag" AS meta_flag,
  "meta.info.note" AS deep_note
FROM JSON_VS.SAMPLE
WHERE EXISTS (
  SELECT 1
  FROM item IN "SAMPLE"."items"
  WHERE item.label = 'B'
)
ORDER BY "id";
```

That gives you one query surface for:

- presence semantics
- variant-type inspection
- scalar extraction
- nested traversal
- rowset array predicates

## SQL Semantics

The adapter does not redefine normal SQL null behavior.

- `col IS NULL` keeps its ordinary SQL meaning
- `JSON_IS_EXPLICIT_NULL(col)` is the JSON-specific helper

This matters because users need both views:

- SQL null behavior for ordinary filtering and joins
- JSON presence semantics when distinguishing missing from explicit `null`

## Current Limits

### The preprocessor is required for the full surface

Exasol currently strips both of these before normal virtual-schema pushdown:

- user-defined scalar function calls such as `JSON_IS_EXPLICIT_NULL(col)`
- quoted dotted identifiers such as `"meta.info.note"`
- quoted array access such as `"tags[0]"` and `"meta.items[1].value"`
- array rowset syntax such as `JOIN item IN row."items"` and `JOIN VALUE tag IN row."tags"`

That is why the companion `SQL_PREPROCESSOR_SCRIPT` is part of the recommended setup.

The generated preprocessor also intentionally restricts this syntax to the configured JSON virtual schemas. That prevents accidental rewrites on ordinary tables in source schemas such as `JVS_SRC`.

### Join-Based Path Rewriting

- quoted dot paths such as `"meta.info.note"` are rewritten into explicit joins against the exposed child virtual tables
- bracket access such as `"tags[0]"`, `"tags[LAST]"`, `"tags[SIZE]"`, and `"items[LAST].value"` uses the same join-based rewrite
- array rowset syntax lowers to explicit joins against the exposed array child tables, with `_index` exposed as a user-facing alias for `_pos`
- metadata stays clean, because synthetic `__path__...` columns are no longer exposed
- array element expressions keep the physical child-column type instead of being forced to `VARCHAR`
- `SIZE` reads the parent `<name>|array` length column directly, so it does not need an array-child join

Current boundaries:

- it is designed for the normal case where the query starts from one base virtual table and path traversal hangs off that table
- if a joined child table exposes the same leaf column name as an unqualified base-table column, qualify the base-table reference, for example `JSON_IS_EXPLICIT_NULL("SAMPLE"."note")`
- rowset array paths currently target array properties directly; for example `JOIN item IN s."items"` or `JOIN entry IN d."chain.entries"`
- when selecting multiple array expressions at once, alias them explicitly if you want stable user-facing result names, because the underlying child columns are often called `_value` or `value`
- prefer explicit projection over `SELECT *` in queries whose filters trigger path or array joins, because Exasol expands `*` before pushdown

## Adapter Properties

- `SCHEMA_NAME` required source schema to wrap
- `TABLE_FILTER` optional comma-separated list of source tables to expose
- `LOG_LEVEL` and `DEBUG_ADDRESS` standard VSCL logging properties

## Testing

Install the Python test dependency first:

```bash
python3 -m pip install -r requirements-dev.txt
```

The Nano integration tests assume a local Exasol Nano instance on `127.0.0.1:8563`
with the default `sys` / `exasol` credentials used by the test helpers.

Smoke test against the local Nano fixture:

```bash
python3 tools/test_nano.py
```

This verifies:

- hidden null-mask columns
- folded variant columns
- `TYPEOF(...)` rewrite
- `CAST(...)` rewrite
- clean root-table metadata without synthetic path columns

Preprocessor regression test:

```bash
python3 tools/test_nano_preprocessor.py
```

This verifies:

- configurable helper aliases such as `JSON_IS_EXPLICIT_NULL(...)`
- dotted path syntax
- join-based bracket access in projection and predicates
- rowset array iteration with `JOIN ... IN ...`, `LEFT JOIN`, `JOIN VALUE`, grouping, and correlated subqueries
- symbolic bracket selectors `FIRST`, `LAST`, and `SIZE`
- comment-safe path rewriting and preservation of quoted dotted aliases
- missing-vs-explicit-null predicates
- `EXPLAIN VIRTUAL` output for rewritten mask references and join-based path expansion

Preprocessor error regression test:

```bash
python3 tools/test_nano_preprocessor_errors.py
```

This verifies:

- clear errors for unsupported array selectors and malformed path syntax
- clear errors when `SIZE` is used as a non-terminal selector
- clear errors for unsupported query shapes that cannot be path-rewritten safely
- clear errors when helper functions are called with the wrong arity or malformed parentheses
- generator-side validation of invalid installer identifiers

Comprehensive end-to-end test:

```bash
python3 tools/test_nano_e2e.py
```

This verifies:

- deep recursive object traversal across many child tables
- explicit-null vs missing semantics on deep paths
- variant `TYPEOF(...)` and `CAST(...)` on deep paths
- root and deeply prefixed array indexing
- deep nested rowset iteration across object arrays and nested scalar arrays
- symbolic array selectors at root and deep nested paths
- filtering and aggregation over rewritten JSON-oriented expressions
- `EXPLAIN VIRTUAL` output for deep joins without array scalar subqueries

Join-mode path regression test:

```bash
python3 tools/test_nano_preprocessor_join_paths.py
```

This verifies:

- shallow and deep dot-path traversal through the join-mode preprocessor
- coexistence with array indexing and explicit-null helpers
- `EXPLAIN VIRTUAL` output without synthetic path-column references or array scalar subqueries

Wrapper-view Phase 4 parity test:

```bash
python3 tools/test_nano_wrapper_phase0.py
```

This verifies:

- generated public root-view metadata matches the current virtual schema for the tested root tables
- the public wrapper schema exposes only the root/document views
- the internal helper schema contains the descendant structure and generated metadata tables
- root-table path, bracket, and rowset queries behave the same on `JSON_VIEW` as on `JSON_VS`
- deep recursive path and nested rowset traversal also match on the wrapper surface
- packaged wrapper generation emits the companion preprocessor and optional activation SQL
- root and deep-path explicit-null semantics now match the virtual schema
- root and deep-path helper-based variant semantics now match the virtual schema behavior for the covered cases
- malformed or unsupported wrapper-helper uses still fail clearly
- the wrapper-view surface remains an ordinary SQL surface for UDF-friendly queries

Wrapper package tool regression test:

```bash
python3 tools/test_nano_wrapper_package_tool.py
```

This verifies:

- full package generation into a deterministic output directory
- targeted preprocessor regeneration from package config + manifest
- installation of the generated wrapper package
- installed-package validation against Nano
- end-to-end wrapper queries through the installed preprocessor

Phase 6 retirement evaluation:

```bash
python3 tools/test_nano_phase6_evaluation.py
```

This verifies:

- installed wrapper-package parity for explicit-null and helper-based variant semantics
- UDF interoperability on the wrapper surface
- the remaining built-in `TYPEOF(...)` compatibility gap versus the virtual schema
- additive source-DDL refresh through package regeneration, install, and validation

Performance study:

```bash
python3 tools/study_preprocessor_only_performance.py
```

This benchmarks the final wrapper package against the virtual schema on Nano for:

- path traversal
- rowset iteration
- explicit-null helper queries
- helper-based variant type and extraction queries
- warm steady-state and isolated cold-start behavior

## Related Files

- Adapter entrypoint: [src/entry.lua](src/entry.lua)
- Adapter bundle tool: [tools/bundle.py](tools/bundle.py)
- Preprocessor generator: [tools/generate_preprocessor_sql.py](tools/generate_preprocessor_sql.py)
- Wrapper-view generator: [tools/generate_wrapper_views_sql.py](tools/generate_wrapper_views_sql.py)
- Wrapper preprocessor generator: [tools/generate_wrapper_preprocessor_sql.py](tools/generate_wrapper_preprocessor_sql.py)
- Wrapper package tool: [tools/wrapper_package_tool.py](tools/wrapper_package_tool.py)
- Wrapper manifest helper: [tools/wrapper_schema_support.py](tools/wrapper_schema_support.py)
- Example preprocessor: [examples/json_path_preprocessor.sql](examples/json_path_preprocessor.sql)
- Example wrapper SQL: [examples/json_wrapper_views.sql](examples/json_wrapper_views.sql)
- Example wrapper manifest: [examples/json_wrapper_manifest.json](examples/json_wrapper_manifest.json)
- Example wrapper preprocessor: [examples/json_wrapper_preprocessor.sql](examples/json_wrapper_preprocessor.sql)
- Wrapper Phase 4 parity test: [tools/test_nano_wrapper_phase0.py](tools/test_nano_wrapper_phase0.py)
- UDF pushdown MRE write-up: [examples/udf_pushdown_stripping_mre.md](examples/udf_pushdown_stripping_mre.md)

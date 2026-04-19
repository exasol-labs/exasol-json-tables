# Query Surface

This page is the detailed reference for the JSON-friendly SQL surface that Exasol JSON Tables installs on top of the raw source tables.

## What The Wrapper Surface Is

The maintained user-facing query surface is a wrapper package consisting of:

- public root/document views in a wrapper schema, for example `JSON_VIEW`
- a helper schema, for example `JSON_VIEW_INTERNAL`
- a scoped SQL preprocessor that only activates JSON syntax on those wrapper schemas

This means users query the wrapper views, not the raw helper tables produced by ingestion.

## Before You Query

Make sure the wrapper package has been installed, then activate the preprocessor in the SQL session where you want to use the JSON-aware syntax:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

Without that activation, the wrapper views still exist, but the extra JSON syntax sugar such as dotted paths and bracket access will not be rewritten.

## Supported Surface

### Helper Functions

- `JSON_IS_EXPLICIT_NULL(expr)`
- `JSON_TYPEOF(expr)`
- `JSON_AS_VARCHAR(expr)`
- `JSON_AS_DECIMAL(expr)`
- `JSON_AS_BOOLEAN(expr)`

### Final-Output Function

- `TO_JSON(*)`
- `TO_JSON(col1, col2, ...)`

### Syntax Sugar

- dotted paths such as `"child.value"` or `"meta.info.note"`
- bracket access such as `"tags[0]"`, `"tags[FIRST]"`, `"tags[LAST]"`, `"tags[SIZE]"`, `"tags[id]"`, `"tags[?]"`, or prepared-statement-safe `"tags[PARAM]"`
- mixed deep access such as `"meta.items[LAST].value"`
- array rowset syntax such as `JOIN item IN s."items"` and `JOIN VALUE tag IN s."tags"`
- iterator-row path and bracket access such as `item."nested.note"` and `entry."extras[LAST]"`

## Core Semantics

### Final JSON Output With `TO_JSON`

`TO_JSON` is the primary final outlet when you want JSON back out of a query.

On wrapped roots, it serializes the row recursively:

```sql
SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW.SAMPLE
ORDER BY "_id";
```

For selected top-level properties, keep the same root and name them explicitly:

```sql
SELECT TO_JSON("id", "meta", "items") AS doc_json
FROM JSON_VIEW.SAMPLE
ORDER BY "_id";
```

For ordinary tables or ordinary views, `TO_JSON` is also available, but it is a flat row serializer rather than a recursive wrapper export:

```sql
SELECT TO_JSON(*) AS row_json
FROM ANALYTICS_ROWS
ORDER BY "id";
```

Important boundaries:

- on wrapped roots, `TO_JSON(*)` recursively serializes the full document row
- on wrapped roots, `TO_JSON("field1", "field2")` recursively serializes only the selected top-level branches
- in joined wrapper queries, use qualified top-level properties such as `TO_JSON(s."id", s."meta")`; joined `TO_JSON(*)` is not supported
- on ordinary tables and ordinary views, `TO_JSON(*)` and `TO_JSON(alias.*)` are flat serializers
- on contract-encoded source-family tables, `TO_JSON(*)` is intentionally rejected; use the wrapper root instead
- in joined ordinary-table queries, use `TO_JSON(alias.*)` or qualified columns such as `TO_JSON(s."id", s."name")`
- nested paths such as `TO_JSON("meta.info.note")`, bracket expressions such as `TO_JSON("tags[SIZE]")`, and derived-table sources are not supported yet

### Missing vs Explicit `null`

Use `JSON_IS_EXPLICIT_NULL(...)` when you care about the difference between:

- a property that was present and explicitly `null`
- a property that was missing from the original JSON

Example:

```sql
SELECT
  "id",
  CASE WHEN JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS explicit_null,
  CASE WHEN "note" IS NULL AND NOT JSON_IS_EXPLICIT_NULL("note") THEN '1' ELSE '0' END AS missing
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

### Variant Values

Use `JSON_TYPEOF(...)` and `JSON_AS_*` for JSON-aware variant semantics:

```sql
SELECT
  "id",
  JSON_TYPEOF("value") AS value_type,
  JSON_AS_VARCHAR("value") AS value_text,
  JSON_AS_DECIMAL("value") AS value_decimal
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

When a variant contains a non-scalar branch:

- if `JSON_TYPEOF(expr) = 'OBJECT'`, traverse it with normal dotted paths such as `expr."note"` or `"flex.note"`
- if `JSON_TYPEOF(expr) = 'ARRAY'`, use bracket access or rowset expansion such as `"flex[LAST].value"` or `JOIN item IN row."flex"`
- `JSON_AS_VARCHAR(...)`, `JSON_AS_DECIMAL(...)`, and `JSON_AS_BOOLEAN(...)` are scalar extractors; object and array branches return `NULL` from those helpers until you navigate to a scalar child

Example:

```sql
SELECT
  "doc_id",
  JSON_TYPEOF("flex") AS flex_type,
  "flex.note" AS flex_object_note,
  "flex[LAST].value" AS flex_array_last_value,
  JSON_AS_VARCHAR("flex") AS flex_scalar_text
FROM JSON_VIEW.DOCS
ORDER BY "doc_id";
```

Important: built-in `TYPEOF(...)` and plain SQL `CAST(...)` on wrapper views reflect the projected SQL type of the view column, not the original per-row JSON type contract.

### Nested Paths

```sql
SELECT
  "id",
  "child.value",
  "meta.info.note"
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

### Array Access

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

Dynamic bracket selectors support:

- numeric literals
- `FIRST`
- `LAST`
- `SIZE`
- `?`
- direct field names on the current row, such as `"tags[id]"` or `item."nested.items[pick].value"`

Arbitrary SQL expressions such as `"tags[id + 1]"` are intentionally rejected.

### Array Expansion Into Rows

When arrays should behave like rowsets, use `JOIN ... IN ...`:

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

If you want the scalar value directly from a scalar array, use a `VALUE` iterator:

```sql
SELECT
  s."id",
  tag._index,
  tag
FROM JSON_VIEW.SAMPLE s
JOIN VALUE tag IN s."tags"
ORDER BY s."id", tag._index;
```

## Iterator-Row Semantics

Object-array iterator rows can use JSON helpers too:

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

Current boundary:

- object-array iterator rows support JSON helpers, path traversal, and bracket traversal
- scalar `VALUE` iterators support plain SQL on the scalar value, but JSON helper/path syntax is intentionally not supported on them

## Modeling-Friendly SQL

The wrapper surface is meant to work in normal modeling shapes, not just direct ad hoc queries.

### CTEs

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

### Persisted Modeling Objects

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

### Joined Queries

In joined queries, qualify root-document helper arguments with the root alias:

```sql
JSON_IS_EXPLICIT_NULL(s."note")
JSON_TYPEOF(s."value")
```

Do the same for wrapper-root `TO_JSON(...)` subset exports:

```sql
SELECT TO_JSON(s."id", s."meta")
FROM JSON_VIEW.SAMPLE s
JOIN JVS_DIM.DOC_FLAGS f
  ON f.DOC_ID = s."id";
```

## Known Boundaries

- The preprocessor is session-local. Activate it in the SQL session where you want wrapper syntax.
- In joined queries, qualify root-document helper arguments with the root alias, for example `JSON_IS_EXPLICIT_NULL(s."note")`.
- `TO_JSON(*)` is the primary final-output surface on wrapped roots, but joined wrapper queries must use qualified top-level subsets such as `TO_JSON(s."id", s."meta")`.
- On ordinary tables and ordinary views, `TO_JSON` is a flat row serializer and joined queries should use `TO_JSON(alias.*)` or qualified columns.
- Path/helper syntax does not start from derived-table roots yet. Move the JSON expression into the inner `SELECT` or query the wrapper view directly.
- `VALUE` iterators support plain SQL on the scalar value, but JSON helper/path syntax is intentionally not supported on them.
- Use `JSON_TYPEOF(...)` and `JSON_AS_*` for JSON-aware variant semantics. Built-in `TYPEOF(...)` and plain `CAST(...)` reflect wrapper view SQL types, not the original per-row JSON type contract.

## Where To Go Next

- Installation and setup: [installation.md](installation.md)
- Ingest details: [ingest.md](ingest.md)
- Structured outputs: [structured-results.md](structured-results.md)

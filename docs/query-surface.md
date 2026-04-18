# Query Surface

This document is the detailed reference for the wrapper SQL surface.

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

Important contract notes:

- Use `JSON_TYPEOF(...)` and `JSON_AS_*` for JSON-aware variant semantics.
- Built-in `TYPEOF(...)` and plain SQL `CAST(...)` on the wrapper views reflect the projected SQL type of the view column, not the original JSON runtime type contract.
- Those helper functions also work on object-array iterator rows such as `JSON_TYPEOF(item."value")`, `JSON_AS_DECIMAL(item."amount")`, and `JSON_AS_BOOLEAN(item."enabled")` after `JOIN item IN s."items"`.
- Object-array iterator rows also support JSON path and bracket traversal such as `item."nested.note"` and `item."nested.items[LAST].value"`.
- Helper functions and path/bracket traversal are still not supported on scalar `VALUE` iterators such as `JOIN VALUE tag IN s."tags"`.
- Path/helper syntax does not resolve through derived-table roots yet. Move the JSON expression into the inner `SELECT` or query the wrapper view directly.

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

Iterator rows can use JSON helpers too:

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

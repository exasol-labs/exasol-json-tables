# JSON Wrapper Views for Exasol

This repo turns `json-to-parquet` tables in Exasol into a much nicer SQL surface.

Instead of querying raw helper columns like `note|n`, `child|object`, or array child tables directly, you get:

- root document views such as `JSON_VIEW.SAMPLE`
- JSON-aware helper functions such as `JSON_IS_EXPLICIT_NULL(...)` and `JSON_TYPEOF(...)`
- dotted path access like `"meta.info.note"`
- bracket access like `"tags[LAST]"` or `"items[id]"`
- array expansion like `JOIN item IN s."items"`

It also goes the other way: you can materialize SQL results back into the same source-like table-family shape and use that as a nested structured result.

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
  "tags[SIZE]" AS tag_count
FROM JSON_VIEW.SAMPLE
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
- access arrays positionally or expand them into rows
- materialize nested structured results back into a reusable source-like table family

## Installation

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

Install it:

```bash
python3 tools/wrapper_package_tool.py install \
  --package-config ./dist/json_wrapper_package.json
```

Validate it:

```bash
python3 tools/wrapper_package_tool.py validate \
  --package-config ./dist/json_wrapper_package.json \
  --check-installed
```

Enable wrapper syntax in the SQL session where you want to use it:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

Recommended operator flow:

1. `generate`
2. `install`
3. run the printed activation and smoke-test snippet in your SQL session
4. `validate --check-installed`
5. after source-schema changes, regenerate and reinstall the package

## Feature Overview

The maintained user-facing surface is:

- public root/document views in a wrapper schema, for example `JSON_VIEW`
- a generated helper schema, for example `JSON_VIEW_INTERNAL`
- a scoped SQL preprocessor that only activates JSON syntax on those wrapper schemas

Core helpers:

- `JSON_IS_EXPLICIT_NULL(expr)`
- `JSON_TYPEOF(expr)`
- `JSON_AS_VARCHAR(expr)`
- `JSON_AS_DECIMAL(expr)`
- `JSON_AS_BOOLEAN(expr)`

Core syntax:

- dotted paths like `"child.value"` and `"meta.info.note"`
- bracket access like `"tags[0]"`, `"tags[FIRST]"`, `"tags[LAST]"`, `"tags[SIZE]"`, `"tags[id]"`, and `"tags[?]"`
- mixed deep traversal like `"meta.items[LAST].value"`
- rowset expansion like `JOIN item IN s."items"` and `JOIN VALUE tag IN s."tags"`
- iterator-row JSON traversal like `item."nested.note"` and `item."nested.items[LAST].value"`

Structured results:

- materialize SQL output back into a source-like table family
- query that result again through the same wrapper surface
- export it back to nested JSON-like rows
- build document-shaped output from either JSON-derived data or ordinary relational tables

## Documentation

- Query surface reference: [docs/query-surface.md](docs/query-surface.md)
- Structured results: [docs/structured-results.md](docs/structured-results.md)
- Testing and validation: [docs/testing.md](docs/testing.md)
- Generated artifacts and code map: [docs/developer-guide.md](docs/developer-guide.md)

### Agent skills:

- MongoDB migration skill: [skills/mongodb-workload-migration/SKILL.md](skills/mongodb-workload-migration/SKILL.md)

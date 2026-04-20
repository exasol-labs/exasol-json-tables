# Structured Results

Structured results let you take SQL output and put it back into the same nested contract that Exasol JSON Tables uses for ingested JSON.

That means a result can become:

- a nested result that can be queried again through the wrapper surface
- a durable intermediate result for downstream SQL
- a wrapped result that can be emitted as final JSON through `TO_JSON(...)`

This is useful both when the input already came from JSON and when the input is ordinary relational data that you want to turn into document-shaped output.

## The Mental Model

The easiest way to think about it is:

- the wrapper surface makes JSON-shaped source data pleasant to query
- structured results take SQL output and put it back into that same JSON-shaped contract
- `TO_JSON(*)` or `TO_JSON("field1", "field2")` is usually the final outlet once that wrapped contract exists

If the result is nested, it will usually become:

- one root table
- plus child tables for nested objects and arrays

## When To Use Structured Results

Use structured results when:

- you want nested output from ordinary relational tables
- you want to persist a nested intermediate result inside Exasol
- you want to keep shape-building inside SQL instead of rebuilding everything in application code
- you want the final nested output to come from the SQL surface through `TO_JSON(...)`

If you only need a flat analytical result, plain SQL tables or views are usually enough.

## Config-First Authoring

The primary structured-results authoring story is:

1. write a JSON config
2. validate the shape rules
3. package or preview it
4. use `TO_JSON(...)` on the wrapped result when you want final output

There are still two config shapes:

- `structured_shape`
  This is the higher-level, recommended starting point for common nested outputs.
- `synthesized_family`
  This is the lower-level format when you want exact control over the generated table family.

In practice, start with `structured_shape` unless you already know you need exact table-by-table control.

## Common Validation Rules

Structured results are easiest to author once the contract rules are explicit:

- `fromSql` is a `FROM` / `JOIN` clause fragment, not a full `SELECT`
- every nested object needs a matching parent field with `kind: "object_ref"`
- every nested array needs a matching parent field with `kind: "array_ref"`
- object arrays need `rowIdSql`
- scalar arrays use `valueSql` and must not also define nested fields, objects, or arrays
- if you want lowercase JSON property names from authored SQL aliases, quote those aliases explicitly

If you are building configs or specs programmatically, validate before materialization:

```python
import json
from pathlib import Path

from exasol_json_tables.result_family_materializer import (
    result_family_spec_from_dict,
    validate_result_family_spec,
)

spec = result_family_spec_from_dict(json.loads(Path("result_family.json").read_text()))
validate_result_family_spec(spec)
```

For the runnable regression behind this config-first workflow, see [tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py).

## Two Main Workflows

### 1. One-Shot Preview

Use this when you want to check the nested result shape immediately:

```bash
exasol-json-tables structured-results preview-json \
  --result-family-config ./dist/result_family_input.json \
  --target-schema JVS_RESULT_PREVIEW \
  --table-kind local_temporary
```

That materializes the family in the current command session and prints the JSON rows directly. Treat it as a fast validation tool for the shape, not the primary durable output surface.

Internally, `preview-json` now installs a temporary wrapper over the materialized family and emits rows through `TO_JSON(*)`. That keeps the preview path aligned with the same SQL-native final-output surface used by durable wrapped results.

### 2. Durable Package

Use this when you want a reusable result family that can be installed, queried again, and handed off operationally:

```bash
exasol-json-tables structured-results package \
  --source-schema JVS_RESULT_SRC \
  --wrapper-schema JSON_VIEW_RESULT \
  --helper-schema JSON_VIEW_RESULT_INTERNAL \
  --preprocessor-schema JVS_RESULT_PP \
  --preprocessor-script JSON_RESULT_PREPROCESSOR \
  --output-dir ./dist \
  --package-name json_result \
  --result-family-config ./dist/result_family_input.json
```

This writes:

- the normal wrapper package files
- the persisted materialization config
- the materialized family manifest

Install it like any other wrapper package:

```bash
exasol-json-tables wrap install \
  --package-config ./dist/json_result_package.json
```

For result-family packages, `install` also recreates the durable source-like family before it installs the wrapper views and preprocessor.

## Primary Final Outlet: `TO_JSON`

Once a structured result is wrapped, the normal way to emit final JSON is SQL:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_RESULT_PP.JSON_RESULT_PREPROCESSOR;

SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "_id";
```

If you only want selected top-level properties, keep the same wrapped root and choose them explicitly:

```sql
SELECT TO_JSON("doc_id", "items") AS doc_json
FROM JSON_VIEW_RESULT.DOC_REPORT
ORDER BY "_id";
```

Important semantics:

- on wrapped result families, `TO_JSON(*)` recursively serializes the whole wrapped row
- `TO_JSON("field1", "field2")` keeps only the selected top-level properties and recursively serializes those branches
- on ordinary tables or views, `TO_JSON` is a flat row serializer; nested output still comes from materializing a family first
- `structured-results preview-json` remains useful for one-shot validation, but it is no longer a separate output model; it now exercises the same wrapped `TO_JSON(*)` path as the primary SQL surface

For durable flat exports built on top of structured results, use the same identifier conventions as other published SQL objects:

- prefer uppercase aliases for columns that will be queried later without quotes
- avoid reserved-word aliases such as `source`, `schema`, `value`, or `type`
- keep natural property names inside `TO_JSON(...)` output rather than forcing SQL-style uppercase into the JSON payload

See [identifier-conventions.md](identifier-conventions.md) for the recommended defaults.

## Quickstart Example

This is the end-to-end generated-package path:

1. quickstart a wrapped JSON surface
2. model a nested structured result on top of that wrapper
3. package and deploy the result
4. finish with `TO_JSON(*)`

Start with the normal one-shot wrapper workflow:

```bash
exasol-json-tables ingest-and-wrap \
  --input ./sample.json \
  --name quickstart_sample \
  --wrapper-schema JSON_VIEW_SAMPLE \
  --helper-schema JSON_VIEW_SAMPLE_INTERNAL \
  --preprocessor-schema JVS_SAMPLE_PP \
  --preprocessor-script JSON_SAMPLE_PREPROCESSOR \
  --package-name quickstart_sample_wrapper \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol-temp-dir /tmp/exasol-json-tables
```

Then author a `structured_shape` config over that generated wrapper surface:

```json
{
  "kind": "structured_shape",
  "rootTable": "SAMPLE_REPORT",
  "root": {
    "fromSql": "FROM \"JSON_VIEW_SAMPLE\".\"sample\" s",
    "idSql": "s.\"id\"",
    "fields": [
      {"name": "sample_id", "sql": "s.\"id\""},
      {"name": "name", "sql": "JSON_AS_VARCHAR(s.\"name\")"},
      {
        "name": "note_state",
        "sql": "CASE WHEN JSON_IS_EXPLICIT_NULL(s.\"note\") THEN 'explicit-null' WHEN s.\"note\" IS NULL THEN 'missing' ELSE 'value' END"
      },
      {"name": "summary", "kind": "object_ref", "sql": "s.\"id\""},
      {
        "name": "tags",
        "kind": "array_ref",
        "sql": "CASE WHEN s.\"tags[SIZE]\" IS NULL THEN 0 ELSE s.\"tags[SIZE]\" END"
      }
    ],
    "objects": [
      {
        "name": "summary",
        "fromSql": "FROM \"JSON_VIEW_SAMPLE\".\"sample\" s",
        "idSql": "s.\"id\"",
        "fields": [
          {"name": "team", "sql": "s.\"meta.team\""},
          {"name": "last_tag", "sql": "s.\"tags[LAST]\""}
        ]
      }
    ],
    "arrays": [
      {
        "name": "tags",
        "fromSql": "FROM \"JSON_VIEW_SAMPLE\".\"sample\" s JOIN VALUE tag IN s.\"tags\"",
        "parentIdSql": "s.\"id\"",
        "positionSql": "tag._index",
        "valueSql": "tag"
      }
    ]
  }
}
```

Package the result:

```bash
exasol-json-tables structured-results package \
  --source-schema JVS_SAMPLE_REPORT_SRC \
  --wrapper-schema JSON_VIEW_SAMPLE_REPORT \
  --helper-schema JSON_VIEW_SAMPLE_REPORT_INTERNAL \
  --preprocessor-schema JVS_SAMPLE_REPORT_PP \
  --preprocessor-script JSON_SAMPLE_REPORT_PREPROCESSOR \
  --output-dir ./dist/exasol-json-tables \
  --package-name sample_report \
  --result-family-config ./dist/exasol-json-tables/sample_report_shape.json
```

Deploy the generated result package through the normal wrapper lifecycle:

```bash
exasol-json-tables wrap deploy \
  --package-config ./dist/exasol-json-tables/sample_report_package.json
```

After activation, the wrapped result behaves like any other JSON-document surface:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_SAMPLE_REPORT_PP.JSON_SAMPLE_REPORT_PREPROCESSOR;

SELECT
  CAST("sample_id" AS VARCHAR(10)),
  COALESCE("summary.team", 'NULL'),
  COALESCE("tags[LAST]", 'NULL')
FROM JSON_VIEW_SAMPLE_REPORT.SAMPLE_REPORT
ORDER BY "sample_id";
```

And when you want the final document payload:

```sql
SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW_SAMPLE_REPORT.SAMPLE_REPORT
ORDER BY "_id";
```

For the complete runnable regression behind this example, see [tests/test_quickstart_structured_result_flow.py](../tests/test_quickstart_structured_result_flow.py).

## Structured Results From Regular Tables

This is a common pattern: start from ordinary relational tables, then materialize a nested result family that is easier to query, emit with `TO_JSON(...)`, or hand off as document-shaped output.

For example, suppose you have upstream tables like:

- `ORDERS`
- `ORDER_ITEMS`
- `CUSTOMERS`
- `PRODUCTS`
- `ORDER_TAGS`

You can materialize them into one nested result family with either:

- `structured_shape`, which is easier to author
- `synthesized_family`, if you want explicit control over every generated table

Example root-level `structured_shape` fragment:

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

For a complete working example, see [tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py).

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

For the final payload, use `TO_JSON(...)` on the wrapped result:

```sql
SELECT
  TO_JSON(*) AS full_json,
  TO_JSON("customer", "items") AS nested_subset_json
FROM JSON_VIEW_RELATIONAL_RESULT.ORDER_REPORT
ORDER BY "_id";
```

## Preview And Advanced Python Surface

There are two secondary paths to keep in mind:

- `structured-results preview-json` for one-shot shape validation without a durable install
- the narrow advanced Python materialization surface for validation, authoring, and packaging workflows

The intentionally supported advanced Python surface is in `result_family_materializer`:

- validation helpers such as `validate_result_family_spec(...)`
- materialization entry points such as `materialize_result_family(...)`
- config serialization helpers such as `result_family_spec_to_dict(...)` and `result_family_spec_from_dict(...)`

Example:

```python
import json
from pathlib import Path

from exasol_json_tables.result_family_materializer import (
    materialize_result_family,
    result_family_spec_from_dict,
    result_family_spec_to_dict,
    validate_result_family_spec,
)

spec = result_family_spec_from_dict(json.loads(Path("result_family.json").read_text()))
validate_result_family_spec(spec)
print(result_family_spec_to_dict(spec)["kind"])
```

Treat lower-level Python helpers as advanced compatibility tooling, not as the primary documented product surface. The supported primary final-output path remains wrapped SQL plus `TO_JSON(...)`.

## Choosing Between Durable And Session-Local

Use durable structured results when you want:

- reproducibility
- operator handoff
- further SQL modeling
- reusable nested results

Use the lower-level in-session path when the result family only needs to live inside the current session, for example on top of `LOCAL TEMPORARY` tables.

## See Also

- [tests/test_wrapper_to_json.py](../tests/test_wrapper_to_json.py)
- [tests/test_result_family_materializer.py](../tests/test_result_family_materializer.py)
- [tests/test_to_json_roundtrip_e2e.py](../tests/test_to_json_roundtrip_e2e.py)
- [tests/test_structured_results_from_relational.py](../tests/test_structured_results_from_relational.py)
- [tests/test_structured_result_ergonomics.py](../tests/test_structured_result_ergonomics.py)
- [tests/test_result_family_package_tool.py](../tests/test_result_family_package_tool.py)

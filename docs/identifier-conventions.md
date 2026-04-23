# Identifier Conventions

Exasol JSON Tables sits on top of Exasol SQL, so identifier behavior matters more than many users expect.

This page captures the practical conventions that reduce friction for:

- published views
- exported tables
- BI tools
- pandas / ML workflows
- agents generating SQL automatically

## Exasol Identifier Model

Exasol follows the standard SQL split between quoted and unquoted identifiers:

- unquoted identifiers are folded to uppercase
- quoted identifiers preserve the exact spelling and case

That means these are different:

```sql
SELECT event_id FROM ANALYTICS.EVENTS;
SELECT "event_id" FROM ANALYTICS.EVENTS;
```

If a column was created as `"event_id"`, then the unquoted `event_id` resolves as `EVENT_ID` and does not match.

## Practical Rule Of Thumb

Use these defaults:

- wrapper-path references stay quoted
- durable published columns and exported tables should usually use uppercase aliases
- avoid reserved words for exposed aliases

Examples:

```sql
SELECT
  "customer.id",
  "meta.info.note"
FROM JSON_VIEW.ORDERS;
```

Those wrapper expressions are part of the JSON Tables surface and should stay quoted.

But when publishing a durable table or view for downstream use, prefer:

```sql
CREATE VIEW ANALYTICS.ORDER_EXPORT AS
SELECT
  CAST("order_id" AS VARCHAR(40)) AS ORDER_ID,
  CAST("customer.id" AS VARCHAR(40)) AS CUSTOMER_ID,
  CAST("status" AS VARCHAR(20)) AS STATUS
FROM JSON_VIEW.ORDERS;
```

That lets downstream SQL use:

```sql
SELECT ORDER_ID, CUSTOMER_ID
FROM ANALYTICS.ORDER_EXPORT
ORDER BY ORDER_ID;
```

without requiring quoted lowercase names everywhere.

## When To Preserve Lowercase Names

Keep lowercase quoted names only when the consumer explicitly benefits from that exact spelling.

Typical examples:

- temporary exploration in one SQL session
- JSON property naming inside `TO_JSON(...)`
- shaped intermediate results where exact property names matter more than downstream SQL ergonomics

If you choose lowercase quoted aliases in a published object, downstream queries must quote them consistently.

## Reserved-Word Avoidance

Common field names from JSON documents often collide with SQL keywords or high-friction names.

Treat these as risky aliases:

- `source`
- `schema`
- `value`
- `type`
- `table`
- `timestamp`
- `user`
- `order`
- `group`
- `method`

Safer patterns:

- `SOURCE_SITE`
- `SCHEMA_NAME`
- `RAW_VALUE`
- `VALUE_TEXT`
- `EVENT_TYPE`
- `ORDER_TS`

If the exact name must be preserved, quote it explicitly:

```sql
SELECT "metadata.source" AS "source"
FROM JSON_VIEW.TRAINING_DATASET;
```

But for durable downstream surfaces, prefer a non-keyword alias instead.

`method` deserves extra caution on the wrapper query surface too. As an iterator alias it can be rewritten into a broken `METHOD_` token once you reference it later in the query. For example, `JOIN VALUE method IN s."tags"` may parse, but `SELECT method` or `WHERE method = 'red'` can fail unexpectedly. Prefer iterator aliases such as `tag`, `entry`, or `raw_method`.

## Agent Defaults

Agents should follow these defaults unless the user asks for something else:

1. On wrapper queries, keep property references quoted exactly as required by the surface.
2. When creating a durable view or table for downstream consumption, emit uppercase SQL-safe aliases by default.
3. Avoid reserved words automatically; prefer a descriptive suffix such as `_NAME`, `_TEXT`, `_SITE`, `_TS`, or `_ID`.
4. If the goal is final JSON output through `TO_JSON(...)`, keep the natural property names and do not invent uppercase aliases inside the JSON payload.
5. If the goal is pandas, BI, or general SQL consumption, optimize for unquoted downstream access and use uppercase aliases.

## Workflow-Specific Guidance

### Published Views

For published views created from wrapper queries:

- use uppercase aliases for columns meant for later SQL
- use quoted lowercase aliases only if consumers accept quoted identifiers

### ML / pandas Exports

For feature tables and training exports:

- use uppercase aliases
- avoid reserved words such as `SOURCE` and `TYPE`
- cast to explicit scalar SQL types where helpful
- for wrapper-syntax notebook work, keep the wrapper query interactive and only publish durable views/tables when you want `export_to_pandas()` or similar downstream access; see [python-dataframes.md](python-dataframes.md)

Example:

```sql
CREATE TABLE ML.FEATURE_MATRIX AS
SELECT
  CAST("sample_id" AS VARCHAR(40)) AS SAMPLE_ID,
  CAST("label" AS VARCHAR(20)) AS LABEL,
  CAST("split" AS VARCHAR(20)) AS SPLIT,
  CAST("features.word_count" AS DECIMAL(18,0)) AS WORD_COUNT
FROM JSON_VIEW.TRAINING_DATASET;
```

### MongoDB Migrations

For MongoDB-oriented payloads:

- rename `_id` to a business-facing field such as `DOC_ID` or `PRODUCT_ID`
- normalize EJSON before ingest
- when publishing analytical outputs, still prefer uppercase SQL-safe aliases

### Final JSON Output

For final nested output:

- keep the wrapped contract and use `TO_JSON(...)`
- do not uppercase property names just to satisfy SQL conventions
- treat SQL-friendly aliases and JSON property names as different concerns

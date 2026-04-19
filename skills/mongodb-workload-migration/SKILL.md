---
name: mongodb-workload-migration
description: Use when migrating MongoDB query or aggregation workloads into the Exasol wrapper-view plus preprocessor architecture in this repository. Covers MQL-to-SQL translation patterns, analytics-focused migration strategy, known wrapper strengths and limits, official MongoDB source patterns, and Nano validation workflow for ported workloads.
---

# MongoDB Workload Migration

## When To Use This Skill

Use this skill when the task is about:

- porting MongoDB queries into the wrapper SQL surface
- translating MQL or aggregation pipelines into Exasol SQL
- evaluating whether a MongoDB analytics workload fits the wrapper architecture
- writing migration examples, playbooks, or compatibility guidance for Mongo users
- debugging why a Mongo-style pattern feels awkward or fails on the wrapper surface

Do not use this skill for generic JSON querying unless the problem is specifically framed as a MongoDB migration or MQL translation task.

## First Moves

1. Identify whether the source workload is:
   - simple document filtering
   - array-heavy analytics
   - collection joins
   - document-shaped result construction
2. Decide whether the migration target is:
   - normalized analytical SQL
   - result-shape parity with MongoDB
3. Test one representative query on Nano before making broad claims about portability.

In this repository, inspect first:

- `README.md`
- `structured-result-materialization-study.md`
- `tests/study_mongodb_migration_focus.py`
- `tests/test_wrapper_surface.py`
- `tests/test_wrapper_errors.py`
- `tests/test_structured_result_ergonomics.py`
- `tests/test_structured_results_from_relational.py`
- `tools/wrapper_package_tool.py`
- `tools/structured_result_tool.py`

Useful live checks:

- `python3 tests/study_mongodb_migration_focus.py`
- `python3 tests/test_wrapper_surface.py`
- `python3 tests/test_wrapper_errors.py`
- `python3 tests/test_structured_result_ergonomics.py`
- `python3 tests/test_structured_results_from_relational.py`

## Migration Mental Model

The wrapper surface is strongest when MongoDB arrays can be normalized into rows.

Think in these buckets:

- **Very strong fit**:
  - dotted object-field filters
  - `$elemMatch`
  - `$unwind` + `$match` + `$group`
  - multi-collection analytical joins
  - window analytics comparable to `$setWindowFields`
  - null-vs-missing provenance

- **Good fit with shape changes**:
  - `$facet`
  - `$filter`
  - `$map`
  - grouped customer/order history

- **Weak fit today**:
  - array-preserving projection as first-class output
  - automatic nested document-shaped reconstruction without switching into the structured-results-plus-`TO_JSON(...)` path

Important practical rule:

- If the target can be expressed as rows, joins, groups, or windows, the wrapper surface is usually a good destination.
- If the target must preserve or rebuild nested document shapes, expect more manual SQL and more migration friction.
- In this repository, that nested-output gap is now addressed by **structured results plus `TO_JSON(...)`**. Use that combination when the migrated workload needs to yield nested arrays/objects again instead of plain rows.

## Core Translation Table

### Object fields

- Mongo: `"a.b.c"`
- Wrapper SQL: `"a.b.c"`

### Array element by position

- Mongo intent: `arr[0]`
- Wrapper SQL: `"arr[0]"`
- Also supported: `"arr[FIRST]"`, `"arr[LAST]"`, `"arr[SIZE]"`

### Any-element array match

- Mongo:
  - `{ "items.name": "x" }`
- Wrapper SQL:
  - `EXISTS (SELECT 1 FROM item IN row."items" WHERE item.name = 'x')`

Do not use:

- `"items.name"`

That now fails intentionally with a wrapper error telling the user to use `JOIN ... IN ...` or `[index]`.

### Same-element array match (`$elemMatch`)

- Mongo:
  - `{ items: { $elemMatch: { a: 1, b: 2 } } }`
- Wrapper SQL:
  - `EXISTS (SELECT 1 FROM item IN row."items" WHERE item.a = 1 AND item.b = 2)`

This is the right default translation for same-element semantics.

### `$unwind`

- Mongo:
  - `{ $unwind: "$items" }`
- Wrapper SQL:
  - `JOIN item IN row."items"`

For scalar arrays:

- `JOIN VALUE tag IN row."tags"`

### `$size`

- Mongo:
  - `{ items: { $size: 2 } }`
- Wrapper SQL:
  - `"items[SIZE]" = 2`

### `$lookup`

- MongoDB `$lookup` with equality or multi-field conditions
- Wrapper SQL:
  - normal SQL `JOIN`
  - plus `JOIN ... IN ...` if one side is still nested in an array

Default strategy:

1. expand the nested side into rows
2. join on business keys using ordinary SQL predicates

### `$setWindowFields`

- Mongo window pipelines usually map directly to SQL window functions
- Prefer normal SQL `OVER (...)` clauses

This is one of the wrapper architecture’s strongest migration destinations.

## Repo-Specific Strengths

These are validated in this repository against Nano:

- Correlated `EXISTS (SELECT 1 FROM item IN row."items" ...)` works.
- Array-dot misuse such as `"items.value"` fails with explicit guidance.
- Mixed `TYPEOF(...)` and `JSON_TYPEOF(...)` works without hidden-column ambiguity.
- Object-array iterators support:
  - helper functions
  - iterator-rooted path/bracket traversal
- UDFs work on the wrapper surface.

See:

- `tests/study_mongodb_migration_focus.py`
- `user-studies/mongodb-focused/README.md`

## Known Boundaries To Tell The User Early

### Result shape divergence

MongoDB often returns nested arrays/documents from aggregation stages.

The wrapper surface naturally returns:

- rows
- grouped rows
- joined rows
- windowed rows

So for these patterns, set expectations explicitly:

- `$facet`
- `$filter`
- `$map`
- `$reduce`

The analytical logic may port well, while the result shape may not.

Important update for this repository:

- there is now a supported **structured results + `TO_JSON(...)`** path for rebuilding nested outputs inside Exasol
- use it when the user needs Mongo-like nested payloads, report objects, or API-facing shapes
- do not promise MongoDB-native pipeline semantics, but do treat structured results as the preferred way to keep shape-building in the database

### VALUE iterators stay plain SQL

`VALUE` iterators intentionally do not support full JSON helper/path semantics.

Tell users:

- object arrays: use full wrapper helper/path syntax
- scalar arrays: use plain SQL on the scalar iterator value

### Derived-table roots are still limited

Path/helper syntax does not start from derived-table roots.

Move those expressions into the inner `SELECT` or query the wrapper view directly.

## Recommended Migration Workflow

1. Start from the business intent, not the Mongo syntax.
2. Decide if the port should preserve:
   - same-element semantics
   - array cardinality
   - document-shaped output
3. Translate arrays first:
   - single element -> bracket access
   - any/same element -> correlated `EXISTS`
   - full traversal -> `JOIN ... IN ...`
4. Translate joins second:
   - normalize array side into rows
   - use ordinary SQL joins for collection-to-collection logic
5. Translate output shape last.
   - if rows are acceptable, stop there
   - if nested output is required, switch to the structured-results workflow and plan to emit the final payload with `TO_JSON(...)`

## Good Default Recommendations

When a Mongo user asks “what is the SQL version of this?”:

- prefer `EXISTS` for `$elemMatch`
- prefer `JOIN ... IN ...` for `$unwind`
- prefer SQL joins for `$lookup`
- prefer window functions for `$setWindowFields`
- prefer warning about shape divergence for `$facet`, `$filter`, `$map`, `$reduce`

When a Mongo user asks for the **same nested output shape**:

- first decide whether the result really needs to stay nested
- if yes, use structured results rather than trying to force everything into one flat query result
- prefer the higher-level `structured_shape` config first
- use the one-shot preview tool to validate the shape quickly
- once the shape is installed, make `TO_JSON(*)` the default final outlet
- only drop to low-level `synthesized_family` if the shape needs exact table-family control

## What To Validate On Nano

For a serious migration task, validate all three:

1. **happy-path port**
   - does the translated SQL return the expected rows?
2. **natural wrong first attempt**
   - does Mongo-style misuse fail with a good wrapper error?
3. **shape consequence**
   - is the result analytically equivalent but structurally different from Mongo output?

If nested output matters, validate a fourth:

4. **structured output path**
   - can the migrated workload be materialized as a structured result family and emitted through `TO_JSON(*)`?

In this repository, `tests/study_mongodb_migration_focus.py` is the best starting harness.

## Practical Patterns To Reuse

### `$elemMatch`

```sql
SELECT ...
FROM wrapper.root r
WHERE EXISTS (
  SELECT 1
  FROM item IN r."items"
  WHERE ...
)
```

### `$unwind` + `$group`

```sql
SELECT item.field, COUNT(*), SUM(...)
FROM wrapper.root r
JOIN item IN r."items"
GROUP BY item.field
```

### Multi-field `$lookup`

```sql
SELECT ...
FROM wrapper.left l
JOIN wrapper.right r ON ...
JOIN item IN r."items"
WHERE l.key1 = item.key1
  AND l.key2 = item.key2
```

### `$lookup` + nested output

If the Mongo workload expects joined nested arrays/documents in the output:

- first write the analytical SQL that produces the needed rows
- then decide whether to:
  - keep rows,
  - or rebuild structure through structured results inside Exasol and emit it with `TO_JSON(...)`

### `$setWindowFields`

```sql
WITH expanded AS (...)
SELECT ...,
       SUM(metric) OVER (
         PARTITION BY ...
         ORDER BY ...
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       )
FROM expanded
```

### Structured results for Mongo-style nested output

Use this when the user wants something closer to:

- Mongo aggregation output
- API-shaped nested documents
- report payloads with nested arrays/objects

Recommended path in this repository:

1. Start with the SQL that computes the desired root/object/array content.
2. Author a `structured_shape` config that describes the output hierarchy.
3. Run:

```bash
exasol-json-tables structured-results preview-json \
  --result-family-config ./dist/result_family_input.json \
  --target-schema JVS_RESULT_PREVIEW \
  --table-kind local_temporary
```

4. If the output shape is right, package it durably with:

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

5. Install the package, activate the preprocessor, and use `TO_JSON(*)` or `TO_JSON("field1", "field2")` as the final outlet.
6. If `structured_shape` is too limiting, move down to `synthesized_family`.

Use this especially for Mongo patterns like:

- `$lookup` followed by document nesting
- `$facet` outputs that need multiple nested arrays
- grouped results that should return arrays of child objects
- analyst or API workflows that want nested result payloads instead of rows

Tell the user clearly:

- this is not MongoDB-native output semantics
- but it is the repository’s supported way to keep structural work inside Exasol rather than rebuilding everything in application code
- `preview-json` is the quick validation path; `TO_JSON(...)` is the main final-output path once the result is wrapped

## Research Anchors

Use official MongoDB docs first for source-semantics verification:

- Aggregation overview: `https://www.mongodb.com/docs/manual/aggregation/`
- `$elemMatch`: `https://www.mongodb.com/docs/manual/reference/operator/query/elemmatch/`
- `$lookup`: `https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/index.html`
- `$facet`: `https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/facet/`
- `$filter`: `https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/`
- `$map`: `https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/`
- `$reduce`: `https://www.mongodb.com/docs/manual/reference/operator/aggregation/reduce/`
- `$setWindowFields`: `https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/setWindowFields/`
- Official aggregation examples:
  - unpack arrays: `https://www.mongodb.com/docs/v8.0/tutorial/aggregation-examples/unpack-arrays/`
  - group and total data: `https://www.mongodb.com/docs/drivers/node/v6.8/aggregation/group-total/`
  - multi-field join: `https://www.mongodb.com/docs/drivers/node/v5.6/aggregation-tutorials/multi-field-join/`

## Final Rule

Do not evaluate a MongoDB migration solely by “can SQL express this?”

Always evaluate both:

- semantic portability
- result-shape portability

This wrapper architecture is now strong on semantic portability for analytics workloads.
For result shape portability:

- plain row-oriented ports are the easiest path
- structured results plus `TO_JSON(...)` are the supported path when nested output must stay inside Exasol
- the remaining gap is automatic Mongo-style output parity, not the total absence of a nested-output workflow

# Exasol JSON Tables

Exasol JSON Tables makes JSON feel natural inside Exasol.

It gives you one workflow for:

- ingesting raw JSON or NDJSON into Exasol
- querying that data with JSON-friendly SQL instead of raw helper tables
- reshaping SQL results back into the nested contract and emitting final JSON with `TO_JSON(...)` when you need it

The usual Exasol pattern for JSON is to store the document as a string and then use built-in JSON functions whenever you need to extract a field, filter on a nested value, or reshape part of the payload. That works, but it gets heavy once the data is deeply nested, reused across many queries, or needs array-aware analytics. Exasol JSON Tables is an alternative workflow: ingest JSON into a stable relational contract once, then query and reshape it through a JSON-friendly SQL surface instead of repeatedly pulling values back out of strings.

## Why Use It

Exasol JSON Tables gives you a clean JSON native interface:

- query nested fields with path syntax like `"meta.info.note"`
- index and expand arrays with syntax like `"tags[LAST]"` and `JOIN item IN s."items"`
- inspect variants with `JSON_TYPEOF(...)` and `JSON_AS_*`
- keep missing vs explicit `null` semantics intact
- materialize structured results back into a reusable nested contract
- finish with `TO_JSON(*)` or `TO_JSON("field1", "field2")` when you want final JSON output
- JSON document size is no longer bound by string size limits

It is especially useful if you want to:

- analyze semi-structured event or API data directly in Exasol
- build bronze/silver/gold pipelines on top of JSON-shaped source data
- migrate analytics workloads from MongoDB into SQL
- return nested, document-style output from ordinary relational tables through structured results plus `TO_JSON(...)`

## What You Get

Exasol JSON Tables has three main capabilities:

### Ingest

Take JSON or NDJSON and load it into Exasol using a stable table-family contract that preserves:

- nested objects
- arrays
- explicit JSON `null`
- mixed/variant fields

### Query

Install a wrapper surface on top of those source tables so users query documents instead of low-level storage details.

That surface supports:

- dotted paths
- bracket access
- rowset expansion for arrays
- explicit-null helpers
- JSON-aware variant helpers

### Reshape

Take query results and materialize them back into the same nested contract, so they can:

- be queried again through the wrapper surface
- be used as a durable intermediate result
- be emitted as final JSON through `TO_JSON(*)` or `TO_JSON(...)`

## Quick Example

After installation and wrapper setup, a query can look like this:

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
  "meta.info.note" AS deep_note,
  "tags[LAST]" AS last_tag
FROM JSON_VIEW.SAMPLE
ORDER BY "id";
```

And when arrays should behave like rows:

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

And when you want the final document back out of a wrapped family:

```sql
SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW.SAMPLE
ORDER BY "_id";
```

## Install

The supported product entrypoint is:

- `exasol-json-tables`

Install the Python package:

```bash
python3 -m pip install -e .
```

Build the Rust ingest engine:

```bash
cargo build --manifest-path crates/json_tables_ingest/Cargo.toml
```

Then verify the CLI:

```bash
exasol-json-tables --help
```

For repo-local development, `python3 -m pip install -r requirements-dev.txt` installs the same package in editable mode.

## Quickstart

The simplest end-to-end path is a single command:

```bash
exasol-json-tables ingest-and-wrap \
  --input ./data.json \
  --name customer_events \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol-temp-dir /tmp/exasol-json-tables
```

That will:

1. ingest the JSON into Exasol
2. emit a source manifest
3. generate the wrapper package
4. install it
5. validate it

After that, activate the wrapper syntax in your SQL session:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

At that point, the primary final-output surface is available too:

```sql
SELECT TO_JSON(*) AS doc_json
FROM JSON_VIEW.CUSTOMER_EVENTS;
```

## Access Modes

There are three supported ways to work with the wrapper surface:

### 1. Manual Session Activation

This is the lowest-level authoring mode:

```sql
ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT = JVS_WRAP_PP.JSON_WRAPPER_PREPROCESSOR;
```

Use it when you are exploring interactively in a SQL client and want full wrapper syntax on that session.

### 2. Connection Bootstrap

For applications, CI, and managed SQL clients, the normal pattern is to run the same activation SQL immediately after opening a connection or when checking one out from a pool.

That keeps wrapper syntax available without asking each user or request handler to remember the activation step manually.

### 3. Published Permanent Surfaces

When a wrapped family becomes part of a long-lived downstream workflow, use the wrapper as the authoring surface and publish ordinary views or tables from it.

That lets downstream consumers query the published objects without any preprocessor activation at all.

For the practical details, see [docs/installation.md](docs/installation.md#access-modes).

If you want more control, the same flow is also available as separate commands:

- `exasol-json-tables ingest`
- `exasol-json-tables wrap generate`
- `exasol-json-tables wrap install`
- `exasol-json-tables wrap deploy`
- `exasol-json-tables validate`
- `exasol-json-tables structured-results ...`

For automation and autonomous agents, the major workflow commands also support `--json`. In that mode they emit a machine-readable summary on stdout with a stable success or failure envelope plus the important outputs, such as package paths, schema names, activation SQL, smoke-test SQL, validation probes, and wrapper-scope warnings. There is also a `describe` command for package- and wrapper-surface discovery. `structured-results preview-json` is the fast one-shot preview path and now uses the same temporary wrapper plus `TO_JSON(*)` outlet as the durable SQL surface; the primary final-output path remains `TO_JSON(...)` on the installed wrapper or result wrapper.

## Further Reading

- Installation: [docs/installation.md](docs/installation.md)
- Automation: [docs/automation.md](docs/automation.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Identifier conventions: [docs/identifier-conventions.md](docs/identifier-conventions.md)
- Ingest guide: [docs/ingest.md](docs/ingest.md)
- Query surface reference: [docs/query-surface.md](docs/query-surface.md)
- Structured results: [docs/structured-results.md](docs/structured-results.md)
- Architecture: [docs/architecture.md](docs/architecture.md)
- Testing and validation: [docs/testing.md](docs/testing.md)
- Developer guide: [docs/developer-guide.md](docs/developer-guide.md)

## Skills

- Ingest skill: [skills/exasol-json-tables-ingest/SKILL.md](skills/exasol-json-tables-ingest/SKILL.md)
- Query skill: [skills/exasol-json-tables-query/SKILL.md](skills/exasol-json-tables-query/SKILL.md)
- Reshape skill: [skills/exasol-json-tables-reshape/SKILL.md](skills/exasol-json-tables-reshape/SKILL.md)
- MongoDB migration skill: [skills/mongodb-workload-migration/SKILL.md](skills/mongodb-workload-migration/SKILL.md)

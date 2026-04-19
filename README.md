# Exasol JSON Tables

Exasol JSON Tables makes JSON feel natural inside Exasol.

It gives you one workflow for:

- ingesting raw JSON or NDJSON into Exasol
- querying that data with JSON-friendly SQL instead of raw helper tables
- reshaping SQL results back into nested, JSON-like output when you need it

The usual Exasol pattern for JSON is to store the document as a string and then use built-in JSON functions whenever you need to extract a field, filter on a nested value, or reshape part of the payload. That works, but it gets heavy once the data is deeply nested, reused across many queries, or needs array-aware analytics. Exasol JSON Tables is an alternative workflow: ingest JSON into a stable relational contract once, then query and reshape it through a JSON-friendly SQL surface instead of repeatedly pulling values back out of strings.

## Why Use It

Exasol JSON Tables gives you a clean JSON native interface:

- query nested fields with path syntax like `"meta.info.note"`
- index and expand arrays with syntax like `"tags[LAST]"` and `JOIN item IN s."items"`
- inspect variants with `JSON_TYPEOF(...)` and `JSON_AS_*`
- keep missing vs explicit `null` semantics intact
- materialize structured results back into a reusable nested contract
- JSON document size is no longer bound by string size limits

It is especially useful if you want to:

- analyze semi-structured event or API data directly in Exasol
- build bronze/silver/gold pipelines on top of JSON-shaped source data
- migrate analytics workloads from MongoDB into SQL
- return nested, document-style output from ordinary relational tables

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
- be exported back to nested JSON-like rows

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

If you want more control, the same flow is also available as separate commands:

- `exasol-json-tables ingest`
- `exasol-json-tables wrap generate`
- `exasol-json-tables wrap install`
- `exasol-json-tables wrap deploy`
- `exasol-json-tables validate`
- `exasol-json-tables structured-results ...`

For automation and autonomous agents, the major workflow commands also support `--json`. In that mode they emit a machine-readable summary on stdout with the important outputs, such as package paths, schema names, activation SQL, smoke-test SQL, and wrapper-scope warnings. `structured-results preview-json` already returns JSON rows directly.

## Further Reading

- Installation: [docs/installation.md](docs/installation.md)
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

# Ingest

The ingest stage turns raw JSON into the relational contract that the rest of Exasol JSON Tables builds on.

That contract is designed so nested JSON remains queryable and reusable inside Exasol instead of being flattened into text blobs or unpacked into ad hoc tables.

## What Ingest Produces

The ingest engine can:

- scan a JSON array of objects or NDJSON
- infer a family of relational tables for nested objects and arrays
- write Parquet staging files for that family
- emit a source-manifest JSON artifact for downstream wrapper generation
- optionally upload the data directly into Exasol
- optionally emit Exasol SQL DDL

The emitted layout is the shared table contract used throughout the project:

- explicit-null masks such as `<name>|n`
- nested object references such as `<name>|object`
- nested array sizes such as `<name>|array`
- array child tables with `_parent` and `_pos`
- identifiers such as `_id` for root/object tables and some nested rows

## The Main Ways To Run It

For most users, the main entrypoint is the installed CLI:

```bash
exasol-json-tables ingest ...
```

The one-shot end-to-end flow is:

```bash
exasol-json-tables ingest-and-wrap ...
```

That path is especially useful when you want the CLI to derive the source, wrapper, helper, and preprocessor names for you and place the generated artifacts in a per-run subdirectory.

If you want to work directly with the Rust engine, you can still run it with Cargo:

```bash
cargo run --manifest-path crates/json_tables_ingest/Cargo.toml -- ...
```

## Typical Workflows

### Generate Local Parquet Output

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables
```

### Emit SQL DDL

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables \
  --schema-sql
```

### Emit A Source Manifest

The unified CLI emits a source manifest by default. If you are calling the Rust crate directly, you can control it explicitly:

```bash
cargo run --manifest-path crates/json_tables_ingest/Cargo.toml -- \
  --input ./data.json \
  --manifest-output ./out/data.source_manifest.json
```

That manifest is useful because the wrapper layer can consume it directly instead of re-introspecting the live source schema.

### Upload Directly Into Exasol

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol exasol://sys:exasol@127.0.0.1:8563/JVS_SRC
```

When you use `--exasol`, the CLI creates the target source schema first if it does not already exist. That makes direct ingest behave like the one-shot workflow instead of failing late after local scan and staging work.

If you want to stage via a temp directory and clean it up afterward:

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol exasol://sys:exasol@127.0.0.1:8563/JVS_SRC \
  --exasol-temp-dir /tmp/json_tables_ingest \
  --exasol-cleanup
```

If you use `ingest-and-wrap` instead, you can also drive the connection through `--dsn`, `--user`, and `--password` without constructing the ingest URL yourself.

The same rule applies there too: the derived or explicit source schema is created automatically before ingest starts.

## Input Shape

Supported input formats:

- a JSON array of objects
- NDJSON, one object per line

The ingest engine auto-detects the format from the first non-whitespace character.

## Example Resulting Shape

Given input like:

```json
[
  {"id": 1, "name": "Cafe", "hours": {"mon": "9-5"}, "tags": ["coffee", "wifi"]},
  {"id": 2, "name": "Diner", "hours": {"mon": null}, "tags": []}
]
```

The ingest engine will typically produce:

- a root table such as `data`
- an object child table such as `data_hours`
- an array child table such as `data_tags_arr`

And the root rows will carry structural link columns such as:

- `hours|object`
- `tags|array`

while the nested content itself lives in the child tables.

## Why The Contract Looks Like This

The table family is designed to preserve JSON structure without collapsing everything to strings:

- one stable scalar type becomes one normal column
- mixed scalar types become sibling variant columns such as `value` and `value|string`
- explicit JSON `null` sets a mask column such as `value|n`
- object values become links through `<name>|object`
- array values become links through `<name>|array` plus an array child table

That contract is what powers the later stages:

- the query layer can distinguish missing from explicit `null`
- deep object traversal can be rewritten into joins
- arrays can be addressed by position or expanded into rows
- structured results can reuse the same contract on output, and wrapped families can emit final JSON through `TO_JSON(...)`

## Next Step After Ingest

Once the source schema exists, install the wrapper surface on top of it:

- generate the wrapper package
- install it into wrapper/helper/preprocessor schemas
- activate the SQL preprocessor in the session where you want JSON-friendly SQL

For the short path, use `exasol-json-tables ingest-and-wrap`. For the lower-level path, see [query-surface.md](query-surface.md) and [installation.md](installation.md).

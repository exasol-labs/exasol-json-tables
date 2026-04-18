# json_to_parquet

`json_to_parquet` is the current Rust ingest engine inside **Exasol JSON Tables**.

Command-line tool for importing semi-structured JSON into Exasol.

## Background

`json_to_parquet` is meant for messy, semi-structured JSON that still needs to end up in
Exasol for analytics or warehousing. Instead of forcing everything into a single, wide table,
the tool walks the JSON tree and emits a small set of related tables:

- The root object becomes the root table.
- Nested objects become child tables with foreign keys back to their parent rows.
- Arrays become child tables with `_parent` and `_pos` columns so element order is preserved.

As it scans, the tool gathers type statistics, infers a schema, writes Parquet staging files
for each table path, and can then import those files directly into Exasol. Simple values become
columns; missing values remain null; explicit nulls get a boolean mask column (`<name>|n`) so
downstream systems can tell "present-but-null" apart from "missing". Optional SQL DDL can also
be generated to mirror the imported layout.

The design goal is to keep the data model familiar (tables and keys) while preserving the
shape and ordering of the original JSON, making it easier to query nested structures in SQL
without custom JSON parsing logic.

## Status

Implemented:
- Scan JSON arrays of objects or newline-delimited JSON (NDJSON).
- Infer a relational table layout for nested objects and arrays.
- Write one or more Parquet staging files from the inferred layout.
- Import those Parquet files directly into Exasol, including generated tables and constraints metadata.

## Future Roadmap

- Import multiple JSON files in one go - ideally defined with glob patterns.
- Import JSON files directly from S3.
- Write Parquet output files directly to S3.
- Import based on an existing schema - possibly extending with new columns and subtables.
- Add option to designate an exsting value in the documents as primary key.

## Invocation

The input can be either a JSON array of objects or NDJSON (one object per line). The tool
auto-detects the format based on the first non-whitespace character.

```bash
# Primary workflow: import directly into Exasol
json_to_parquet --input data.json --exasol "exasol://user:pass@host:8563/schema"

# Import via temp staging files and clean them up after upload
json_to_parquet --input data.json --exasol "exasol://user:pass@host:8563/schema" --exasol-temp-dir /tmp/json_to_parquet --exasol-cleanup

# Emit an Exasol SQL DDL file alongside the generated Parquet outputs
json_to_parquet --input data.json --schema-sql

# Only generate Parquet files locally
json_to_parquet --input data.json --output-dir ./out
```

## Exasol Import

When `--exasol` is provided, the tool uploads the generated Parquet files to Exasol using the
`exarrow-rs` driver. Tables are created automatically from Parquet metadata, and uploads run
in parallel using multiple HTTP connections for higher throughput.

The generated `PRIMARY KEY` and `FOREIGN KEY` constraints are emitted as `DISABLE` in Exasol.
This keeps them as relationship metadata instead of relying on the session's
`CONSTRAINT_STATE_DEFAULT`, which can otherwise make enforcement vary by environment.

### Exasol Flags

- `--exasol "<url>"`: Exasol connection URL (format: `exasol://user:pass@host:port/schema`).
- `--exasol-temp-dir <dir>`: Write intermediate Parquet files to a specific directory.
- `--exasol-cleanup`: Remove intermediate Parquet files after a successful upload.

### Example

Input (`data.json`):

```json
[
  {"id": 1, "name": "Cafe", "hours": {"mon": "9-5"}, "tags": ["coffee", "wifi"]},
  {"id": 2, "name": "Diner", "hours": {"mon": null}, "tags": []}
]
```

Typical Exasol result:

- Root table: `data`
- Child object table: `data_hours`
- Child array table: `data_tags_arr`

Corresponding staging files:

- `data.parquet` (root table: `id`, `name`, `hours|object`, `tags|array`)
- `data.hours.parquet` (child table: `_id`, `mon`, `mon|n`)
- `data.tags[].parquet` (array table: `_parent`, `_pos`, `_value`)

## Development

The command-line app is build in Rust, so you will need the [rust toolset](https://www.rust-lang.org/tools/install) installed.

```bash
cargo build
cargo test

# Optional Exasol-backed end-to-end tests
# Defaults to local ExaNano at exasol://sys:exasol@127.0.0.1:8563
cargo test exasol_e2e -- --ignored

# Override the target Exasol instance if needed
JSON_TO_PARQUET_EXASOL_BASE_URL="exasol://user:pass@host:8563?tls=1&validateservercertificate=0" \
  cargo test exasol_e2e -- --ignored

# Example invocation
cargo run -- --input data.json --schema-sql
```

Running the current binary will print a summary of observed property/type combinations in the input JSON file.

## Encoding Details

JSON properties are not always type-stable across rows, so the tool uses a small encoding scheme
to preserve mixed variants without collapsing everything to text:

- If a property has one scalar type, it becomes a normal column such as `name` or `score`.
- If a property has multiple scalar types, the most common non-null type becomes the main column
  and the other scalar variants get alternate columns named `<name>|<type>`.
- Integers and floating-point numbers are treated as one numeric family, so mixed integer/number
  values are stored in a single numeric column.
- Explicit JSON `null` values set a boolean mask column named `<name>|n`.
- Object-valued properties are represented in the parent row by `<name>|object`, which stores the
  referenced child row id.
- Array-valued properties are represented in the parent row by `<name>|array`, which stores the
  array length, while the elements themselves go into a child array table.

For example, if `value` is sometimes a string and sometimes a number, the resulting layout might
contain `value` plus `value|string`. If `child` is sometimes an object and sometimes `null`, the
parent table will contain `child|object` and `child|n`, while the actual object fields live in
the child table.

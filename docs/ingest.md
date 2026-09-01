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

When ingest writes copied tables into Exasol, it also stamps every table with a
`COPY provenance {...}` comment. `SYS.EXA_ALL_TABLES.TABLE_COMMENT` therefore
exposes the source file, the `local-file` source connection kind, the import
timestamp, the source file modification timestamp when available, the JSON
path represented by each generated table, and the contract version. The comment
intentionally excludes Exasol connection credentials.

```json
{"source":"/imports/orders.json","sourceConnection":"local-file",
 "importedAt":"2026-09-01T10:00:00Z","tablePath":"root",
 "tool":"exasol-json-tables","contractVersion":1,
 "sourceModifiedAt":"2026-09-01T09:40:00Z"}
```

`contractVersion` is the version of the shared table contract — the `|` column
grammar and the structural columns — that wrote the family. A consumer that
parses column names should check it and refuse a version it does not know rather
than misread the encoding; the value is bumped whenever a marker, separator or
structural column changes meaning. See
[The Shared Table Contract](architecture.md#the-shared-table-contract).

When a wrapper package is generated from that source manifest, the root source
table's comment is copied onto the public wrapper view. Consumers can therefore
read the same provenance and freshness timestamps from
`SYS.EXA_ALL_VIEWS.VIEW_COMMENT` without knowing about the internal `_SRC`
schema.

That manifest is useful because the wrapper layer can consume it directly instead of re-introspecting the live source schema.

### Upload Directly Into Exasol

```bash
exasol-json-tables ingest \
  --input ./data.json \
  --artifact-dir ./dist/exasol-json-tables \
  --exasol exasol://sys:exasol@127.0.0.1:8563/JVS_SRC
```

When you use `--exasol`, the CLI creates the target source schema first if it does not already exist. That makes direct ingest behave like the one-shot workflow instead of failing late after local scan and staging work.

The Exasol control connection TLS settings live in the `--exasol` URL. The bulk import HTTP transport has a separate TLS switch. Keep it disabled for a local Exasol Personal or Docker deployment, and add `--exasol-http-tls` for production or SaaS targets that expect TLS on the data channel.

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

## In-Database Ingest (Rust UDF)

Ingest can also run **inside** Exasol, as a Rust UDF, with no client in the data
path. That path lives in [crates/json_tables_udf](../crates/json_tables_udf) and
shares its normalisation with the CLI, so both produce the same contract.

Install once (see the crate README for the build and BucketFS upload):

```sql
-- crates/json_tables_udf/sql/install.sql
CREATE OR REPLACE CONNECTION JSON_TABLES_SELF TO '127.0.0.1:8563' USER 'sys' IDENTIFIED BY '...';
CREATE OR REPLACE RUST SCALAR SCRIPT JSON_TABLES.INGEST_JSON(...) EMITS (...) AS
%connection JSON_TABLES_SELF;
%udf_object /buckets/bfsdefault/rust/libjson_tables_udf.so;
/
```

Then ingest is one statement, and its result is the run report:

```sql
SELECT JSON_TABLES.INGEST_JSON('bfs:/buckets/bfsdefault/rust/orders.ndjson',
                               'EJT_ORDERS_SRC', 'JSON_TABLES_SELF',
                               '{"replace": true}');

TABLE_NAME             ROWS_LOADED  STATUS
orders                      100000  loaded
orders_flags                100000  loaded
orders_items_arr            300000  loaded
orders_events_arr           200000  loaded
orders_tags_arr             200000  loaded
```

### Sources

- `bfs:/buckets/...` — a file on the BucketFS mount.
- `table://SCHEMA.TABLE[.COLUMN]` — JSON text already in the database, one
  document (or one chunk) per row. The column defaults to `DOC`. Identifiers are matched exactly, so
  a family created by ingest — whose table names are lower case — needs
  `table://SCHEMA.orders.PAYLOAD`, not `table://SCHEMA.ORDERS.PAYLOAD`.
- `exatunnel://host:port` — **a file on the client's machine**, streamed through
  Exasol's bulk tunnel. The client opens the tunnel and passes the address; the
  UDF reads it.
- `http://host:port/path` — any internal HTTP source.
- `s3://bucket/key` or `https://host/object` — fetched by the **database** using
  the named `CONNECTION`, so credentials stay in Exasol.

### Reviewing the plan before loading

```sql
SELECT JSON_TABLES.PLAN_JSON('bfs:/buckets/bfsdefault/rust/orders.ndjson', NULL, NULL);
-- emits (plan, ddl): the inferred plan, and the DDL the driver would run
```

### What to expect

On a small single-node deployment the UDF is **slower** than the CLI for the same
file — it parses the source once per target table, so a five-table family reads it
six times (measured 5.8s against the CLI's 4.8s for 100k documents). Its value is
not speed on one node; it is that ingest becomes a database operation:
schedulable, grantable, with no client in the data path, and parsing that scales
with the cluster rather than one client process.

### Wrapping a UDF-loaded family

Nothing changes. The family is contract-identical to a CLI-loaded one, so the
wrapper package is generated and installed exactly as above:

```bash
exasol-json-tables wrap generate --source-schema EJT_ORDERS_SRC \
  --wrapper-schema EJT_ORDERS_VIEW --no-auto-source-manifest ...
exasol-json-tables wrap install --package-config .../orders_package.json
```

## Next Step After Ingest

Once the source schema exists, install the wrapper surface on top of it:

- generate the wrapper package
- install it into wrapper/helper/preprocessor schemas
- activate the SQL preprocessor in the session where you want JSON-friendly SQL

For the short path, use `exasol-json-tables ingest-and-wrap`. For the lower-level path, see [query-surface.md](query-surface.md) and [installation.md](installation.md).

# json_tables_udf

The **in-database** ingest front end for Exasol JSON Tables: a Rust UDF that
turns JSON into the shared table contract with no client in the data path.

Normalisation is [`json_tables_core`](../json_tables_core) — the same code the
[`json_to_parquet`](../json_tables_ingest) CLI uses. This crate adds the sources,
the emit path, and the SQL orchestration.

```sql
SELECT JSON_TABLES.INGEST_JSON('s3://acme-lake/orders/*.ndjson',
                               'EJT_ORDERS_SRC', 'JSON_TABLES_SELF', NULL);

TABLE_NAME             ROWS_LOADED  STATUS
orders                      100000  loaded
orders_flags                100000  loaded
orders_items_arr            300000  loaded
```

## The three scripts

| Script | Purpose |
|---|---|
| `INGEST_JSON(src, target_schema, conn_name, opts)` | One call: infer the family, create the tables, load them, stamp provenance. Emits one report row per table. |
| `PLAN_JSON(src, conn_name, opts)` | Pass 1 only. Emits `(plan, ddl)` for review — nothing is created or loaded. |
| `LOAD_TABLE (...) EMITS (...)` | Emits the rows of **one** table. The driver calls it once per table; usable directly. |

## How a load moves

`INGEST_JSON` opens an ordinary SQL session (connect-back) and uses it purely as
a **control** channel — `CREATE SCHEMA`, `CREATE TABLE`, one
`INSERT INTO … SELECT LOAD_TABLE(…)` per table, the constraint DDL, and the
`COMMENT` provenance. The rows themselves never cross that session: each `INSERT`
runs inside the engine and the loader emits into it directly.

That split matters because the SDK's connect-back API has no bulk path — only
`execute`. Pushing 900k rows through SQL text measured ~9k rows/s in this
database; emitting them measured ~3M rows/s.

```
INGEST_JSON  ──connect-back──>  CREATE TABLE …            (control)
             ──connect-back──>  INSERT INTO t SELECT LOAD_TABLE(…)
                                        │
                                        └── engine runs LOAD_TABLE, rows emit
                                            straight into t          (data)
```

## Sources

| Locator | Read by | Notes |
|---|---|---|
| `bfs:/buckets/…` (or `/buckets/…`) | the UDF, from the BucketFS mount | Re-readable |
| `table://SCHEMA.TABLE[.COLUMN]` | the UDF, over connect-back | Re-readable; one document or one text chunk per row |
| `exatunnel://host:port` | the UDF, over plain HTTP | **A file on the client's machine**, streamed through Exasol's bulk tunnel |
| `http://host:port/path` | the UDF, over plain HTTP | Any internal HTTP source |
| `s3://bucket/key`, `https://host/object` | the **database**, via `IMPORT … AT` | Credentials come from the named `CONNECTION`; no signing code here |

A source that can only be read once (a tunnel, a cloud object) is materialised
into a landing table first, because the loader needs the source again for every
table. Text streams are chunked into `VARCHAR` rows — a 50 MB source is ~50 rows,
so even the slow SQL-text path costs milliseconds.

## Why identities agree across statements

Each table is loaded by its own `INSERT`, yet a root row's `hours|object` must
equal the `_id` the `hours` table gave that child. The traversal assigns
identities per table in document order, so the loader keeps a counter for **every**
table in the plan even though it emits one. Pass *n* and pass *m* therefore agree
by construction, with no shared state between them.

The plan is inferred once by the driver and passed to each loader as JSON
([`plan_wire`](src/plan_wire.rs)), so no pass re-infers and none can drift.

## Two engine rules worth knowing

Both were learned the hard way and are encoded in the code and its tests:

- **The loader must be declared with dynamic parameters**, `(...) EMITS (...)`.
  A static `EMITS` clause makes the engine reject a per-call column list:
  *"The script has a static return argument definition."*
- **The `INSERT` form must not carry an `EMITS` clause.** Exasol infers an
  emitting function's columns from the target table: *"The return arguments for
  EMITS functions are inferred from the table to insert into."*

## Build and deploy

The artifact is a Linux `.so` whose SDK version and rustc must match the
installed language container's fingerprint — a mismatch fails at load with
`F-UDF-CL-RUST-9001: Fingerprint mismatch`. Check the container's expectation by
running any script and reading the error.

```bash
# Build in a Linux image whose glibc is at or below the container's floor.
docker run --rm -v "$PWD:/build" -w /build/crates/json_tables_udf \
  rust:1.94.1-bookworm bash -c 'cargo build --release'

# Upload to BucketFS (Exasol Personal publishes no BucketFS endpoint, so copy
# into the deployment VM; a normal cluster uses the BucketFS HTTP API).
D=~/.exasol/personal/deployments/default
scp -i $D/local/node_access.pem -P "$(jq -r .connection.sshPort $D/deployment.json)" \
  target/release/libjson_tables_udf.so \
  root@127.0.0.1:/var/lib/exa/bucketfs/bfsdefault/rust/

# Register the connection and the three scripts.
exasol connect -f sql/install.sql
```

## Tests

```bash
cargo test --manifest-path crates/json_tables_udf/Cargo.toml
```

The host suite covers source parsing, the plan wire format, the emit sink
(including cross-pass identity agreement) and the driver's whole SQL sequence
against a fake connection. What it cannot cover — the language container, the
engine's EMITS rules, connect-back — is exercised by running the scripts against
a real database; see [docs/ingest.md](../../docs/ingest.md).

## Known limits

- **One pass per target table.** A five-table family parses the source six times
  (one plan pass, five loads). On a small single-node deployment that makes the
  UDF *slower* than the CLI; the fix is a single-pass load into a union staging
  table, then an engine-side split.
- **The whole source is read into memory** by the UDF for each pass. Bounded by
  the UDF's memory limit (4 GiB on the tested deployment).
- **No `https://` fetch inside the UDF.** Cloud objects are read by the engine
  instead, which owns credentials and TLS.
- **Ingest is not atomic.** The connect-back session autocommits, so a failure
  part-way leaves a partly-loaded family — the same guarantee the CLI has.

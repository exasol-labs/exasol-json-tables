# Changelog

Changes accumulate under `Unreleased` until the next formal version cut.

The format is loosely based on Keep a Changelog and focuses on user-visible behavior, migration-relevant changes, and operational fixes.

## [Unreleased]

## [0.3] - 2026-09-02

### Added

- Added in-database ingest: [crates/json_tables_udf](crates/json_tables_udf), a
  Rust UDF that loads JSON into the table contract with no client in the data
  path. `SELECT JSON_TABLES.INGEST_JSON(source, target_schema, connection, options)`
  infers the family, creates the tables, loads them and stamps provenance in one
  statement, emitting one report row per table. `PLAN_JSON` returns the plan and
  DDL for review; `LOAD_TABLE` emits one table's rows and is usable directly.

  Sources: a BucketFS file, JSON already held in a table, an HTTP stream —
  including Exasol's bulk tunnel, which streams a file from the client's own
  machine — and cloud storage (`s3://`, `https://`) fetched by the database itself
  through a named `CONNECTION`, so object-store credentials never enter the UDF.

  The loader shares its normalisation with the CLI: a family loaded by the UDF is
  identical to a CLI-loaded one down to the row, and the existing wrapper package
  installs over it unchanged. On a single-node deployment it is *slower* than the
  CLI, because it parses the source once per target table; its purpose is making
  ingest a database operation rather than a client one.

- Added zero-argument `TO_JSON()` for MongoDB Virtual Schema roots and their
  generated wrapper views. It returns the connector's complete canonical
  Extended JSON source document, including fields outside the inferred schema,
  while `TO_JSON(*)` and subset forms retain their existing reconstruction
  semantics for modeled and derived results.
- Added support for the MongoDB connector's `sourceDocumentColumn` manifest
  extension during offline wrapper generation.

- Added `COMPILE_SQL`, a preprocessor-free route to the same rewrite. The JSON Tables
  rewrite is a pure function of the statement text and the package metadata, so it is now
  also reachable as an ordinary script:
  `EXECUTE SCRIPT JVS_COMPILE.COMPILE_SQL('SELECT "meta.info.note" FROM JSON_VIEW.SAMPLE')`
  returns one row carrying `STATUS`, `ERROR_CODE`, `ERROR_MESSAGE`, `ORIGINAL_SQL`,
  `GENERATED_SQL`, `PLAN_JSON` and `CLARIFICATION_JSON`. The returned SQL runs in any
  session: no `ALTER SESSION`, nothing to activate, and no single-preprocessor slot to
  contend for. `exasol-json-tables compile install` and `exasol-json-tables compile run`
  install and exercise it; with no `--wrapper-schema`, `compile install` builds one entry
  point serving every installed package, so a single statement may join two of them. The
  package metadata is baked into the script, so re-run `compile install` after installing
  or regenerating a package.

  It is a rewrite, not a validation: a `STATUS` of `OK` still leaves ordinary SQL errors —
  a mistyped table, a type mismatch — to the statement itself, and `PLAN_JSON` names the
  packages a statement reached as provenance for a reader, not as an access decision.

- Added `contractVersion` to the `COPY provenance {...}` comment, from `CONTRACT_VERSION`
  in [crates/json_tables_core/src/contract.rs](crates/json_tables_core/src/contract.rs).
  Consumers outside this repository parse the `|` column grammar themselves, so a family
  now records which version of that grammar wrote it and a downstream reader can refuse a
  version it does not know instead of silently misreading it. The value is bumped whenever
  a marker, separator or structural column changes meaning; purely additive changes that
  leave existing names meaning what they meant do not bump it.

- Added provenance stamping to every route that produces a family, not only direct import.
  The comments are derived once per run and now travel on the `--schema-sql` DDL (as a
  `-- Provenance` section after the creates), on `tableComment` in the `--manifest-output`
  source manifest, and on materialized result families (with
  `"sourceConnection":"result-family"` and a `table://` or `query://` source). Because the
  manifest carries the comment, a wrapper package generated from a purely local run stamps
  its public view too. `PLAN_JSON` is the deliberate exception — its DDL is a review
  artifact with no load to timestamp — and `LOCAL TEMPORARY` families cannot carry a
  comment. Keep the `-- Provenance` statements with the `CREATE TABLE` statements when
  applying DDL by hand: dropping them is what produces a family the catalog cannot
  describe.

- Added prebuilt release artifacts, built by
  [.github/workflows/release.yml](.github/workflows/release.yml) when a `v*` tag is pushed.
  A release now carries the `json_to_parquet` ingest CLI for Linux x86_64 and macOS arm64,
  the `json_tables_udf` in-database loader (`libjson_tables_udf.so`, built in the Debian
  release the script language container stages its runtime from and packaged with its
  `install.sql`), and the Python sdist and wheel — each with a SHA-256 checksum.

### Fixed

- Fixed the diagnostics for a `table://` source in the in-database loader (BUG-135). A table source is
  now checked against the catalog before it is read, so the six ways a locator can be wrong each name
  the source and the fix instead of surfacing as `Protocol error: object DOC not found` from the
  loader's own `SELECT`: a missing table, a missing column (with the columns that do exist listed), a
  case mismatch on the table or on the column (with the actual name suggested), and a column that is
  not `CHAR`/`VARCHAR`. The column still defaults to `DOC`, and that default is now documented where
  the locator is.

  Case mismatches matter more here than they look: ingest creates lower-case table names, and Exasol
  folds unquoted identifiers to upper case.

- Fixed non-reproducible generated DDL (BUG-134). `--schema-sql` emitted the object-link
  `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` statements in whatever order the process's hash seed
  produced, so the same input yielded a differently-ordered file on every run — ten runs of one binary
  gave ten distinct checksums on a 19-table family. The schema was never wrong, but the artefact could
  not be checksummed or diffed, which matters because generated DDL is what a DBA reviews and what CI
  compares against a stored copy. Foreign keys are now emitted in constraint-name order.

  The same root cause made the CLI's progress output unstable: staging files were written in
  `HashMap` order, so the `Wrote Parquet file for table …` lines varied per run. They are now written
  in table-path order. The Parquet files themselves, the source manifest, and the `CREATE TABLE`
  order were already stable and are unchanged.

- Fixed `VALUE` iteration over an array of objects. `VALUE` binds an array's scalar
  element, which only exists where the array child table carries the contract's `_value`
  column; over an array of objects the generated SQL asked for a `_value` that does not
  exist and the database answered with an internal alias. It is now refused by name with a
  `JVS-ITER-ERROR` that says the array is an array of objects and shows the row-iterator
  form to use instead. A property that happens to be called `value` is an ordinary
  property — the physical column decides — and the value-object shape, where an element
  table has `_value` alongside its own columns, still supports both forms.

- Fixed the Python package's `__version__`, which still reported `0.1.0` after the 0.2 cut.

### Changed

- Made **Exasol Personal** the documented default deployment target and removed the
  ExaNano-era references. The fixture-helper module is renamed
  `nano_support` → `personal_support` (with the `tools/` compatibility wrapper renamed to match), and
  `tools/test_nano_preprocessor_parser_lane.py` is now
  `tools/test_preprocessor_parser_lane.py`. [docs/installation.md](docs/installation.md) and
  [docs/testing.md](docs/testing.md) now describe `exasol install local`, the TLS requirement on the
  control connection, and how that relates to the separate `--exasol-http-tls` bulk-import switch.

  Dated reports under `plans/` and `docs/user-studies/` still say ExaNano where that is what the
  measurements ran against; those are historical records and were left as written.

- Split the Rust ingest engine into two crates. The normalisation core — the
  table contract, schema inference, the document traversal, DDL generation and
  the source manifest — now lives in
  [crates/json_tables_core](crates/json_tables_core), with no file, Parquet or
  driver dependencies. [crates/json_tables_ingest](crates/json_tables_ingest)
  keeps the `json_to_parquet` CLI: reading local files, staging Parquet, and
  importing over `exarrow-rs`.

  The core reads from any `BufRead` and writes through a `RowSink` trait, so the
  same normalisation can back an in-database loader that streams its source and
  emits rows without buffering the family. Provenance is no longer hard-coded to
  `local-file`; a caller supplies its own source kind.

  No user-visible behavior changes: the CLI's flags, generated Parquet, DDL,
  manifest and provenance comments are byte-identical, and the manifest still
  reports `"generator": "json_to_parquet"`.

- Upgraded the in-database loader from `exasol-udf-sdk` / `exasol-udf-macros` `0.21.3` to
  `0.23.0`, which exposes connect-back unconditionally. The documented build image moves
  from `rust:1.94.1-bookworm` to `rust:1.94.1-trixie` — the Debian release the script
  language container stages its runtime from, glibc floor 2.41 — so the artifact links
  against the glibc it will load against.

## [0.2] - 2026-08-19

### Added

- Added automatic catalog provenance for copied JSON sources. Exasol ingest now
  stamps each generated local table with its source file, source connection
  kind, import timestamp, source modification timestamp, and JSON table path in
  `EXA_ALL_TABLES.TABLE_COMMENT`.
- Added a flat query-path discovery surface to `describe package` and
  `describe wrapper`. JSON output now includes `description.querySurface` with
  roots, canonical paths, JSON types, example expressions, and array iterator
  syntax; plain-text output prints the same paths and preprocessor activation SQL.
- Added `ingest-and-wrap --if-exists {fail,replace,skip}` so automated retries can
  explicitly reject, rebuild, or leave an existing four-schema workflow unchanged.
- Added `--exasol-http-tls` to the Rust ingest CLI and unified `exasol-json-tables ingest` / `ingest-and-wrap` workflows. This enables TLS for the Exasol HTTP bulk-import transport independently from the control connection TLS settings in the `--exasol` URL.
- Added `httpTransportTls` to ingest JSON summaries when direct Exasol import is used.

### Changed

- Upgraded the Rust ingest engine from `exarrow-rs` `0.9.0` to `0.16.0`.
- Direct Exasol ingest can now benefit from `exarrow-rs` native Parquet import automatically on Exasol 2025.1.11 and newer, while older Exasol versions continue to use the existing CSV conversion path.
- Direct Exasol ingest now relies on `exarrow-rs` automatic schema activation during `connect()`, avoiding the previous explicit `set_schema()` round trip on every ingest connection.
- Updated ingest documentation to distinguish Exasol control-connection TLS from the HTTP transport TLS used for bulk imports.

### Fixed

- Propagated copied-source provenance from ingest manifests and source-table
  comments to public wrapper-view comments, making freshness visible in
  `EXA_ALL_VIEWS.VIEW_COMMENT` on the surface consumers actually query.
- Fixed `describe ... querySurface` examples for object-valued paths. Their
  `exampleExpression` now uses runnable `TO_JSON(...)` syntax for top-level
  objects and `JSON_TYPEOF(...)` for dotted nested objects, instead of a bare
  object reference that Exasol rejects as an unknown column.
- Fixed `JSON_IS_EXPLICIT_NULL(...)` / `JNULL(...)` for fields without an
  explicit-null (`|n`) branch. These calls now rewrite directly to `FALSE`
  without creating a hidden helper projection join.
- Fixed combined-preprocessor JSON helpers on later sources in a cross-schema
  join. Hidden helper projection joins are now inserted after the wrapper alias
  they reference instead of after the query's first source.
- Documented and regression-tested combined preprocessors spanning several
  independently generated wrapper schemas, enabling JSON path syntax on both
  sides of a cross-collection join in one Exasol session. Repeated
  `wrap generate --source-schema` values now fail explicitly instead of silently
  replacing one another.
- Fixed wrapper generation for optional string fields encoded with `|empty`
  masks. Masks are no longer coalesced into logical values as `TRUE`/`FALSE`;
  `TO_JSON(...)` now reconstructs empty strings, preserves explicit `null`,
  omits missing properties, and emits each property key only once.

## [0.1] - 2026-04-23

### Added

- Added a user-facing changelog so downstream users can track notable changes between releases.
- Added `publicViews` to the machine-readable wrapper workflow surface:
  - `ingest-and-wrap --json`
  - `wrap generate --json`
  - `wrap install --json`
  - `wrap deploy --json`
  - `validate --json`
  - `describe wrapper --json`
  - `describe wrappers --json`
- Added recursive wrapper discovery to `describe package --json` and `describe wrapper --json`:
  - per-root `fieldTree` data for nested object and object-array branches
  - per-root `familyTables` entries that map child helper tables back to paths such as `meta.info` or `items[]`
- Added support for `TO_JSON(item.*)` on object-array iterator rows, so joined array items can now be serialized directly from the wrapper surface.
- Added broader iterator-row `TO_JSON(...)` coverage for wrapped array items, including:
  - `TO_JSON(item.*)`
  - subset forms such as `TO_JSON(item."sku", item."name", ...)`
  - use inside CTEs on expanded array rows
- Added [docs/identifier-conventions.md](docs/identifier-conventions.md) and aligned the agent skills with explicit guidance for quoted wrapper references, uppercase durable aliases, and reserved-word avoidance.

### Changed

- Hidden JSON export views are now generated for the full table family, not just the public roots. This expands the internal export surface used by `TO_JSON(...)` while keeping the user-facing contract unchanged.
- `structured-results preview-json` now uses the same temporary wrapper plus `TO_JSON(*)` outlet as the installed SQL surface.
- The old product-side Python JSON exporter surface was retired in favor of the SQL-native `TO_JSON(...)` path.

### Fixed

- Fixed wrapper/export generation for documents where array items contain nested object fields. `ingest-and-wrap` now succeeds for shapes such as `reviews[].date`.
- Fixed Python-side CLI flag consistency so commands like `validate`, `wrap install`, `wrap deploy`, and `describe ...` accept `--no-tls` alongside the ingest workflows.
- Fixed `describe wrapper --json` to expose `nextActions.activationSql` like `describe package --json`.
- Fixed `describe wrappers --json` so each wrapper entry includes top-level `wrapperSchema`, `helperSchema`, `sourceSchema`, and `publicViews`.
- Fixed wrapper workflow visibility around actual public view names. `--name` still controls derived schema/package names, and the actual public views are now surfaced explicitly in JSON responses and documented in the user docs.
- Fixed the installed-package hidden export surface so validation and helper-object expectations include all required export views.
- Fixed a shape-dependent iterator-row `TO_JSON(...)` failure on wrapped object arrays such as `orders.items`, where queries like `TO_JSON(i.*)` or `TO_JSON(i."sku", i."name")` could fail or return incomplete results.
- Fixed iterator-row `TO_JSON(...)` behavior for wrapped child tables that are keyed by multiple structural columns, so child export joins now line up with the full table-family contract instead of relying on a simplified row-key heuristic.
- Fixed a related preprocessor rewrite issue where generated iterator/derived sources could lose the correct join insertion point during later rewrite stages. This hardens both iterator-row `TO_JSON(...)` and qualified iterator-path rewrites.
- Fixed opaque nested-path errors so missing fields and invalid scalar/object bracket traversal now fail with `JVS-PATH-ERROR` guidance instead of leaking internal rewrite aliases.

### Migration Notes

- If you previously depended on the removed Python exporter helpers such as `export_root_family_to_json`, move to one of these supported paths:
  - final output from installed wrappers: query `TO_JSON(*)` or `TO_JSON(col1, col2, ...)`
  - one-shot preview from structured-results configs: `exasol-json-tables structured-results preview-json`
- If you automate wrapper discovery, prefer the stable top-level JSON fields:
  - `objects.publicViews`
  - `nextActions.activationSql`
  - `wrappers[].wrapperSchema`
  - `wrappers[].publicViews`

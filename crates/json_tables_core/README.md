# json_tables_core

The normalisation core of **Exasol JSON Tables**: JSON documents in, relational
table-family rows out.

This crate holds the parts of ingest that have no opinion about I/O. It reads
from any `BufRead` and writes through a `RowSink` trait, so the same code backs
both front ends:

- [`json_tables_ingest`](../json_tables_ingest) — the `json_to_parquet` CLI, which
  sinks into column buffers and stages Parquet files;
- an in-database loader — which streams its source and flushes rows as it goes.

## What lives here

| Module | Responsibility |
|---|---|
| `contract` | The shared table contract: `TablePath`, `SimpleType`, `ColumnPlan`, `PlannedTable`, and the identifier/naming rules |
| `infer` | Pass 1 — `StatsCollector` observes documents; `build_all_schema_plans` derives the family |
| `read` | Framing a byte stream into documents (JSON array or NDJSON), detected from the first non-whitespace byte |
| `sink` | Pass 2 — the traversal, which turns documents into rows and hands them to a `RowSink` |
| `buffer` | `ColumnBuffers`, the in-memory columnar sink the CLI stages Parquet from |
| `ddl` | `CREATE TABLE` plus disabled PK/FK constraints for a planned family |
| `manifest` | The source manifest, and the provenance comments stamped onto each table |

## The two passes

The schema has to be known before any row can be written, so the input is read
twice: once to observe, once to write.

```rust
use json_tables_core::{
    buffer::ColumnBuffers,
    infer::{build_all_schema_plans, StatsCollector},
    read::{detect_format, for_each_document},
    sink::write_document,
};

// Pass 1 — observe the documents and derive the family.
let mut reader = open_source();
let format = detect_format(&mut reader)?;
let mut stats = StatsCollector::new();
for_each_document(reader, format, |_, document| {
    stats.record_document(document);
    Ok(())
})?;
let plans = build_all_schema_plans(&stats.finish());

// Pass 2 — write the rows into any sink.
let mut sink = ColumnBuffers::new(&plans);
let mut reader = open_source();
let format = detect_format(&mut reader)?;
for_each_document(reader, format, |_, document| write_document(&mut sink, document))?;
```

A caller whose source cannot be re-read must buffer it or infer from a sample.

## Implementing a sink

`RowSink` receives the rows the traversal produces. The contract that makes
streaming possible:

> Writes always target the **most recently started row** for a given table path.

A sink may therefore keep one open row per table and flush it when the next row
for that table starts; it never has to retain earlier rows. A parent row does
stay open while its children are written, because the traversal sets the parent's
object-link column after descending — so the number of simultaneously open rows
is bounded by the depth of the family, not by the number of documents.

`ColumnBuffers` is the batch implementation. `src/tests.rs` carries a streaming
implementation that asserts the invariant, as a worked example and a guard
against regressions.

## Design notes

- **`detect_format` consumes only leading whitespace**, leaving the reader
  positioned at the first document. A stream that cannot be rewound can therefore
  be framed and consumed in one pass.
- **NDJSON streams; a top-level JSON array does not.** Array input is parsed
  whole before iteration, so it is bounded by available memory. NDJSON is the
  shape to prefer for large inputs.
- **Errors are concrete.** `CoreError` is `Send + Sync` so a caller can map it
  onto its own error type rather than an opaque `Box<dyn Error>`.
- **Provenance is not hard-coded to local files.** A caller supplies the source
  locator and connection kind, so an in-database loader can stamp `s3` rather
  than `local-file`.

## Tests

```bash
cargo test --manifest-path crates/json_tables_core/Cargo.toml
```

The end-to-end behaviour of the whole pipeline — Parquet output, DDL, manifest,
and live Exasol imports — is covered by the test suite in
[`json_tables_ingest`](../json_tables_ingest).

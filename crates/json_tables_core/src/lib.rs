//! Normalisation core for **Exasol JSON Tables**: JSON documents in, relational
//! table-family rows out.
//!
//! This crate holds the parts of ingest that have no opinion about I/O — the
//! table contract, schema inference, the document traversal, the DDL and the
//! source manifest. It reads from any [`std::io::BufRead`] and writes through the
//! [`RowSink`] trait, so the same logic backs:
//!
//! - the `json_to_parquet` CLI, which sinks into column buffers and stages Parquet;
//! - an in-database loader, which sinks into a stream or a UDF `EMITS` channel.
//!
//! # The two passes
//!
//! ```no_run
//! use json_tables_core::{
//!     buffer::ColumnBuffers,
//!     infer::{build_all_schema_plans, StatsCollector},
//!     read::{detect_format, for_each_document},
//!     sink::write_document,
//! };
//! # fn main() -> Result<(), json_tables_core::CoreError> {
//! # let open = || -> std::io::BufReader<std::fs::File> { unimplemented!() };
//!
//! // Pass 1 — observe the documents and derive the family.
//! let mut reader = open();
//! let format = detect_format(&mut reader)?;
//! let mut stats = StatsCollector::new();
//! for_each_document(reader, format, |_, document| {
//!     stats.record_document(document);
//!     Ok(())
//! })?;
//! let plans = build_all_schema_plans(&stats.finish());
//!
//! // Pass 2 — write the rows into any sink.
//! let mut sink = ColumnBuffers::new(&plans);
//! let mut reader = open();
//! let format = detect_format(&mut reader)?;
//! for_each_document(reader, format, |_, document| write_document(&mut sink, document))?;
//! # Ok(())
//! # }
//! ```
//!
//! The schema must be known before rows can be written, so the input is read
//! twice. A caller whose source cannot be re-read must either buffer it or infer
//! from a sample.

pub mod buffer;
pub mod contract;
pub mod ddl;
pub mod error;
pub mod infer;
pub mod manifest;
pub mod read;
pub mod sink;

pub use error::{CoreError, CoreResult};

#[cfg(test)]
mod tests;

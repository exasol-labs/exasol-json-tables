//! **In-database JSON Tables loader** — an Exasol Rust UDF that ingests JSON
//! into the shared table contract without a client in the data path.
//!
//! The normalisation is [`json_tables_core`], the same code the
//! `json_to_parquet` CLI uses. This crate is the in-database front end.
//!
//! # The three scripts
//!
//! | Script | Shape | Purpose |
//! |---|---|---|
//! | `INGEST_JSON` | `SCALAR … EMITS` | One-call driver: infer, create the tables, load them, stamp provenance |
//! | `LOAD_TABLE` | `SCALAR … EMITS` | Emit the rows of **one** table; called by the driver, one statement per table |
//! | `PLAN_JSON` | `SCALAR … EMITS` | Pass 1 only: return the plan and its DDL for review |
//!
//! # How a load actually moves
//!
//! `INGEST_JSON` opens an ordinary SQL session (connect-back) and uses it for
//! *control* — `CREATE SCHEMA`, `CREATE TABLE`, `COMMENT`, and one
//! `INSERT INTO … SELECT LOAD_TABLE(…) EMITS (…)` per table. The rows never
//! travel that session: they go out through the engine's own emit channel inside
//! each `INSERT`. That matters because the SDK's connect-back API has no bulk
//! path — only `execute`, and row-at-a-time SQL is two orders of magnitude
//! slower than emitting.
//!
//! # Install
//!
//! ```sql
//! CREATE CONNECTION JSON_TABLES_SELF TO '<cluster-ip>:8563' USER 'sys' IDENTIFIED BY '…';
//!
//! CREATE OR REPLACE RUST SCALAR SCRIPT JSON_TABLES.INGEST_JSON(
//!   source VARCHAR(2000), target_schema VARCHAR(128),
//!   connection_name VARCHAR(128), options VARCHAR(4000)
//! ) EMITS (table_name VARCHAR(128), rows_loaded DECIMAL(18,0), status VARCHAR(200)) AS
//! %connection JSON_TABLES_SELF;
//! %udf_object /buckets/bfsdefault/rust/libjson_tables_udf.so;
//! /
//! ```
//!
//! See [`sql/install.sql`](../sql/install.sql) for the full set.

pub mod driver;
pub mod load;
pub mod plan_wire;
pub mod source;
pub mod sql;

use exasol_udf_macros::exasol_udf;
use exasol_udf_sdk::connect_back::ExaConnection;
use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{Decimal, Value};
use json_tables_core::contract::TablePath;
use json_tables_core::ddl::build_sql_schema;
use json_tables_core::read::for_each_document;
use json_tables_core::sink::write_document;

use crate::driver::{ingest, Options, TableReport};
use crate::load::EmitSink;
use crate::source::Source;

/// The `CONNECTION` a script points at when the caller does not name one.
pub const DEFAULT_CONNECTION: &str = "JSON_TABLES_SELF";
/// The loader script the driver invokes when the caller does not name one.
pub const DEFAULT_LOADER: &str = "LOAD_TABLE";

/// `INGEST_JSON(source, target_schema, connection_name, options)`
///
/// Emits one row per table: `(table_name, rows_loaded, status)`.
#[exasol_udf]
pub fn ingest_json(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    let source_text = required_string(ctx, 0, "source")?;
    let target_schema = required_string(ctx, 1, "target_schema")?;
    let connection_name =
        optional_string(ctx, 2)?.unwrap_or_else(|| DEFAULT_CONNECTION.to_string());
    let options_text = optional_string(ctx, 3)?;

    let source = Source::parse(&source_text)?;
    let options = parse_options(
        options_text.as_deref(),
        &source,
        &connection_name,
        ctx.script_schema().as_str(),
    )?;

    let mut connection = open_connection(ctx, &connection_name)?;
    let report = ingest(&mut connection, &source, &target_schema, &options)?;

    for TableReport {
        table_name,
        rows_loaded,
        status,
    } in report
    {
        ctx.emit(&[
            Value::String(table_name),
            Value::Numeric(Decimal {
                unscaled: rows_loaded as i128,
                scale: 0,
            }),
            Value::String(status),
        ])?;
    }
    Ok(())
}

/// `LOAD_TABLE(source, table_path, plan, connection_name)`
///
/// Emits the rows of one table, with the call-site `EMITS` clause the driver
/// generated from the same plan.
#[exasol_udf]
pub fn load_table(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    let source_text = required_string(ctx, 0, "source")?;
    let table_path = required_string(ctx, 1, "table_path")?;
    let plan_text = required_string(ctx, 2, "plan")?;
    let connection_name = optional_string(ctx, 3)?;

    let source = Source::parse(&source_text)?;
    let (plans, _stem) = plan_wire::decode(&plan_text)?;
    let target = find_path(&plans, &table_path)?;

    let text = match &source {
        Source::Table { .. } => {
            let name = connection_name
                .clone()
                .unwrap_or_else(|| DEFAULT_CONNECTION.to_string());
            let mut connection = open_connection(ctx, &name)?;
            source::read_to_string(&source, Some(&mut connection))?
        }
        _ => source::read_to_string(&source, None)?,
    };

    let (format, cursor) = source::documents_of(&text)?;
    let mut sink = EmitSink::new(ctx, &plans, &target)?;
    for_each_document(cursor, format, |_, document| {
        write_document(&mut sink, document)
    })
    .map_err(|err| UdfError::User(format!("cannot load the source: {err}")))?;
    sink.finish()?;
    Ok(())
}

/// `PLAN_JSON(source, connection_name, options)`
///
/// Emits `(plan, ddl)`: the wire plan the loaders take, and the DDL the driver
/// would run. Pass 1 only — nothing is created and nothing is loaded.
#[exasol_udf]
pub fn plan_json(ctx: &mut dyn UdfContext) -> Result<(), UdfError> {
    let source_text = required_string(ctx, 0, "source")?;
    let connection_name = optional_string(ctx, 1)?;
    let options_text = optional_string(ctx, 2)?;

    let source = Source::parse(&source_text)?;
    let options = parse_options(
        options_text.as_deref(),
        &source,
        connection_name.as_deref().unwrap_or(DEFAULT_CONNECTION),
        ctx.script_schema().as_str(),
    )?;

    let text = match &source {
        Source::Table { .. } => {
            let name = connection_name
                .clone()
                .unwrap_or_else(|| DEFAULT_CONNECTION.to_string());
            let mut connection = open_connection(ctx, &name)?;
            source::read_to_string(&source, Some(&mut connection))?
        }
        _ => source::read_to_string(&source, None)?,
    };

    let plans = driver::plan_text(&text)?;
    let plan = plan_wire::encode(&plans, &options.stem);
    let (create_stmts, constraint_stmts) = build_sql_schema(&plans, &options.stem);
    let ddl = create_stmts
        .into_iter()
        .chain(constraint_stmts)
        .collect::<Vec<_>>()
        .join("\n");

    ctx.emit(&[Value::String(plan), Value::String(ddl)])?;
    Ok(())
}

fn open_connection(
    ctx: &mut dyn UdfContext,
    connection_name: &str,
) -> Result<Box<dyn ExaConnection>, UdfError> {
    let credentials = ctx.connection(connection_name).map_err(|err| {
        UdfError::User(format!(
            "cannot read CONNECTION '{connection_name}': {err}. \
             Declare it in the script with '%connection {connection_name};'"
        ))
    })?;
    ctx.connect_back(&credentials)
}

fn parse_options(
    options_text: Option<&str>,
    source: &Source,
    connection_name: &str,
    script_schema: &str,
) -> Result<Options, UdfError> {
    let parsed: serde_json::Value = match options_text {
        Some(text) if !text.trim().is_empty() => serde_json::from_str(text)
            .map_err(|err| UdfError::User(format!("options is not valid JSON: {err}")))?,
        _ => serde_json::Value::Null,
    };

    let stem = parsed["stem"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| default_stem(source));
    let loader = parsed["loader"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| {
            if script_schema.is_empty() {
                DEFAULT_LOADER.to_string()
            } else {
                format!("{}.{}", quote(script_schema), DEFAULT_LOADER)
            }
        });

    Ok(Options {
        stem,
        loader,
        connection_name: connection_name.to_string(),
        replace: parsed["replace"].as_bool().unwrap_or(false),
        keep_landing: parsed["keepLanding"].as_bool().unwrap_or(false),
    })
}

fn quote(name: &str) -> String {
    crate::sql::quote_ident(name)
}

/// A table-name prefix derived from the source: the basename without its
/// extension, or the table name for a table source.
pub fn default_stem(source: &Source) -> String {
    let raw = match source {
        Source::BucketFs { path } => path
            .rsplit('/')
            .next()
            .unwrap_or("json")
            .split('.')
            .next()
            .unwrap_or("json")
            .to_string(),
        Source::Table { table, .. } => table.clone(),
        Source::Http { path, .. } => {
            let last = path.rsplit('/').next().unwrap_or("");
            let stem = last.split('.').next().unwrap_or("");
            if stem.is_empty() {
                "json".to_string()
            } else {
                stem.to_string()
            }
        }
        Source::Cloud { file, url } => {
            let candidate = if file.is_empty() { url } else { file };
            candidate
                .rsplit('/')
                .next()
                .unwrap_or("json")
                .split('.')
                .next()
                .unwrap_or("json")
                .to_string()
        }
    };
    let cleaned: String = raw
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    let trimmed = cleaned.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "json".to_string()
    } else {
        trimmed
    }
}

fn find_path(
    plans: &[json_tables_core::contract::PlannedTable],
    wanted: &str,
) -> Result<TablePath, UdfError> {
    plans
        .iter()
        .find(|plan| plan.path.to_string() == wanted)
        .map(|plan| plan.path.clone())
        .ok_or_else(|| UdfError::User(format!("the plan has no table for path '{wanted}'")))
}

fn required_string(ctx: &dyn UdfContext, col: usize, name: &str) -> Result<String, UdfError> {
    match ctx.get_string(col)? {
        Some(text) if !text.trim().is_empty() => Ok(text.to_string()),
        _ => Err(UdfError::User(format!("{name} must not be null or empty"))),
    }
}

fn optional_string(ctx: &dyn UdfContext, col: usize) -> Result<Option<String>, UdfError> {
    if col >= ctx.num_columns() {
        return Ok(None);
    }
    Ok(ctx
        .get_string(col)?
        .map(str::to_string)
        .filter(|text| !text.trim().is_empty()))
}

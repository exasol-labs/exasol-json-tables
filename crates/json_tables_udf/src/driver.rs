//! The one-statement ingest driver.
//!
//! `INGEST_JSON` is the control plane: it infers the family, creates the tables
//! through its connect-back session, then asks the engine to run one
//! `INSERT … SELECT loader(…)` per table. **Only SQL crosses the connect-back
//! session; the rows travel the engine's emit channel**, which is why this is
//! fast despite the session having no bulk-load API.

use exasol_udf_sdk::connect_back::ExaConnection;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;
use json_tables_core::contract::{table_raw_name, PlannedTable};
use json_tables_core::ddl::build_sql_schema;
use json_tables_core::infer::{build_all_schema_plans, StatsCollector};
use json_tables_core::manifest::{build_provenance_comments, Provenance};
use json_tables_core::read::for_each_document;

use crate::plan_wire;
use crate::source::{materialise_into_landing, read_to_string, Source};
use crate::sql::{
    comment_statement, load_statement, qualify_ddl, quote_ident, without_trailing_semicolon,
};

/// What a caller can tune per invocation.
#[derive(Debug, Clone)]
pub struct Options {
    /// Prefix for generated table names. Defaults to the source's basename.
    pub stem: String,
    /// Fully-qualified name of the row-emitting loader script.
    pub loader: String,
    /// The `CONNECTION` the loader statements should use, passed through to them.
    pub connection_name: String,
    /// Drop existing tables of the same name instead of failing.
    pub replace: bool,
    /// Keep the landing table a stream source was materialised into.
    pub keep_landing: bool,
}

/// One line of the run report.
#[derive(Debug, Clone, PartialEq)]
pub struct TableReport {
    pub table_name: String,
    pub rows_loaded: i64,
    pub status: String,
}

/// Infer, create, and load the whole family. Returns one report row per table.
pub fn ingest(
    connection: &mut Box<dyn ExaConnection>,
    source: &Source,
    target_schema: &str,
    options: &Options,
) -> Result<Vec<TableReport>, UdfError> {
    connection.execute(&format!(
        "CREATE SCHEMA IF NOT EXISTS {}",
        quote_ident(target_schema)
    ))?;

    // A stream or cloud object can only be read once, so land it first and read
    // that instead — every load pass needs the source again.
    let landing = if source.is_rereadable() {
        None
    } else {
        Some(materialise_into_landing(
            connection,
            source,
            target_schema,
            &format!("_JT_LANDING_{}", options.stem.to_uppercase()),
            Some(&options.connection_name),
        )?)
    };
    let effective_source = match &landing {
        Some(landing) => landing.as_source(),
        None => source.clone(),
    };

    let plans = plan_source(connection, &effective_source, options)?;
    let plan_text = plan_wire::encode(&plans, &options.stem);

    let (create_stmts, constraint_stmts) = build_sql_schema(&plans, &options.stem);
    if options.replace {
        for plan in &plans {
            connection.execute(&format!(
                "DROP TABLE IF EXISTS {}.{} CASCADE",
                quote_ident(target_schema),
                quote_ident(&table_raw_name(&plan.path, &options.stem))
            ))?;
        }
    }
    for statement in &create_stmts {
        connection.execute(&qualify_ddl(
            without_trailing_semicolon(statement),
            target_schema,
        ))?;
    }

    let mut report = Vec::with_capacity(plans.len());
    for plan in &plans {
        let table_name = table_raw_name(&plan.path, &options.stem);
        let statement = load_statement(
            target_schema,
            &table_name,
            &options.loader,
            &[
                effective_source.to_string(),
                plan.path.to_string(),
                plan_text.clone(),
                options.connection_name.clone(),
            ],
        );
        let rows = connection.execute(&statement)?;
        report.push(TableReport {
            table_name,
            rows_loaded: rows as i64,
            status: "loaded".to_string(),
        });
    }

    for statement in &constraint_stmts {
        connection.execute(&qualify_ddl(
            without_trailing_semicolon(statement),
            target_schema,
        ))?;
    }

    stamp_provenance(connection, &plans, source, target_schema, options)?;

    if let Some(landing) = landing {
        if options.keep_landing {
            report.push(TableReport {
                table_name: landing.table.clone(),
                rows_loaded: 0,
                status: "landing kept".to_string(),
            });
        } else {
            connection.execute(&landing.drop_statement())?;
        }
    }

    Ok(report)
}

/// Pass 1: observe the documents and derive the family.
fn plan_source(
    connection: &mut Box<dyn ExaConnection>,
    source: &Source,
    options: &Options,
) -> Result<Vec<PlannedTable>, UdfError> {
    let text = read_to_string(source, Some(connection))?;
    let plans = plan_text(&text)?;
    if plans.is_empty() {
        return Err(UdfError::User(format!(
            "no documents found in {source} (stem '{}')",
            options.stem
        )));
    }
    Ok(plans)
}

/// Derive the family from already-read text. Split out so it is testable without
/// a source or a connection.
pub fn plan_text(text: &str) -> Result<Vec<PlannedTable>, UdfError> {
    let (format, cursor) = crate::source::documents_of(text)?;
    let mut collector = StatsCollector::new();
    for_each_document(cursor, format, |_, document| {
        collector.record_document(document);
        Ok(())
    })
    .map_err(|err| UdfError::User(format!("cannot scan the source: {err}")))?;
    Ok(build_all_schema_plans(&collector.finish()))
}

fn stamp_provenance(
    connection: &mut Box<dyn ExaConnection>,
    plans: &[PlannedTable],
    source: &Source,
    target_schema: &str,
    options: &Options,
) -> Result<(), UdfError> {
    let imported_at = database_timestamp(connection)?;
    let locator = source.to_string();
    let provenance = Provenance {
        source: &locator,
        source_connection: source.connection_kind(),
        imported_at: &imported_at,
        source_modified_at: None,
    };
    for (table_name, comment) in build_provenance_comments(plans, &options.stem, &provenance) {
        connection.execute(&comment_statement(target_schema, &table_name, &comment))?;
    }
    Ok(())
}

/// The database's own clock, so ingest timestamps agree with everything else
/// recorded in the database rather than with the UDF container's clock.
fn database_timestamp(connection: &mut Box<dyn ExaConnection>) -> Result<String, UdfError> {
    let rows = connection.query("SELECT CURRENT_TIMESTAMP")?;
    match rows.first().and_then(|row| row.first()) {
        Some(Value::Timestamp(ts)) => Ok(ts.format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(UdfError::User(
            "could not read the database timestamp".to_string(),
        )),
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;

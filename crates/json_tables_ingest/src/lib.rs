//! `json_to_parquet` — the local-file ingest CLI for **Exasol JSON Tables**.
//!
//! The normalisation itself lives in [`json_tables_core`]: this crate is the
//! I/O-bound half — reading files, staging Parquet, and importing into Exasol
//! over `exarrow-rs`. Anything that would also be needed by an in-database
//! loader (the table contract, schema inference, the document traversal, DDL and
//! the source manifest) belongs in the core crate rather than here.

use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use exarrow_rs::adbc::{Connection, Driver};
use exarrow_rs::import::ParquetImportOptions;
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::{SerializedColumnWriter, SerializedFileWriter},
    },
    schema::types::TypePtr,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    error::Error,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::Arc,
};

pub use json_tables_core::buffer::{ColumnBuffers, ColumnValues, TableBuffer};
pub use json_tables_core::contract::{
    classify_value, column_sql_type, column_type_metadata, encode_path_component, sanitize_ident,
    table_raw_name, table_sql_name, table_token, ColumnKind, ColumnPlan, FieldKey, PathKind,
    PathSegment, PlannedTable, PropertyColumns, SimpleType, TablePath,
};
pub use json_tables_core::ddl::build_sql_schema;
pub use json_tables_core::infer::{
    accumulate_array_stats, accumulate_object_stats, build_all_schema_plans, build_schema_plan,
    PropertyStats, StatsCollector, TableStats,
};
pub use json_tables_core::manifest::{
    apply_table_comments, build_source_manifest, provenance_comment_statement, Provenance,
};
pub use json_tables_core::read::InputFormat;
pub use json_tables_core::sink::write_document;
pub use json_tables_core::{CoreError, CoreResult};

use json_tables_core::read::{detect_format, for_each_document};

type DynError = Box<dyn Error + Send + Sync>;

/// Command-line arguments for the json_to_parquet tool.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the input JSON file to convert.
    #[arg(short, long)]
    input: PathBuf,

    /// Directory where Parquet files will be written (defaults to current directory).
    #[arg(short, long)]
    output_dir: Option<PathBuf>,

    /// Optionally emit an Exasol SQL schema describing the generated Parquet tables.
    #[arg(long, default_value_t = false)]
    schema_sql: bool,

    /// Optionally emit a source-manifest JSON artifact describing the planned table family.
    #[arg(long)]
    manifest_output: Option<PathBuf>,

    /// Exasol connection URL (exasol://user:pass@host:port/schema?param=value).
    #[arg(long)]
    exasol: Option<String>,

    /// Use TLS for Exasol HTTP import transport (separate from the control connection TLS).
    #[arg(long, default_value_t = false)]
    exasol_http_tls: bool,

    /// When importing into Exasol, write intermediate Parquet files to this directory.
    #[arg(long)]
    exasol_temp_dir: Option<PathBuf>,

    /// When importing into Exasol, clean up intermediate Parquet files after upload.
    #[arg(long, default_value_t = false)]
    exasol_cleanup: bool,
}

pub fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let mut output_dir = args
        .output_dir
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let mut temp_dir_created = false;
    if args.exasol.is_some() {
        if let Some(exasol_temp_dir) = args.exasol_temp_dir.as_ref() {
            output_dir = exasol_temp_dir.clone();
        } else if args.exasol_cleanup {
            output_dir = create_temp_output_dir()?;
            temp_dir_created = true;
        }
    }

    // Stage 1: scan and count property/type combos.
    let format = detect_input_format(&args.input)?;
    let table_stats = scan_all_stats(&args.input, format)?;
    let stem = args
        .input
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "output".to_string());

    println!("Scanned JSON file: {:?} (format: {:?})", args.input, format);
    println!("Output directory: {:?}", output_dir);
    println!();

    for table in &table_stats {
        println!("Table: {}", table.path);
        print_stats(&table.stats);
        println!();
    }

    let planned_tables = build_all_schema_plans(&table_stats);

    // Stage 2: derive schema from stats and write Parquet tables (root + subtables).
    std::fs::create_dir_all(&output_dir)?;
    if args.schema_sql {
        write_sql_schema(&planned_tables, &output_dir, &stem)?;
    }
    if let Some(manifest_output) = args.manifest_output.as_ref() {
        write_source_manifest(&planned_tables, manifest_output, &stem)?;
    }
    let table_files = write_all_tables(&args.input, format, &planned_tables, &output_dir, &stem)?;

    let provenance_comments = if let Some(exasol_url) = args.exasol.as_deref() {
        Some(import_into_exasol(
            exasol_url,
            args.exasol_http_tls,
            &planned_tables,
            &table_files,
            &stem,
            &args.input,
        )?)
    } else {
        None
    };
    if let (Some(manifest_output), Some(comments)) =
        (args.manifest_output.as_ref(), provenance_comments.as_ref())
    {
        write_source_manifest_with_comments(&planned_tables, manifest_output, &stem, comments)?;
    }

    if args.exasol.is_some() && args.exasol_cleanup {
        let should_cleanup = temp_dir_created || args.exasol_temp_dir.is_some();
        if should_cleanup {
            std::fs::remove_dir_all(&output_dir)?;
            println!("Cleaned up intermediate files at {:?}", output_dir);
        } else {
            println!(
                "Skipping cleanup (no Exasol temp dir set and not auto-created): {:?}",
                output_dir
            );
        }
    }

    Ok(())
}

fn create_temp_output_dir() -> Result<PathBuf, Box<dyn Error>> {
    let base = std::env::temp_dir();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let pid = std::process::id();
    let dir = base.join(format!("json_to_parquet_{pid}_{ts}"));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[derive(Debug, Clone)]
struct TableFile {
    path: TablePath,
    file_path: PathBuf,
}

async fn connect_exasol(exasol_url: &str) -> Result<Connection, DynError> {
    let driver = Driver::new();
    let database = driver.open(exasol_url)?;
    Ok(database.connect().await?)
}

fn import_into_exasol(
    exasol_url: &str,
    exasol_http_tls: bool,
    planned_tables: &[PlannedTable],
    table_files: &[TableFile],
    stem: &str,
    input_path: &Path,
) -> Result<Vec<(String, String)>, Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime
        .block_on(async {
            import_into_exasol_async(
                exasol_url,
                exasol_http_tls,
                planned_tables,
                table_files,
                stem,
                input_path,
            )
            .await
        })
        .map_err(|err| err as Box<dyn Error>)
}

async fn import_into_exasol_async(
    exasol_url: &str,
    exasol_http_tls: bool,
    planned_tables: &[PlannedTable],
    table_files: &[TableFile],
    stem: &str,
    input_path: &Path,
) -> Result<Vec<(String, String)>, DynError> {
    let (create_stmts, constraint_stmts) = build_sql_schema(planned_tables, stem);
    let mut table_name_map = HashMap::new();
    for plan in planned_tables {
        table_name_map.insert(plan.path.clone(), table_sql_name(&plan.path, stem));
    }

    let mut table_to_files: HashMap<TablePath, Vec<PathBuf>> = HashMap::new();
    for table_file in table_files {
        table_to_files
            .entry(table_file.path.clone())
            .or_default()
            .push(table_file.file_path.clone());
    }

    {
        let mut connection = connect_exasol(exasol_url).await?;
        for stmt in &create_stmts {
            connection.execute(stmt.clone()).await?;
        }
        connection.close().await?;
    }

    let max_parallel = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(16);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallel));
    let mut join_set = tokio::task::JoinSet::new();

    for (table_path, files) in table_to_files {
        let table_name = table_name_map
            .get(&table_path)
            .cloned()
            .unwrap_or_else(|| table_sql_name(&table_path, stem));
        let url = exasol_url.to_string();
        let semaphore = semaphore.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire().await?;
            let mut connection = connect_exasol(&url).await?;
            let import_options = ParquetImportOptions::default().use_tls(exasol_http_tls);
            let rows = connection
                .import_parquet_from_files(&table_name, files, import_options)
                .await?;
            connection.close().await?;
            Ok::<(String, usize), DynError>((table_name, rows as usize))
        });
    }

    while let Some(task) = join_set.join_next().await {
        let (table, rows) = task??;
        println!("Imported {} rows into Exasol table {}", rows, table);
    }

    if !constraint_stmts.is_empty() {
        let mut connection = connect_exasol(exasol_url).await?;
        for stmt in &constraint_stmts {
            connection.execute(stmt.clone()).await?;
        }
        connection.close().await?;
    }

    let imported_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let source_path = input_path
        .canonicalize()
        .unwrap_or_else(|_| input_path.to_path_buf());
    let source_modified_at = std::fs::metadata(&source_path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .map(|modified| DateTime::<Utc>::from(modified).to_rfc3339_opts(SecondsFormat::Secs, true));
    let provenance_comments = build_provenance_comments(
        planned_tables,
        stem,
        &source_path,
        &imported_at,
        source_modified_at.as_deref(),
    );
    let mut connection = connect_exasol(exasol_url).await?;
    for (table_name, comment) in &provenance_comments {
        connection
            .execute(provenance_comment_statement(table_name, comment))
            .await?;
    }
    connection.close().await?;

    Ok(provenance_comments)
}

/// Provenance for a file read from the machine running the CLI.
///
/// An in-database loader builds [`Provenance`] itself with its own source kind
/// (`s3`, a landing table, a client tunnel) and calls the core builder directly.
fn build_provenance_comments(
    planned_tables: &[PlannedTable],
    stem: &str,
    source_path: &Path,
    imported_at: &str,
    source_modified_at: Option<&str>,
) -> Vec<(String, String)> {
    let source = source_path.to_string_lossy();
    let provenance = Provenance::local_file(source.as_ref(), imported_at)
        .with_source_modified_at(source_modified_at);
    json_tables_core::manifest::build_provenance_comments(planned_tables, stem, &provenance)
}

fn write_sql_schema(
    plans: &[PlannedTable],
    output_dir: &Path,
    stem: &str,
) -> Result<(), Box<dyn Error>> {
    let (create_stmts, constraint_stmts) = build_sql_schema(plans, stem);

    let mut ddl = String::new();
    ddl.push_str("-- Exasol SQL schema generated by json_to_parquet\n");
    ddl.push_str("-- Tables mirror the emitted Parquet files; identifiers are quoted to preserve names with special characters.\n\n");

    for stmt in create_stmts {
        ddl.push_str(&stmt);
        ddl.push_str("\n\n");
    }

    if !constraint_stmts.is_empty() {
        ddl.push_str("-- Constraints\n");
        for stmt in constraint_stmts {
            ddl.push_str(&stmt);
            ddl.push('\n');
        }
        ddl.push('\n');
    }

    let output_path = output_dir.join(format!("{stem}.sql"));
    std::fs::write(&output_path, ddl)?;
    println!("Wrote Exasol SQL schema to {:?}", output_path);
    Ok(())
}

fn write_source_manifest(
    plans: &[PlannedTable],
    output_path: &Path,
    stem: &str,
) -> Result<(), Box<dyn Error>> {
    let manifest = build_source_manifest(plans, stem);
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(output_path, serde_json::to_string_pretty(&manifest)? + "\n")?;
    println!("Wrote source manifest to {:?}", output_path);
    Ok(())
}

fn write_source_manifest_with_comments(
    plans: &[PlannedTable],
    output_path: &Path,
    stem: &str,
    comments: &[(String, String)],
) -> Result<(), Box<dyn Error>> {
    let mut manifest = build_source_manifest(plans, stem);
    apply_table_comments(&mut manifest, comments);
    std::fs::write(output_path, serde_json::to_string_pretty(&manifest)? + "\n")?;
    println!("Updated source manifest provenance at {:?}", output_path);
    Ok(())
}

/// Detect the framing of a file on disk.
fn detect_input_format(path: &Path) -> Result<InputFormat, Box<dyn Error>> {
    let mut reader = BufReader::new(File::open(path)?);
    Ok(detect_format(&mut reader)?)
}

/// Walk the documents of a file on disk.
fn for_each_object<F>(path: &Path, format: InputFormat, f: F) -> Result<(), Box<dyn Error>>
where
    F: FnMut(usize, &serde_json::Map<String, Value>) -> CoreResult<()>,
{
    let mut reader = BufReader::new(File::open(path)?);
    // Re-detect so the reader is positioned past any leading whitespace, exactly
    // as `detect_input_format` left the probe reader.
    let _ = detect_format(&mut reader)?;
    Ok(for_each_document(reader, format, f)?)
}

/// Pass 1 over a file: observe every document and return per-table statistics.
fn scan_all_stats(path: &Path, format: InputFormat) -> Result<Vec<TableStats>, Box<dyn Error>> {
    let mut collector = StatsCollector::new();
    for_each_object(path, format, |_, obj| {
        collector.record_document(obj);
        Ok(())
    })?;
    Ok(collector.finish())
}

/// Read-back helper for quick sanity: prints row count and column names.
fn log_parquet_summary(path: &Path) -> Result<(), Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata().file_metadata();
    let cols: Vec<String> = meta
        .schema_descr()
        .columns()
        .iter()
        .map(|c: &Arc<parquet::schema::types::ColumnDescriptor>| c.path().string())
        .collect();

    println!(
        "Parquet summary: {} rows | {} columns",
        meta.num_rows(),
        cols.len()
    );
    println!("Columns: {}", cols.join(", "));
    Ok(())
}

fn print_stats(stats: &PropertyStats) {
    let mut entries: Vec<_> = stats.counts.iter().collect();
    entries.sort_by(|(left_key, _), (right_key, _)| {
        left_key
            .name
            .cmp(&right_key.name)
            .then(left_key.ty.cmp(&right_key.ty))
    });

    println!("Property/type combinations and counts:");
    for (key, count) in entries {
        println!("{:30} {:>8} -> {}", key.name, key.ty, count);
    }
}

/// Pass 2 over a file: normalise into column buffers, then stage one Parquet
/// file per table.
fn write_all_tables(
    input_path: &Path,
    format: InputFormat,
    planned_tables: &[PlannedTable],
    output_dir: &Path,
    stem: &str,
) -> Result<Vec<TableFile>, Box<dyn Error>> {
    let mut buffers = ColumnBuffers::new(planned_tables);
    let mut table_files = Vec::new();

    // Populate rows recursively starting from root table.
    for_each_object(input_path, format, |_, obj| {
        write_document(&mut buffers, obj)
    })?;

    // Write each table to disk.
    for (path, table) in buffers.tables() {
        let file_name = match path.file_suffix() {
            None => format!("{stem}.parquet"),
            Some(suffix) => format!("{stem}.{suffix}.parquet"),
        };
        let output_path = output_dir.join(file_name);
        write_parquet(table, &output_path)?;
        table_files.push(TableFile {
            path: path.clone(),
            file_path: output_path.clone(),
        });
        println!("Wrote Parquet file for table {} to {:?}", path, output_path);
        let _ = log_parquet_summary(&output_path);
    }

    Ok(table_files)
}

fn write_parquet(table: &TableBuffer, output_path: &Path) -> Result<(), Box<dyn Error>> {
    let columns = &table.plan.columns;

    let schema = build_parquet_schema(columns)?;
    let props = WriterProperties::builder().build().into();
    let file = File::create(output_path)?;
    let mut writer = SerializedFileWriter::new(file, schema, props)?;

    {
        let mut row_group = writer.next_row_group()?;

        for column in columns {
            let mut col_writer = row_group
                .next_column()?
                .ok_or("Row group column writer missing")?;

            let values = table.column(column)?;
            write_column_values(&mut col_writer, values)?;
            col_writer.close()?;
        }

        row_group.close()?;
    }

    writer.close()?;
    Ok(())
}

fn build_parquet_schema(columns: &[ColumnPlan]) -> parquet::errors::Result<TypePtr> {
    let mut fields = Vec::with_capacity(columns.len());

    for column in columns {
        // Every column is OPTIONAL so missing values become nulls (except required null-mask columns).
        let builder = match column.ty {
            SimpleType::Bool => parquet::schema::types::Type::primitive_type_builder(
                &column.name,
                PhysicalType::BOOLEAN,
            ),
            SimpleType::Integer => parquet::schema::types::Type::primitive_type_builder(
                &column.name,
                PhysicalType::INT64,
            ),
            SimpleType::Number => parquet::schema::types::Type::primitive_type_builder(
                &column.name,
                PhysicalType::DOUBLE,
            ),
            SimpleType::String => parquet::schema::types::Type::primitive_type_builder(
                &column.name,
                PhysicalType::BYTE_ARRAY,
            )
            .with_converted_type(ConvertedType::UTF8),
            SimpleType::Null | SimpleType::Object | SimpleType::Array => continue,
        };

        let repetition = if column.is_null_mask || column.is_required {
            Repetition::REQUIRED
        } else {
            Repetition::OPTIONAL
        };

        let field = builder.with_repetition(repetition).build()?;
        fields.push(Arc::new(field));
    }

    let schema = parquet::schema::types::Type::group_type_builder("schema")
        .with_fields(fields)
        .build()?;

    Ok(Arc::new(schema))
}

fn write_column_values(
    writer: &mut SerializedColumnWriter<'_>,
    values: &ColumnValues,
) -> Result<(), Box<dyn Error>> {
    let untyped = writer.untyped();
    match (untyped, values) {
        (ColumnWriter::BoolColumnWriter(ref mut w), ColumnValues::Bool(v)) => {
            // Definition levels mark which rows are present (1) vs null (0); data only includes present values.
            let def_levels: Vec<i16> = v
                .iter()
                .map(|val| if val.is_some() { 1 } else { 0 })
                .collect();
            let data: Vec<bool> = v.iter().filter_map(|val| *val).collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        (ColumnWriter::BoolColumnWriter(ref mut w), ColumnValues::BoolMask(v)) => {
            // Required bool: no definition levels, all rows present.
            w.write_batch(v, None, None)?;
        }
        (ColumnWriter::Int64ColumnWriter(ref mut w), ColumnValues::Int(v)) => {
            let def_levels: Vec<i16> = v
                .iter()
                .map(|val| if val.is_some() { 1 } else { 0 })
                .collect();
            let data: Vec<i64> = v.iter().filter_map(|val| *val).collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        (ColumnWriter::DoubleColumnWriter(ref mut w), ColumnValues::Double(v)) => {
            let def_levels: Vec<i16> = v
                .iter()
                .map(|val| if val.is_some() { 1 } else { 0 })
                .collect();
            let data: Vec<f64> = v.iter().filter_map(|val| *val).collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        (ColumnWriter::ByteArrayColumnWriter(ref mut w), ColumnValues::Str(v)) => {
            let def_levels: Vec<i16> = v
                .iter()
                .map(|val| if val.is_some() { 1 } else { 0 })
                .collect();
            let data: Vec<ByteArray> = v
                .iter()
                .filter_map(|val| val.as_ref().map(|s| ByteArray::from(s.as_bytes())))
                .collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        _ => return Err("Column type mismatch during Parquet write".into()),
    }

    Ok(())
}

#[cfg(test)]
mod tests;

//! json_to_parquet: Normalize semi-structured JSON into Parquet tables.
//!
//! High-level approach:
//! - Inputs are JSON array or NDJSON of top-level objects. We recursively scan to collect per-table
//!   stats. Each table corresponds to a path in the JSON:
//!     * root object -> root table.
//!     * nested objects -> child tables (path segments use the property name).
//!     * arrays -> child tables (path segments suffixed with `[]`), preserving element order.
//! - Column derivation:
//!     * Simple values (null/bool/int/number/string) become columns named after the property.
//!     * Explicit nulls add a `<name>|n` required bool mask; missing values remain nulls.
//!     * Object properties in a parent table become integer FK columns `<name>|object`.
//!     * Array properties in a parent table become integer count columns `<name>|array`.
//!     * Array elements live in child tables with `_value` columns (per main/alternate type) plus
//!       inline object fields; nested arrays/objects recurse into deeper subtables.
//!     * Integer + number are merged to number.
//!     * Columns follow first-seen JSON order (after identifiers), to keep a familiar layout.
//! - Table identifiers:
//!     * Every table can include `_id` when needed (always for objects; for arrays only when they
//!       contain nested arrays to support backrefs).
//!     * Array tables always have `_parent` (FK to parent row) and `_pos` (array index).
//! - Writing:
//!     * We materialize column-major buffers per table, walk the JSON recursively, assign IDs,
//!       set FKs/counts, preserve array positions, then emit one Parquet file per table
//!       (`<stem>.parquet`, `<stem>.<path>.parquet`).
//!
//! Running:
//!   * `cargo run -- --input path/to/data.json` (auto-detects array vs NDJSON).
//!   * Optional flags: `--output-dir DIR` for Parquet/SQL output, `--schema-sql` to emit an Exasol
//!     DDL file describing all tables/keys.

use clap::Parser;
use exarrow_rs::adbc::{Connection, Driver};
use exarrow_rs::import::ParquetImportOptions;
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        reader::FileReader,
        reader::SerializedFileReader,
        writer::{SerializedColumnWriter, SerializedFileWriter},
    },
    schema::types::TypePtr,
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    error::Error,
    fmt,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
};

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

    /// When importing into Exasol, write intermediate Parquet files to this directory.
    #[arg(long)]
    exasol_temp_dir: Option<PathBuf>,

    /// When importing into Exasol, clean up intermediate Parquet files after upload.
    #[arg(long, default_value_t = false)]
    exasol_cleanup: bool,
}

#[derive(Debug, Clone, Copy)]
enum InputFormat {
    Array,
    Lines,
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

    if let Some(exasol_url) = args.exasol.as_deref() {
        import_into_exasol(exasol_url, &planned_tables, &table_files, &stem)?;
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
    let schema = database.params().schema.clone();
    let mut connection = database.connect().await?;
    if let Some(schema) = schema {
        connection.set_schema(schema).await?;
    }
    Ok(connection)
}

fn import_into_exasol(
    exasol_url: &str,
    planned_tables: &[PlannedTable],
    table_files: &[TableFile],
    stem: &str,
) -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime
        .block_on(async { import_into_exasol_async(exasol_url, planned_tables, table_files, stem).await })
        .map_err(|err| err as Box<dyn Error>)
}

async fn import_into_exasol_async(
    exasol_url: &str,
    planned_tables: &[PlannedTable],
    table_files: &[TableFile],
    stem: &str,
) -> Result<(), DynError> {
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
            let import_options = ParquetImportOptions::default();
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

    Ok(())
}

fn sanitize_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

fn table_token(path: &TablePath, stem: &str) -> String {
    let raw = match path.file_suffix() {
        None => stem.to_string(),
        Some(suffix) => format!("{}_{}", stem, suffix),
    };
    let mut token: String = raw
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    while token.contains("__") {
        token = token.replace("__", "_");
    }
    token.trim_matches('_').to_string()
}

fn table_sql_name(path: &TablePath, stem: &str) -> String {
    sanitize_ident(&table_raw_name(path, stem))
}

fn table_raw_name(path: &TablePath, stem: &str) -> String {
    let raw = path
        .file_suffix()
        .map(|s| s.replace("[]", "_arr").replace('.', "_"))
        .unwrap_or_default();
    if raw.is_empty() {
        stem.to_string()
    } else {
        format!("{}_{}", stem, raw)
    }
}

fn column_sql_type(ty: SimpleType) -> Option<&'static str> {
    match ty {
        SimpleType::Bool => Some("BOOLEAN"),
        SimpleType::Integer => Some("DECIMAL(18,0)"),
        SimpleType::Number => Some("DOUBLE"),
        SimpleType::String => Some("VARCHAR(2000000)"),
        SimpleType::Null | SimpleType::Object | SimpleType::Array => None,
    }
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

fn path_kind_label(kind: &PathKind) -> &'static str {
    match kind {
        PathKind::Object => "object",
        PathKind::Array => "array",
    }
}

fn column_type_metadata(column: &ColumnPlan) -> (String, Option<u32>, Option<u32>, Option<u32>) {
    match column.ty {
        SimpleType::Bool => ("BOOLEAN".to_string(), None, None, None),
        SimpleType::Integer => ("DECIMAL(18,0)".to_string(), None, Some(18), Some(0)),
        SimpleType::Number => ("DOUBLE".to_string(), None, None, None),
        SimpleType::String => ("VARCHAR(2000000)".to_string(), Some(2_000_000), None, None),
        SimpleType::Null | SimpleType::Object | SimpleType::Array => {
            unreachable!("column plans only contain physical columns")
        }
    }
}

fn build_source_manifest(plans: &[PlannedTable], stem: &str) -> serde_json::Value {
    let mut relationships = Vec::new();
    let mut family_tables_by_root: HashMap<String, Vec<String>> = HashMap::new();

    for plan in plans {
        let table_name = table_raw_name(&plan.path, stem);
        if let Some(parent_path) = plan.path.parent() {
            let parent_table = table_raw_name(&parent_path, stem);
            let segment = plan
                .path
                .segments
                .last()
                .expect("non-root table path must have at least one segment");
            relationships.push(json!({
                "parentTable": parent_table,
                "childTable": table_name,
                "segmentName": segment.name,
                "relationKind": path_kind_label(&segment.kind),
            }));
        }
    }

    let tables = plans
        .iter()
        .map(|plan| {
            let table_name = table_raw_name(&plan.path, stem);
            let root_table = if plan.path.is_root() {
                table_name.clone()
            } else {
                table_raw_name(&TablePath::root(), stem)
            };
            family_tables_by_root
                .entry(root_table.clone())
                .or_default()
                .push(table_name.clone());
            let path_segments: Vec<serde_json::Value> = plan
                .path
                .segments
                .iter()
                .map(|segment| {
                    json!({
                        "name": segment.name,
                        "kind": path_kind_label(&segment.kind),
                    })
                })
                .collect();
            let columns: Vec<serde_json::Value> = plan
                .columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let (type_name, size, precision, scale) = column_type_metadata(column);
                    json!({
                        "name": column.name,
                        "typeName": type_name,
                        "ordinal": index + 1,
                        "size": size,
                        "precision": precision,
                        "scale": scale,
                        "isRequired": column.is_required,
                        "isNullMask": column.is_null_mask,
                    })
                })
                .collect();
            json!({
                "tableName": table_name,
                "path": plan.path.to_string(),
                "pathSegments": path_segments,
                "kind": path_kind_label(&plan.kind),
                "hasNestedArray": plan.has_nested_array,
                "rootTable": root_table,
                "columns": columns,
            })
        })
        .collect::<Vec<_>>();

    let mut root_names: Vec<String> = family_tables_by_root.keys().cloned().collect();
    root_names.sort();
    let roots = root_names
        .iter()
        .map(|root_table| {
            let mut family_tables = family_tables_by_root
                .get(root_table)
                .cloned()
                .unwrap_or_default();
            family_tables.sort();
            json!({
                "tableName": root_table,
                "familyTables": family_tables,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "format": "exasol-json-tables-source-manifest",
        "version": 1,
        "generator": "json_to_parquet",
        "stem": stem,
        "roots": roots,
        "relationships": relationships,
        "tables": tables,
    })
}

fn build_sql_schema(
    plans: &[PlannedTable],
    stem: &str,
) -> (Vec<String>, Vec<String>) {
    let mut name_map = HashMap::new();
    let mut token_map = HashMap::new();
    for plan in plans {
        name_map.insert(plan.path.clone(), table_sql_name(&plan.path, stem));
        token_map.insert(plan.path.clone(), table_token(&plan.path, stem));
    }

    let mut create_stmts = Vec::new();
    let mut pk_stmts = Vec::new();
    let mut fk_stmts = Vec::new();

    for plan in plans {
        let table_name = name_map
            .get(&plan.path)
            .cloned()
            .unwrap_or_else(|| sanitize_ident("table"));
        let table_token = token_map
            .get(&plan.path)
            .cloned()
            .unwrap_or_else(|| "table".to_string());

        let mut columns: Vec<String> = Vec::new();
        for col in &plan.columns {
            if let Some(sql_ty) = column_sql_type(col.ty) {
                let nn = if col.is_required || col.is_null_mask { " NOT NULL" } else { "" };
                columns.push(format!("  {} {}{}", sanitize_ident(&col.name), sql_ty, nn));
            }
        }

        let create_stmt = format!("CREATE TABLE {} (\n{}\n);", table_name, columns.join(",\n"));
        create_stmts.push(create_stmt);

        let mut pk_cols: Vec<String> = Vec::new();
        let has_id = plan.columns.iter().any(|c| c.name == "_id");
        if has_id {
            pk_cols.push(sanitize_ident("_id"));
        } else if plan.kind == PathKind::Array {
            pk_cols.push(sanitize_ident("_parent"));
            pk_cols.push(sanitize_ident("_pos"));
        }
        if !pk_cols.is_empty() {
            let pk_name = sanitize_ident(&format!("pk_{}", table_token));
            pk_stmts.push(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({}) DISABLE;",
                table_name,
                pk_name,
                pk_cols.join(", ")
            ));
        }

        if plan.columns.iter().any(|c| c.name == "_parent") {
            if let Some(parent_path) = plan.path.parent() {
                if let Some(parent_name) = name_map.get(&parent_path) {
                    let fk_name = sanitize_ident(&format!("fk_{}_parent", table_token));
                    fk_stmts.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) DISABLE;",
                        table_name,
                        fk_name,
                        sanitize_ident("_parent"),
                        parent_name,
                        sanitize_ident("_id")
                    ));
                }
            }
        }

        for (prop, cols) in &plan.properties {
            if let Some(object_fk) = cols.object_fk.as_ref() {
                let child_path = plan.path.child_object(prop);
                if let Some(child_name) = name_map.get(&child_path) {
                    let fk_name = sanitize_ident(&format!("fk_{}_{}", table_token, prop));
                    fk_stmts.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) DISABLE;",
                        table_name,
                        fk_name,
                        sanitize_ident(object_fk),
                        child_name,
                        sanitize_ident("_id")
                    ));
                }
            }
        }
    }

    let mut constraint_stmts = pk_stmts;
    constraint_stmts.extend(fk_stmts);
    (create_stmts, constraint_stmts)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum SimpleType {
    Null,
    Bool,
    Integer,
    Number,
    String,
    Object,
    Array,
}

impl std::fmt::Display for SimpleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            SimpleType::Null => "null",
            SimpleType::Bool => "bool",
            SimpleType::Integer => "integer",
            SimpleType::Number => "number",
            SimpleType::String => "string",
            SimpleType::Object => "object",
            SimpleType::Array => "array",
        };
        write!(f, "{label}")
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct FieldKey {
    name: String,
    ty: SimpleType,
}

#[derive(Debug, Clone)]
struct ColumnPlan {
    name: String,
    ty: SimpleType,
    is_null_mask: bool,
    is_required: bool,
    _kind: ColumnKind,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ColumnKind {
    Primary { property: String, main_type: SimpleType },
    Alternate { property: String, source_ty: SimpleType },
    NullBitmask { property: String },
}

#[derive(Debug, Default, Clone)]
struct PropertyColumns {
    main_type: Option<SimpleType>,
    primary: Option<String>,
    object_fk: Option<String>,
    null_mask: Option<String>,
    array_count: Option<String>,
    alternates: HashMap<SimpleType, String>,
}

#[derive(Debug, Default, Clone)]
struct PropertyStats {
    counts: HashMap<FieldKey, usize>,
    order: Vec<String>,
}

impl PropertyStats {
    fn record_value(&mut self, name: &str, value: &Value) {
        let ty = match classify_value(value) {
            Some(t) => t,
            None => return,
        };

        if !self.order.iter().any(|n| n == name) {
            self.order.push(name.to_owned());
        }

        let key = FieldKey {
            name: name.to_owned(),
            ty,
        };
        *self.counts.entry(key).or_insert(0) += 1;
    }
}

fn classify_value(value: &Value) -> Option<SimpleType> {
    match value {
        Value::Null => Some(SimpleType::Null),
        Value::Bool(_) => Some(SimpleType::Bool),
        Value::Number(n) => {
            if n.is_i64() {
                Some(SimpleType::Integer)
            } else if let Some(u) = n.as_u64() {
                if i64::try_from(u).is_ok() {
                    Some(SimpleType::Integer)
                } else {
                    // Preserve large unsigned values by routing them to DOUBLE-backed columns.
                    Some(SimpleType::Number)
                }
            } else {
                Some(SimpleType::Number)
            }
        }
        Value::String(_) => Some(SimpleType::String),
        Value::Object(_) => Some(SimpleType::Object),
        Value::Array(_) => Some(SimpleType::Array),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PathKind {
    Object,
    Array,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathSegment {
    name: String,
    kind: PathKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TablePath {
    segments: Vec<PathSegment>,
}

fn encode_path_component(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        let ch = byte as char;
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

impl TablePath {
    fn root() -> Self {
        Self { segments: Vec::new() }
    }

    fn child_object(&self, segment: &str) -> Self {
        let mut segments = self.segments.clone();
        segments.push(PathSegment {
            name: segment.to_owned(),
            kind: PathKind::Object,
        });
        Self { segments }
    }

    fn child_array(&self, segment: &str) -> Self {
        let mut segments = self.segments.clone();
        segments.push(PathSegment {
            name: segment.to_owned(),
            kind: PathKind::Array,
        });
        Self { segments }
    }

    fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    fn parent(&self) -> Option<Self> {
        if self.segments.is_empty() {
            None
        } else {
            let mut segments = self.segments.clone();
            segments.pop();
            Some(Self { segments })
        }
    }

    fn file_suffix(&self) -> Option<String> {
        if self.segments.is_empty() {
            None
        } else {
            let parts: Vec<String> = self
                .segments
                .iter()
                .map(|seg| match seg.kind {
                    PathKind::Object => encode_path_component(&seg.name),
                    PathKind::Array => format!("{}[]", encode_path_component(&seg.name)),
                })
                .collect();
            Some(parts.join("."))
        }
    }
}

impl fmt::Display for TablePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            write!(f, "root")
        } else {
            let parts: Vec<String> = self
                .segments
                .iter()
                .map(|seg| match seg.kind {
                    PathKind::Object => seg.name.clone(),
                    PathKind::Array => format!("{}[]", seg.name),
                })
                .collect();
            write!(f, "{}", parts.join("."))
        }
    }
}

#[derive(Debug)]
struct TableStats {
    path: TablePath,
    stats: PropertyStats,
    kind: PathKind,
    has_nested_array: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // some metadata fields are kept for future use
struct PlannedTable {
    path: TablePath,
    kind: PathKind,
    columns: Vec<ColumnPlan>,
    properties: HashMap<String, PropertyColumns>,
    has_nested_array: bool,
}

fn build_all_schema_plans(table_stats: &[TableStats]) -> Vec<PlannedTable> {
    table_stats
        .iter()
        .map(|t| {
            let needs_child_id = t.kind == PathKind::Array && t.has_nested_array;
            let include_id = match t.kind {
                PathKind::Object => true,
                PathKind::Array => needs_child_id,
            };
            let (columns, properties) =
                build_schema_plan(&t.stats, include_id, t.kind == PathKind::Array);
            PlannedTable {
                path: t.path.clone(),
                kind: t.kind.clone(),
                columns,
                properties,
                has_nested_array: t.has_nested_array,
            }
        })
        .collect()
}

fn build_table_writers(plans: &[PlannedTable]) -> HashMap<TablePath, TableWriter> {
    plans
        .iter()
        .map(|p| (p.path.clone(), TableWriter::new(p.clone())))
        .collect()
}

fn scan_all_stats(path: &Path, format: InputFormat) -> Result<Vec<TableStats>, Box<dyn Error>> {
    let mut tables: HashMap<TablePath, TableStats> = HashMap::new();

    for_each_object(path, format, |_, obj| {
        accumulate_object_stats(&mut tables, &TablePath::root(), obj);
        Ok(())
    })?;

    let mut result: Vec<TableStats> = tables
        .into_iter()
        .map(|(_, stats)| stats)
        .collect();
    result.sort_by(|a, b| a.path.to_string().cmp(&b.path.to_string()));
    Ok(result)
}

fn get_or_create_table<'a>(
    tables: &'a mut HashMap<TablePath, TableStats>,
    path: &TablePath,
) -> &'a mut TableStats {
    tables
        .entry(path.clone())
        .or_insert_with(|| TableStats {
            path: path.clone(),
            stats: PropertyStats::default(),
            kind: if path.is_root() {
                PathKind::Object
            } else {
                path.segments.last().map(|s| s.kind.clone()).unwrap_or(PathKind::Object)
            },
            has_nested_array: false,
        })
}

fn accumulate_object_stats(
    tables: &mut HashMap<TablePath, TableStats>,
    path: &TablePath,
    obj: &serde_json::Map<String, Value>,
) {
    let mut current = get_or_create_table(tables, path);

    for (name, value) in obj {
        match value {
            Value::Object(map) => {
                current.stats.record_value(name, value);
                let child_path = path.child_object(name);
                let _ = current;
                accumulate_object_stats(tables, &child_path, map);
                current = get_or_create_table(tables, path);
            }
            Value::Array(arr) => {
                current.stats.record_value(name, value);
                let child_path = path.child_array(name);
                let _ = current;
                accumulate_array_stats(tables, &child_path, arr);
                current = get_or_create_table(tables, path);
            }
            _ => {
                current.stats.record_value(name, value);
            }
        }
    }
}

fn accumulate_array_stats(
    tables: &mut HashMap<TablePath, TableStats>,
    path: &TablePath,
    arr: &[Value],
) {
    let mut current = get_or_create_table(tables, path);
    current.kind = PathKind::Array;

    for value in arr {
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    match v {
                        Value::Object(child) => {
                            current.stats.record_value(k, v);
                            let child_path = path.child_object(k);
                            let _ = current;
                            accumulate_object_stats(tables, &child_path, child);
                            current = get_or_create_table(tables, path);
                        }
                        Value::Array(child_arr) => {
                            current.has_nested_array = true;
                            current.stats.record_value(k, v);
                            let child_path = path.child_array(k);
                            let _ = current;
                            accumulate_array_stats(tables, &child_path, child_arr);
                            current = get_or_create_table(tables, path);
                        }
                        _ => {
                            current.stats.record_value(k, v);
                        }
                    }
                }
            }
            Value::Array(child_arr) => {
                current.has_nested_array = true;
                current.stats.record_value("value", value);
                let child_path = path.child_array("value");
                let _ = current;
                accumulate_array_stats(tables, &child_path, child_arr);
                current = get_or_create_table(tables, path);
            }
            _ => {
                current.stats.record_value("value", value);
            }
        }
    }
}

#[derive(Debug)]
struct TableWriter {
    plan: PlannedTable,
    columns: HashMap<String, ColumnValues>,
    next_id: i64,
    row_count: usize,
}

impl TableWriter {
    fn new(plan: PlannedTable) -> Self {
        let mut columns = HashMap::new();
        for col in &plan.columns {
            columns.insert(
                col.name.clone(),
                ColumnValues::new(col.ty, col.is_null_mask),
            );
        }
        Self {
            plan,
            columns,
            next_id: 1,
            row_count: 0,
        }
    }

    fn push_row(&mut self) -> (usize, Option<i64>) {
        for col in self.columns.values_mut() {
            col.push_default();
        }
        let row_idx = self.row_count;
        self.row_count += 1;

        let mut assigned_id = None;
        if let Some(pk_col) = self
            .plan
            .columns
            .iter()
            .find(|c| c.name == "_id" && c.is_required)
        {
            let id = self.next_id;
            self.next_id += 1;
            if let Some(col) = self.columns.get_mut(&pk_col.name) {
                col.set_int(row_idx, id);
            }
            assigned_id = Some(id);
        }

        (row_idx, assigned_id)
    }
}

fn detect_input_format(path: &Path) -> Result<InputFormat, Box<dyn Error>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Peek first non-whitespace byte to decide between JSON array and NDJSON.
    let mut first_non_ws: Option<u8> = None;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            break;
        }
        for byte in buffer {
            if !byte.is_ascii_whitespace() {
                first_non_ws = Some(*byte);
                break;
            }
        }
        if first_non_ws.is_some() {
            break;
        }
        let consumed = buffer.len();
        reader.consume(consumed);
    }

    match first_non_ws {
        Some(b'[') => Ok(InputFormat::Array),
        Some(_) => Ok(InputFormat::Lines),
        None => Err("Input file is empty".into()),
    }
}

fn for_each_object<F>(path: &Path, format: InputFormat, mut f: F) -> Result<(), Box<dyn Error>>
where
    F: FnMut(usize, &serde_json::Map<String, Value>) -> Result<(), Box<dyn Error>>,
{
    match format {
        InputFormat::Array => {
            let reader = BufReader::new(File::open(path)?);
            let payload: Value = serde_json::from_reader(reader)?;
            let entries = payload.as_array().ok_or("Expected top-level JSON array")?;

            for (idx, entry) in entries.iter().enumerate() {
                let obj = entry
                    .as_object()
                    .ok_or_else(|| format!("Entry at index {idx} is not an object"))?;
                f(idx, obj)?;
            }
        }
        InputFormat::Lines => {
            let reader = BufReader::new(File::open(path)?);
            for (line_num, line) in reader.lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(&line)
                    .map_err(|e| format!("Line {}: {}", line_num + 1, e))?;
                let obj = value
                    .as_object()
                    .ok_or_else(|| format!("Line {} is not an object", line_num + 1))?;
                f(line_num, obj)?;
            }
        }
    }

    Ok(())
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

fn build_schema_plan(
    stats: &PropertyStats,
    include_id: bool,
    is_array_table: bool,
) -> (Vec<ColumnPlan>, HashMap<String, PropertyColumns>) {
    // Heuristics:
    // 1) Single observed non-null type -> main column with original name.
    // 2) Multiple non-null types -> most frequent becomes main column; others become "<name>|<type>" alternates.
    // 3) If explicit nulls are observed alongside non-null values -> add "<name>|n" bool column to mark explicit nulls.
    //    In JSON there is a distinction between non-existant values and explict nulls, so we need to record this.
    // 4) If both integer and number are observed for a property, merge them into a single number column.
    // Nested/array values are dropped earlier in `classify_value` (objects only trigger subtable creation).
    let mut per_property: HashMap<String, HashMap<SimpleType, usize>> = HashMap::new();
    for (key, count) in &stats.counts {
        per_property
            .entry(key.name.clone())
            .or_default()
            .insert(key.ty, *count);
    }

    let mut columns = Vec::new();
    let mut properties: HashMap<String, PropertyColumns> = HashMap::new();

    // Add identifiers first: for array tables, include required _parent/_pos, and optional _id if requested.
    if include_id {
        columns.push(ColumnPlan {
            name: "_id".to_string(),
            ty: SimpleType::Integer,
            is_null_mask: false,
            is_required: true,
            _kind: ColumnKind::Primary {
                property: "_id".to_string(),
                main_type: SimpleType::Integer,
            },
        });
    }
    if is_array_table {
        columns.push(ColumnPlan {
            name: "_parent".to_string(),
            ty: SimpleType::Integer,
            is_null_mask: false,
            is_required: true,
            _kind: ColumnKind::Primary {
                property: "_parent".to_string(),
                main_type: SimpleType::Integer,
            },
        });
        columns.push(ColumnPlan {
            name: "_pos".to_string(),
            ty: SimpleType::Integer,
            is_null_mask: false,
            is_required: true,
            _kind: ColumnKind::Primary {
                property: "_pos".to_string(),
                main_type: SimpleType::Integer,
            },
        });
    }

    // Respect first-seen property order as best we can (PK already added first).
    let mut properties_in_order: Vec<(String, HashMap<SimpleType, usize>)> = Vec::new();
    for name in &stats.order {
        if let Some(counts) = per_property.remove(name) {
            properties_in_order.push((name.clone(), counts));
        }
    }
    // Any remaining (unlikely) go at the end in name order for determinism.
    let mut remaining: Vec<_> = per_property.into_iter().collect();
    remaining.sort_by(|a, b| a.0.cmp(&b.0));
    properties_in_order.extend(remaining);

    for (property, mut type_counts) in properties_in_order {
        let base_name = if is_array_table && property == "value" {
            "_value".to_string()
        } else {
            property.clone()
        };
        let object_count = type_counts.remove(&SimpleType::Object).unwrap_or(0);
        let array_count = type_counts.remove(&SimpleType::Array).unwrap_or(0);
        // Merge integer + number into number without losing either count.
        if type_counts.contains_key(&SimpleType::Integer) && type_counts.contains_key(&SimpleType::Number) {
            let merged = type_counts[&SimpleType::Integer] + type_counts[&SimpleType::Number];
            type_counts.insert(SimpleType::Number, merged);
            type_counts.remove(&SimpleType::Integer);
        }

        let null_count = type_counts.get(&SimpleType::Null).copied().unwrap_or(0);
        let mut prop_columns = PropertyColumns {
            array_count: None,
            ..Default::default()
        };

        let mut has_any = false;

        if object_count > 0 {
            let object_col = format!("{base_name}|object");
            columns.push(ColumnPlan {
                name: object_col.clone(),
                ty: SimpleType::Integer,
                is_null_mask: false,
                is_required: false,
                _kind: ColumnKind::Primary {
                    property: property.clone(),
                    main_type: SimpleType::Integer,
                },
            });
            prop_columns.object_fk = Some(object_col);
            has_any = true;
        }

        if array_count > 0 {
            let primary_name = format!("{base_name}|array");
            columns.push(ColumnPlan {
                name: primary_name.clone(),
                ty: SimpleType::Integer,
                is_null_mask: false,
                is_required: false,
                _kind: ColumnKind::Primary {
                    property: property.clone(),
                    main_type: SimpleType::Integer,
                },
            });

            prop_columns.array_count = Some(primary_name);
            has_any = true;
        }

        // Exclude null when choosing a main type; if only nulls exist, skip this property.
        let mut typed: Vec<(SimpleType, usize)> = type_counts
            .iter()
            .filter(|(ty, _)| **ty != SimpleType::Null && **ty != SimpleType::Object)
            .map(|(ty, count)| (*ty, *count))
            .collect();

        if !typed.is_empty() {
            typed.sort_by(|(ty_a, count_a), (ty_b, count_b)| {
                count_b.cmp(count_a).then_with(|| ty_a.cmp(ty_b))
            });

            let main_type = typed[0].0;

            columns.push(ColumnPlan {
                name: base_name.clone(),
                ty: main_type,
                is_null_mask: false,
                is_required: false,
                _kind: ColumnKind::Primary {
                    property: property.clone(),
                    main_type,
                },
            });

            prop_columns.main_type = Some(main_type);
            prop_columns.primary = Some(base_name.clone());
            has_any = true;

            for (ty, _) in typed.into_iter().skip(1) {
                let alt_name = format!("{base_name}|{ty}");
                columns.push(ColumnPlan {
                    name: alt_name.clone(),
                    ty,
                    is_null_mask: false,
                    is_required: false,
                    _kind: ColumnKind::Alternate {
                        property: property.clone(),
                        source_ty: ty,
                    },
                });
                prop_columns.alternates.insert(ty, alt_name);
            }
        }

        if null_count > 0 && has_any {
            let null_mask_name = format!("{base_name}|n");
            columns.push(ColumnPlan {
                name: null_mask_name.clone(),
                ty: SimpleType::Bool,
                is_null_mask: true,
                is_required: false,
                _kind: ColumnKind::NullBitmask {
                    property: property.clone(),
                },
            });
            prop_columns.null_mask = Some(null_mask_name);
        }

        if has_any {
            properties.insert(property, prop_columns);
        }
    }

    (columns, properties)
}

#[derive(Debug)]
enum ColumnValues {
    Bool(Vec<Option<bool>>),
    BoolMask(Vec<bool>), // required bool, defaults to false
    Int(Vec<Option<i64>>),
    Double(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl ColumnValues {
    fn new(ty: SimpleType, is_null_mask: bool) -> Self {
        if is_null_mask {
            return ColumnValues::BoolMask(Vec::new());
        }
        match ty {
            SimpleType::Bool => ColumnValues::Bool(Vec::new()),
            SimpleType::Integer => ColumnValues::Int(Vec::new()),
            SimpleType::Number => ColumnValues::Double(Vec::new()),
            SimpleType::String => ColumnValues::Str(Vec::new()),
            SimpleType::Null => ColumnValues::Bool(Vec::new()), // not used
            SimpleType::Object => ColumnValues::Bool(Vec::new()), // not used
            SimpleType::Array => ColumnValues::Bool(Vec::new()), // not used
        }
    }

    fn push_default(&mut self) {
        match self {
            ColumnValues::Bool(v) => v.push(None),
            ColumnValues::BoolMask(v) => v.push(false),
            ColumnValues::Int(v) => v.push(None),
            ColumnValues::Double(v) => v.push(None),
            ColumnValues::Str(v) => v.push(None),
        }
    }

    fn set_bool(&mut self, idx: usize, value: bool) {
        match self {
            ColumnValues::Bool(v) => v[idx] = Some(value),
            ColumnValues::BoolMask(v) => v[idx] = value,
            _ => {}
        }
    }

    fn set_int(&mut self, idx: usize, value: i64) {
        match self {
            ColumnValues::Int(v) => v[idx] = Some(value),
            _ => {}
        }
    }

    fn set_double(&mut self, idx: usize, value: f64) {
        match self {
            ColumnValues::Double(v) => v[idx] = Some(value),
            _ => {}
        }
    }

    fn set_str(&mut self, idx: usize, value: String) {
        match self {
            ColumnValues::Str(v) => v[idx] = Some(value),
            _ => {}
        }
    }
}

fn write_parquet(
    table: &TableWriter,
    output_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let columns = &table.plan.columns;
    let column_data = &table.columns;

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

            let values = column_data
                .get(&column.name)
                .ok_or_else(|| format!("No data for column {}", column.name))?;
            write_column_values(&mut col_writer, values)?;
            col_writer.close()?;
        }

        row_group.close()?;
    }

    writer.close()?;
    Ok(())
}

fn set_column_value(
    data: &mut HashMap<String, ColumnValues>,
    column_name: &str,
    ty: SimpleType,
    value: &Value,
    row_idx: usize,
) -> Result<(), Box<dyn Error>> {
    let col = data
        .get_mut(column_name)
        .ok_or_else(|| format!("Missing column data for {}", column_name))?;

    match ty {
        SimpleType::Bool => {
            let v = value
                .as_bool()
                .ok_or_else(|| format!("Expected bool for {}", column_name))?;
            col.set_bool(row_idx, v);
        }
        SimpleType::Integer => {
            let v = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|u| i64::try_from(u).ok()))
                .ok_or_else(|| format!("Expected integer for {}", column_name))?;
            col.set_int(row_idx, v);
        }
        SimpleType::Number => {
            let v = value
                .as_f64()
                .ok_or_else(|| format!("Expected number for {}", column_name))?;
            col.set_double(row_idx, v);
        }
        SimpleType::String => {
            let v = value
                .as_str()
                .ok_or_else(|| format!("Expected string for {}", column_name))?;
            col.set_str(row_idx, v.to_owned());
        }
        SimpleType::Null | SimpleType::Object | SimpleType::Array => {}
    }

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

fn write_all_tables(
    input_path: &Path,
    format: InputFormat,
    planned_tables: &[PlannedTable],
    output_dir: &Path,
    stem: &str,
) -> Result<Vec<TableFile>, Box<dyn Error>> {
    let mut writers = build_table_writers(planned_tables);
    let mut table_files = Vec::new();

    // Populate rows recursively starting from root table.
    for_each_object(input_path, format, |_, obj| {
        process_object(&TablePath::root(), obj, &mut writers)?;
        Ok(())
    })?;

    // Write each table to disk.
    for (path, writer) in writers.iter() {
        let file_name = match path.file_suffix() {
            None => format!("{stem}.parquet"),
            Some(suffix) => format!("{stem}.{suffix}.parquet"),
        };
        let output_path = output_dir.join(file_name);
        write_parquet(writer, &output_path)?;
        table_files.push(TableFile {
            path: path.clone(),
            file_path: output_path.clone(),
        });
        println!(
            "Wrote Parquet file for table {} to {:?}",
            path,
            output_path
        );
        let _ = log_parquet_summary(&output_path);
    }

    Ok(table_files)
}

#[derive(Clone, Copy)]
enum MissingArrayParentIdPolicy {
    Ignore,
    Error,
}

fn get_property_plan(
    writers: &HashMap<TablePath, TableWriter>,
    path: &TablePath,
    property: &str,
) -> Result<Option<PropertyColumns>, Box<dyn Error>> {
    let writer = writers
        .get(path)
        .ok_or_else(|| format!("Missing writer for path {}", path))?;
    Ok(writer.plan.properties.get(property).cloned())
}

fn set_bool_column_if_exists(
    writers: &mut HashMap<TablePath, TableWriter>,
    path: &TablePath,
    column_name: &str,
    row_idx: usize,
    value: bool,
) -> Result<(), Box<dyn Error>> {
    let writer = writers
        .get_mut(path)
        .ok_or_else(|| format!("Missing writer for path {}", path))?;
    if let Some(col) = writer.columns.get_mut(column_name) {
        col.set_bool(row_idx, value);
    }
    Ok(())
}

fn set_int_column_if_exists(
    writers: &mut HashMap<TablePath, TableWriter>,
    path: &TablePath,
    column_name: &str,
    row_idx: usize,
    value: i64,
) -> Result<(), Box<dyn Error>> {
    let writer = writers
        .get_mut(path)
        .ok_or_else(|| format!("Missing writer for path {}", path))?;
    if let Some(col) = writer.columns.get_mut(column_name) {
        col.set_int(row_idx, value);
    }
    Ok(())
}

fn write_scalar_with_plan(
    writers: &mut HashMap<TablePath, TableWriter>,
    path: &TablePath,
    plan: &PropertyColumns,
    value: &Value,
    row_idx: usize,
) -> Result<(), Box<dyn Error>> {
    let Some(mut ty) = classify_value(value) else {
        return Ok(());
    };
    if ty == SimpleType::Integer && plan.main_type == Some(SimpleType::Number) {
        ty = SimpleType::Number;
    }

    let writer = writers
        .get_mut(path)
        .ok_or_else(|| format!("Missing writer for path {}", path))?;
    if let Some(primary_name) = &plan.primary {
        if Some(ty) == plan.main_type {
            set_column_value(&mut writer.columns, primary_name, ty, value, row_idx)?;
            return Ok(());
        }
    }

    if let Some(alt_name) = plan.alternates.get(&ty) {
        set_column_value(&mut writer.columns, alt_name, ty, value, row_idx)?;
    }
    Ok(())
}

fn process_property_value(
    path: &TablePath,
    property: &str,
    value: &Value,
    prop_plan: &PropertyColumns,
    row_idx: usize,
    row_id: Option<i64>,
    missing_array_parent_id: MissingArrayParentIdPolicy,
    writers: &mut HashMap<TablePath, TableWriter>,
) -> Result<(), Box<dyn Error>> {
    match value {
        Value::Null => {
            if let Some(mask_name) = prop_plan.null_mask.as_deref() {
                set_bool_column_if_exists(writers, path, mask_name, row_idx, true)?;
            }
        }
        Value::Object(map) => {
            let child_path = path.child_object(property);
            let child_id = process_object(&child_path, map, writers)?;
            if let (Some(fk_col), Some(id)) = (prop_plan.object_fk.as_deref(), child_id) {
                set_int_column_if_exists(writers, path, fk_col, row_idx, id)?;
            }
        }
        Value::Array(arr) => {
            let count_col = prop_plan
                .array_count
                .as_ref()
                .or_else(|| prop_plan.primary.as_ref());
            if let Some(count_col) = count_col {
                set_int_column_if_exists(writers, path, count_col, row_idx, arr.len() as i64)?;
            }

            if let Some(parent_id) = row_id {
                let child_path = path.child_array(property);
                for (idx, elem) in arr.iter().enumerate() {
                    process_array_elem(&child_path, elem, parent_id, idx as i64, writers)?;
                }
            } else if matches!(missing_array_parent_id, MissingArrayParentIdPolicy::Error) {
                return Err(format!("Table {} requires _id to link nested arrays", path).into());
            }
        }
        _ => {
            write_scalar_with_plan(writers, path, prop_plan, value, row_idx)?;
        }
    }

    Ok(())
}

fn process_object(
    path: &TablePath,
    obj: &serde_json::Map<String, Value>,
    writers: &mut HashMap<TablePath, TableWriter>,
) -> Result<Option<i64>, Box<dyn Error>> {
    let (row_idx, assigned_id) = {
        let writer = writers
            .get_mut(path)
            .ok_or_else(|| format!("Missing writer for path {}", path))?;
        writer.push_row()
    };

    for (name, value) in obj {
        let Some(prop_plan) = get_property_plan(writers, path, name)? else {
            continue;
        };
        process_property_value(
            path,
            name,
            value,
            &prop_plan,
            row_idx,
            assigned_id,
            MissingArrayParentIdPolicy::Ignore,
            writers,
        )?;
    }

    Ok(assigned_id)
}

fn process_array_elem(
    path: &TablePath,
    value: &Value,
    parent_id: i64,
    pos: i64,
    writers: &mut HashMap<TablePath, TableWriter>,
) -> Result<Option<i64>, Box<dyn Error>> {
    let (row_idx, assigned_id) = {
        let writer = writers
            .get_mut(path)
            .ok_or_else(|| format!("Missing writer for path {}", path))?;
        let res = writer.push_row();
        if let Some(col) = writer.columns.get_mut("_parent") {
            col.set_int(res.0, parent_id);
        }
        if let Some(col) = writer.columns.get_mut("_pos") {
            col.set_int(res.0, pos);
        }
        res
    };

    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let Some(prop_plan) = get_property_plan(writers, path, k)? else {
                    continue;
                };
                process_property_value(
                    path,
                    k,
                    v,
                    &prop_plan,
                    row_idx,
                    assigned_id,
                    MissingArrayParentIdPolicy::Error,
                    writers,
                )?;
            }
        }
        _ => {
            if let Some(value_plan) = get_property_plan(writers, path, "value")? {
                process_property_value(
                    path,
                    "value",
                    value,
                    &value_plan,
                    row_idx,
                    assigned_id,
                    MissingArrayParentIdPolicy::Ignore,
                    writers,
                )?;
            }
        }
    }

    Ok(assigned_id)
}

fn write_column_values(
    writer: &mut SerializedColumnWriter<'_>,
    values: &ColumnValues,
) -> Result<(), Box<dyn Error>> {
    let untyped = writer.untyped();
    match (untyped, values) {
        (ColumnWriter::BoolColumnWriter(ref mut w), ColumnValues::Bool(v)) => {
            // Definition levels mark which rows are present (1) vs null (0); data only includes present values.
            let def_levels: Vec<i16> = v.iter().map(|val| if val.is_some() { 1 } else { 0 }).collect();
            let data: Vec<bool> = v.iter().filter_map(|val| *val).collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        (ColumnWriter::BoolColumnWriter(ref mut w), ColumnValues::BoolMask(v)) => {
            // Required bool: no definition levels, all rows present.
            w.write_batch(v, None, None)?;
        }
        (ColumnWriter::Int64ColumnWriter(ref mut w), ColumnValues::Int(v)) => {
            let def_levels: Vec<i16> = v.iter().map(|val| if val.is_some() { 1 } else { 0 }).collect();
            let data: Vec<i64> = v.iter().filter_map(|val| *val).collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        (ColumnWriter::DoubleColumnWriter(ref mut w), ColumnValues::Double(v)) => {
            let def_levels: Vec<i16> = v.iter().map(|val| if val.is_some() { 1 } else { 0 }).collect();
            let data: Vec<f64> = v.iter().filter_map(|val| *val).collect();
            w.write_batch(&data, Some(&def_levels), None)?;
        }
        (ColumnWriter::ByteArrayColumnWriter(ref mut w), ColumnValues::Str(v)) => {
            let def_levels: Vec<i16> = v.iter().map(|val| if val.is_some() { 1 } else { 0 }).collect();
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

//! The source manifest and the provenance comments that travel with it.

use std::collections::HashMap;

use serde_json::{json, Value};

use crate::contract::{
    column_type_metadata, sanitize_ident, table_raw_name, PlannedTable, TablePath,
};

/// Where a loaded family came from. Stamped onto every table as a
/// `COPY provenance {...}` comment and copied onto the public wrapper view.
#[derive(Debug, Clone)]
pub struct Provenance<'a> {
    /// The source locator: a file path, an S3 URI, a table reference.
    pub source: &'a str,
    /// The kind of source connection, e.g. `local-file` or `s3`.
    pub source_connection: &'a str,
    /// RFC 3339 timestamp for when the load ran.
    pub imported_at: &'a str,
    /// RFC 3339 timestamp of the source's own last modification, when known.
    pub source_modified_at: Option<&'a str>,
}

impl<'a> Provenance<'a> {
    /// Provenance for a file read from the machine running the loader.
    pub fn local_file(source: &'a str, imported_at: &'a str) -> Self {
        Self {
            source,
            source_connection: "local-file",
            imported_at,
            source_modified_at: None,
        }
    }

    pub fn with_source_modified_at(mut self, modified_at: Option<&'a str>) -> Self {
        self.source_modified_at = modified_at;
        self
    }
}

/// One `(table_name, comment)` pair per planned table.
pub fn build_provenance_comments(
    plans: &[PlannedTable],
    stem: &str,
    provenance: &Provenance<'_>,
) -> Vec<(String, String)> {
    plans
        .iter()
        .map(|plan| {
            let mut fields = json!({
                "source": provenance.source,
                "sourceConnection": provenance.source_connection,
                "importedAt": provenance.imported_at,
                "tablePath": plan.path.to_string(),
                "tool": "exasol-json-tables",
            });
            if let Some(modified_at) = provenance.source_modified_at {
                fields["sourceModifiedAt"] = Value::String(modified_at.to_string());
            }
            (
                table_raw_name(&plan.path, stem),
                format!("COPY provenance {fields}"),
            )
        })
        .collect()
}

/// The `COMMENT ON TABLE` statement for one provenance comment.
pub fn provenance_comment_statement(table_name: &str, comment: &str) -> String {
    format!(
        "COMMENT ON TABLE {} IS '{}';",
        sanitize_ident(table_name),
        comment.replace('\'', "''")
    )
}

/// Build the source manifest describing the planned family.
pub fn build_source_manifest(plans: &[PlannedTable], stem: &str) -> Value {
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
                "relationKind": segment.kind.label(),
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
            let path_segments: Vec<Value> = plan
                .path
                .segments
                .iter()
                .map(|segment| {
                    json!({
                        "name": segment.name,
                        "kind": segment.kind.label(),
                    })
                })
                .collect();
            let columns: Vec<Value> = plan
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
                "kind": plan.kind.label(),
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

/// Copy provenance comments onto the manifest's table entries.
pub fn apply_table_comments(manifest: &mut Value, comments: &[(String, String)]) {
    let comments_by_table: HashMap<&str, &str> = comments
        .iter()
        .map(|(table_name, comment)| (table_name.as_str(), comment.as_str()))
        .collect();
    if let Some(tables) = manifest["tables"].as_array_mut() {
        for table in tables {
            let Some(table_name) = table["tableName"].as_str() else {
                continue;
            };
            if let Some(comment) = comments_by_table.get(table_name) {
                table["tableComment"] = Value::String((*comment).to_string());
            }
        }
    }
}

//! The plan as a wire format.
//!
//! The driver infers the table family once and passes the plan to each loader
//! statement, so every pass writes against **identical** columns and assigns
//! identical identities. Re-inferring per pass would also work — inference is
//! deterministic — but it would repeat the scan and, worse, make the loads
//! silently depend on the source not changing underneath them.
//!
//! The format is also the seam an explicit, reviewed plan would arrive through.

use std::collections::HashMap;

use exasol_udf_sdk::error::UdfError;
use json_tables_core::contract::{
    ColumnKind, ColumnPlan, PathKind, PathSegment, PlannedTable, PropertyColumns, SimpleType,
    TablePath,
};
use serde_json::{json, Map, Value};

pub const PLAN_FORMAT: &str = "exasol-json-tables-plan";
pub const PLAN_VERSION: u64 = 1;

/// Serialise a planned family.
pub fn encode(plans: &[PlannedTable], stem: &str) -> String {
    let tables: Vec<Value> = plans.iter().map(encode_table).collect();
    json!({
        "format": PLAN_FORMAT,
        "version": PLAN_VERSION,
        "stem": stem,
        "tables": tables,
    })
    .to_string()
}

/// Parse a planned family, returning the plans and the stem they were built for.
pub fn decode(text: &str) -> Result<(Vec<PlannedTable>, String), UdfError> {
    let value: Value =
        serde_json::from_str(text).map_err(|err| user(format!("plan is not valid JSON: {err}")))?;

    let format = value["format"].as_str().unwrap_or_default();
    if format != PLAN_FORMAT {
        return Err(user(format!(
            "plan format '{format}' is not '{PLAN_FORMAT}'"
        )));
    }
    let version = value["version"].as_u64().unwrap_or_default();
    if version != PLAN_VERSION {
        return Err(user(format!(
            "plan version {version} is not supported (expected {PLAN_VERSION})"
        )));
    }
    let stem = value["stem"]
        .as_str()
        .ok_or_else(|| user("plan is missing its stem"))?
        .to_string();
    let tables = value["tables"]
        .as_array()
        .ok_or_else(|| user("plan is missing its tables"))?;

    let plans = tables
        .iter()
        .map(decode_table)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((plans, stem))
}

fn encode_table(plan: &PlannedTable) -> Value {
    let path: Vec<Value> = plan
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
        .map(|column| {
            json!({
                "name": column.name,
                "type": column.ty.to_string(),
                "isNullMask": column.is_null_mask,
                "isRequired": column.is_required,
                "kind": encode_column_kind(&column.kind),
            })
        })
        .collect();

    let mut property_names: Vec<&String> = plan.properties.keys().collect();
    property_names.sort();
    let mut properties = Map::new();
    for name in property_names {
        let columns = &plan.properties[name];
        let mut alternates = Map::new();
        let mut alternate_types: Vec<&SimpleType> = columns.alternates.keys().collect();
        alternate_types.sort();
        for ty in alternate_types {
            alternates.insert(
                ty.to_string(),
                Value::String(columns.alternates[ty].clone()),
            );
        }
        properties.insert(
            name.clone(),
            json!({
                "mainType": columns.main_type.map(|ty| ty.to_string()),
                "primary": columns.primary,
                "objectFk": columns.object_fk,
                "nullMask": columns.null_mask,
                "arrayCount": columns.array_count,
                "alternates": Value::Object(alternates),
            }),
        );
    }

    json!({
        "path": path,
        "kind": plan.kind.label(),
        "hasNestedArray": plan.has_nested_array,
        "columns": columns,
        "properties": Value::Object(properties),
    })
}

fn decode_table(value: &Value) -> Result<PlannedTable, UdfError> {
    let segments = value["path"]
        .as_array()
        .ok_or_else(|| user("table is missing its path"))?
        .iter()
        .map(|segment| {
            Ok(PathSegment {
                name: segment["name"]
                    .as_str()
                    .ok_or_else(|| user("path segment is missing its name"))?
                    .to_string(),
                kind: decode_path_kind(segment["kind"].as_str().unwrap_or_default())?,
            })
        })
        .collect::<Result<Vec<_>, UdfError>>()?;

    let columns = value["columns"]
        .as_array()
        .ok_or_else(|| user("table is missing its columns"))?
        .iter()
        .map(decode_column)
        .collect::<Result<Vec<_>, UdfError>>()?;

    let mut properties = HashMap::new();
    if let Some(entries) = value["properties"].as_object() {
        for (name, columns) in entries {
            properties.insert(name.clone(), decode_property_columns(columns)?);
        }
    }

    Ok(PlannedTable {
        path: TablePath { segments },
        kind: decode_path_kind(value["kind"].as_str().unwrap_or_default())?,
        columns,
        properties,
        has_nested_array: value["hasNestedArray"].as_bool().unwrap_or(false),
    })
}

fn decode_column(value: &Value) -> Result<ColumnPlan, UdfError> {
    let name = value["name"]
        .as_str()
        .ok_or_else(|| user("column is missing its name"))?
        .to_string();
    let ty = decode_simple_type(value["type"].as_str().unwrap_or_default())?;
    Ok(ColumnPlan {
        kind: decode_column_kind(&value["kind"], &name, ty)?,
        name,
        ty,
        is_null_mask: value["isNullMask"].as_bool().unwrap_or(false),
        is_required: value["isRequired"].as_bool().unwrap_or(false),
    })
}

fn decode_property_columns(value: &Value) -> Result<PropertyColumns, UdfError> {
    let mut alternates = HashMap::new();
    if let Some(entries) = value["alternates"].as_object() {
        for (ty, column) in entries {
            alternates.insert(
                decode_simple_type(ty)?,
                column
                    .as_str()
                    .ok_or_else(|| user("alternate column must be a string"))?
                    .to_string(),
            );
        }
    }
    Ok(PropertyColumns {
        main_type: match value["mainType"].as_str() {
            Some(ty) => Some(decode_simple_type(ty)?),
            None => None,
        },
        primary: value["primary"].as_str().map(str::to_string),
        object_fk: value["objectFk"].as_str().map(str::to_string),
        null_mask: value["nullMask"].as_str().map(str::to_string),
        array_count: value["arrayCount"].as_str().map(str::to_string),
        alternates,
    })
}

fn encode_column_kind(kind: &ColumnKind) -> Value {
    match kind {
        ColumnKind::Primary {
            property,
            main_type,
        } => json!({"kind": "primary", "property": property, "mainType": main_type.to_string()}),
        ColumnKind::Alternate {
            property,
            source_ty,
        } => {
            json!({"kind": "alternate", "property": property, "sourceType": source_ty.to_string()})
        }
        ColumnKind::NullBitmask { property } => json!({"kind": "nullMask", "property": property}),
    }
}

fn decode_column_kind(value: &Value, name: &str, ty: SimpleType) -> Result<ColumnKind, UdfError> {
    let property = value["property"].as_str().unwrap_or(name).to_string();
    match value["kind"].as_str() {
        Some("alternate") => Ok(ColumnKind::Alternate {
            property,
            source_ty: decode_simple_type(value["sourceType"].as_str().unwrap_or_default())?,
        }),
        Some("nullMask") => Ok(ColumnKind::NullBitmask { property }),
        // Absent or "primary": the structural default.
        _ => Ok(ColumnKind::Primary {
            property,
            main_type: match value["mainType"].as_str() {
                Some(main) => decode_simple_type(main)?,
                None => ty,
            },
        }),
    }
}

fn decode_path_kind(label: &str) -> Result<PathKind, UdfError> {
    match label {
        "object" => Ok(PathKind::Object),
        "array" => Ok(PathKind::Array),
        other => Err(user(format!("unknown path kind '{other}'"))),
    }
}

fn decode_simple_type(label: &str) -> Result<SimpleType, UdfError> {
    match label {
        "null" => Ok(SimpleType::Null),
        "bool" => Ok(SimpleType::Bool),
        "integer" => Ok(SimpleType::Integer),
        "number" => Ok(SimpleType::Number),
        "string" => Ok(SimpleType::String),
        "object" => Ok(SimpleType::Object),
        "array" => Ok(SimpleType::Array),
        other => Err(user(format!("unknown column type '{other}'"))),
    }
}

fn user(message: impl Into<String>) -> UdfError {
    UdfError::User(message.into())
}

#[cfg(test)]
#[path = "plan_wire_tests.rs"]
mod tests;

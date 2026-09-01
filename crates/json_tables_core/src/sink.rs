//! Pass 2: walk documents against a plan and hand rows to a [`RowSink`].
//!
//! The traversal owns the contract semantics — identity assignment, `_parent` /
//! `_pos` linkage, explicit-null masks, variant column selection — and knows
//! nothing about where the rows go. That is what lets the same code back a
//! Parquet writer, a CSV stream, or a UDF `EMITS` channel.
//!
//! # Sink contract
//!
//! Writes always target the **most recently started row** for a given table
//! path. A sink may therefore keep one open row per table and flush it when the
//! next row for that table starts; it never has to retain earlier rows. The
//! traversal does write to a parent row *after* descending into its children, so
//! a row stays open across child rows of other tables.

use serde_json::Value;

use crate::contract::{classify_value, PlannedTable, PropertyColumns, SimpleType, TablePath};
use crate::error::{CoreError, CoreResult};

/// A handle to the row a sink just started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRef {
    /// Sink-defined row position. The in-memory sink uses it as a buffer index.
    pub index: usize,
    /// The `_id` assigned to this row, when the table has one.
    pub id: Option<i64>,
}

/// Receives the rows produced by [`write_document`].
pub trait RowSink {
    /// The plan for `path`, or an error when the traversal reaches a table the
    /// sink was not built for.
    fn plan(&self, path: &TablePath) -> CoreResult<&PlannedTable>;

    /// Start a new row in `path`, assigning `_id` when the table has one.
    fn start_row(&mut self, path: &TablePath) -> CoreResult<RowRef>;

    /// Set a boolean column, ignoring columns absent from the plan.
    fn set_bool(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: bool,
    ) -> CoreResult<()>;

    /// Set an integer column, ignoring columns absent from the plan.
    fn set_int(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: i64,
    ) -> CoreResult<()>;

    /// Set a typed scalar column from a JSON value.
    fn set_scalar(
        &mut self,
        path: &TablePath,
        column: &str,
        ty: SimpleType,
        value: &Value,
        row: RowRef,
    ) -> CoreResult<()>;

    /// The columns a property maps onto, if the property is in the plan.
    fn property_columns(
        &self,
        path: &TablePath,
        property: &str,
    ) -> CoreResult<Option<PropertyColumns>> {
        Ok(self.plan(path)?.property_columns(property).cloned())
    }
}

/// Whether a table row missing an `_id` may still carry nested arrays.
#[derive(Clone, Copy)]
enum MissingArrayParentIdPolicy {
    /// Root and object tables: an absent id simply means no array children.
    Ignore,
    /// Array element tables: array children cannot be linked without an id.
    Error,
}

/// Write one root document into the sink, recursing through the whole family.
pub fn write_document<S: RowSink + ?Sized>(
    sink: &mut S,
    document: &serde_json::Map<String, Value>,
) -> CoreResult<()> {
    process_object(sink, &TablePath::root(), document)?;
    Ok(())
}

fn process_object<S: RowSink + ?Sized>(
    sink: &mut S,
    path: &TablePath,
    obj: &serde_json::Map<String, Value>,
) -> CoreResult<Option<i64>> {
    let row = sink.start_row(path)?;

    for (name, value) in obj {
        let Some(prop_plan) = sink.property_columns(path, name)? else {
            continue;
        };
        process_property_value(
            sink,
            path,
            name,
            value,
            &prop_plan,
            row,
            MissingArrayParentIdPolicy::Ignore,
        )?;
    }

    Ok(row.id)
}

fn process_array_elem<S: RowSink + ?Sized>(
    sink: &mut S,
    path: &TablePath,
    value: &Value,
    parent_id: i64,
    pos: i64,
) -> CoreResult<Option<i64>> {
    let row = sink.start_row(path)?;
    sink.set_int(path, "_parent", row, parent_id)?;
    sink.set_int(path, "_pos", row, pos)?;

    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let Some(prop_plan) = sink.property_columns(path, k)? else {
                    continue;
                };
                process_property_value(
                    sink,
                    path,
                    k,
                    v,
                    &prop_plan,
                    row,
                    MissingArrayParentIdPolicy::Error,
                )?;
            }
        }
        _ => {
            if let Some(value_plan) = sink.property_columns(path, "value")? {
                process_property_value(
                    sink,
                    path,
                    "value",
                    value,
                    &value_plan,
                    row,
                    MissingArrayParentIdPolicy::Ignore,
                )?;
            }
        }
    }

    Ok(row.id)
}

fn process_property_value<S: RowSink + ?Sized>(
    sink: &mut S,
    path: &TablePath,
    property: &str,
    value: &Value,
    prop_plan: &PropertyColumns,
    row: RowRef,
    missing_array_parent_id: MissingArrayParentIdPolicy,
) -> CoreResult<()> {
    match value {
        Value::Null => {
            if let Some(mask_name) = prop_plan.null_mask.as_deref() {
                sink.set_bool(path, mask_name, row, true)?;
            }
        }
        Value::Object(map) => {
            let child_path = path.child_object(property);
            let child_id = process_object(sink, &child_path, map)?;
            if let (Some(fk_col), Some(id)) = (prop_plan.object_fk.as_deref(), child_id) {
                sink.set_int(path, fk_col, row, id)?;
            }
        }
        Value::Array(arr) => {
            let count_col = prop_plan
                .array_count
                .as_ref()
                .or(prop_plan.primary.as_ref())
                .cloned();
            if let Some(count_col) = count_col {
                sink.set_int(path, &count_col, row, arr.len() as i64)?;
            }

            if let Some(parent_id) = row.id {
                let child_path = path.child_array(property);
                for (idx, elem) in arr.iter().enumerate() {
                    process_array_elem(sink, &child_path, elem, parent_id, idx as i64)?;
                }
            } else if matches!(missing_array_parent_id, MissingArrayParentIdPolicy::Error) {
                return Err(CoreError::msg(format!(
                    "Table {path} requires _id to link nested arrays"
                )));
            }
        }
        _ => {
            write_scalar_with_plan(sink, path, prop_plan, value, row)?;
        }
    }

    Ok(())
}

fn write_scalar_with_plan<S: RowSink + ?Sized>(
    sink: &mut S,
    path: &TablePath,
    plan: &PropertyColumns,
    value: &Value,
    row: RowRef,
) -> CoreResult<()> {
    let Some(mut ty) = classify_value(value) else {
        return Ok(());
    };
    if ty == SimpleType::Integer && plan.main_type == Some(SimpleType::Number) {
        ty = SimpleType::Number;
    }

    if let Some(primary_name) = &plan.primary {
        if Some(ty) == plan.main_type {
            sink.set_scalar(path, primary_name, ty, value, row)?;
            return Ok(());
        }
    }

    if let Some(alt_name) = plan.alternates.get(&ty) {
        sink.set_scalar(path, alt_name, ty, value, row)?;
    }
    Ok(())
}

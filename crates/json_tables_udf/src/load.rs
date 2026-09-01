//! The emitting sink: rows for one table go out through the UDF's `EMITS`
//! channel, which is the engine's own fast path.
//!
//! # Why it walks the whole family to emit one table
//!
//! Identities have to agree across statements. Each load pass runs as its own
//! `INSERT`, but a root row's `hours|object` value must equal the `_id` the
//! `hours` table gave that child. The traversal assigns identities per table in
//! document order, so the sink keeps a counter for **every** table in the plan
//! even though it only emits one — that makes the identities of pass *n* and
//! pass *m* identical by construction, without any shared state between them.

use std::collections::HashMap;

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{Decimal, Value as ExaValue};
use json_tables_core::contract::{ColumnPlan, PlannedTable, SimpleType, TablePath};
use json_tables_core::sink::{RowRef, RowSink};
use json_tables_core::{CoreError, CoreResult};
use serde_json::Value as JsonValue;

/// Emits the rows of one table from a walk over the whole family.
pub struct EmitSink<'ctx> {
    ctx: &'ctx mut dyn UdfContext,
    plans: HashMap<TablePath, PlannedTable>,
    target: TablePath,
    target_columns: Vec<ColumnPlan>,
    column_index: HashMap<String, usize>,
    next_id: HashMap<TablePath, i64>,
    open_row: Option<Vec<ExaValue>>,
    emitted: u64,
    /// The first emit error, kept because the core's sink errors are `CoreError`.
    failure: Option<String>,
}

impl<'ctx> EmitSink<'ctx> {
    /// Build a sink that emits `target`'s rows and silently walks the rest.
    pub fn new(
        ctx: &'ctx mut dyn UdfContext,
        plans: &[PlannedTable],
        target: &TablePath,
    ) -> Result<Self, UdfError> {
        let plan = plans
            .iter()
            .find(|plan| &plan.path == target)
            .ok_or_else(|| UdfError::User(format!("the plan has no table for path '{target}'")))?;

        // Only physical columns reach the wire, in plan order.
        let target_columns: Vec<ColumnPlan> = plan
            .columns
            .iter()
            .filter(|column| {
                !matches!(
                    column.ty,
                    SimpleType::Null | SimpleType::Object | SimpleType::Array
                )
            })
            .cloned()
            .collect();
        let column_index = target_columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.clone(), index))
            .collect();

        Ok(Self {
            ctx,
            plans: plans
                .iter()
                .map(|plan| (plan.path.clone(), plan.clone()))
                .collect(),
            target: target.clone(),
            target_columns,
            column_index,
            next_id: HashMap::new(),
            open_row: None,
            emitted: 0,
            failure: None,
        })
    }

    /// Flush the last open row and report how many rows were emitted.
    pub fn finish(mut self) -> Result<u64, UdfError> {
        self.flush()?;
        if let Some(failure) = self.failure {
            return Err(UdfError::User(failure));
        }
        Ok(self.emitted)
    }

    fn flush(&mut self) -> Result<(), UdfError> {
        if let Some(row) = self.open_row.take() {
            self.ctx.emit(&row)?;
            self.emitted += 1;
        }
        Ok(())
    }

    /// A fresh row: absent values are SQL NULL, except `NOT NULL` explicit-null
    /// masks, which default to false.
    fn blank_row(&self) -> Vec<ExaValue> {
        self.target_columns
            .iter()
            .map(|column| {
                if column.is_null_mask {
                    ExaValue::Bool(false)
                } else {
                    ExaValue::Null
                }
            })
            .collect()
    }

    fn is_target(&self, path: &TablePath) -> bool {
        path == &self.target
    }

    fn set(&mut self, column: &str, value: ExaValue) {
        let Some(index) = self.column_index.get(column).copied() else {
            return;
        };
        if let Some(row) = self.open_row.as_mut() {
            row[index] = value;
        } else if self.failure.is_none() {
            self.failure = Some(format!("value for '{column}' arrived with no open row"));
        }
    }

    fn column_type(&self, column: &str) -> Option<SimpleType> {
        self.column_index
            .get(column)
            .map(|index| self.target_columns[*index].ty)
    }
}

/// The next identity for `path`, counting from 1 in document order.
fn take_id(next_id: &mut HashMap<TablePath, i64>, path: &TablePath) -> i64 {
    let counter = next_id.entry(path.clone()).or_insert(1);
    let id = *counter;
    *counter += 1;
    id
}

fn integer(value: i64) -> ExaValue {
    // Exasol DECIMAL/BIGINT travel as NUMERIC on the wire, not as Int64.
    ExaValue::Numeric(Decimal {
        unscaled: value as i128,
        scale: 0,
    })
}

impl RowSink for EmitSink<'_> {
    fn plan(&self, path: &TablePath) -> CoreResult<&PlannedTable> {
        self.plans
            .get(path)
            .ok_or_else(|| CoreError::msg(format!("the plan has no table for path '{path}'")))
    }

    fn start_row(&mut self, path: &TablePath) -> CoreResult<RowRef> {
        let has_id = self
            .plan(path)?
            .columns
            .iter()
            .any(|column| column.name == "_id" && column.is_required);
        let id = has_id.then(|| take_id(&mut self.next_id, path));

        if self.is_target(path) {
            self.flush()
                .map_err(|err| CoreError::msg(format!("emit failed: {err}")))?;
            self.open_row = Some(self.blank_row());
            if let Some(id) = id {
                self.set("_id", integer(id));
            }
        }

        // `index` is unused by this sink: rows are written as they are opened.
        Ok(RowRef { index: 0, id })
    }

    fn set_bool(
        &mut self,
        path: &TablePath,
        column: &str,
        _row: RowRef,
        value: bool,
    ) -> CoreResult<()> {
        if self.is_target(path) {
            self.set(column, ExaValue::Bool(value));
        }
        Ok(())
    }

    fn set_int(
        &mut self,
        path: &TablePath,
        column: &str,
        _row: RowRef,
        value: i64,
    ) -> CoreResult<()> {
        if self.is_target(path) {
            self.set(column, integer(value));
        }
        Ok(())
    }

    fn set_scalar(
        &mut self,
        path: &TablePath,
        column: &str,
        ty: SimpleType,
        value: &JsonValue,
        _row: RowRef,
    ) -> CoreResult<()> {
        if !self.is_target(path) {
            return Ok(());
        }
        // The declared column type wins over the classified one: it is what the
        // EMITS clause promised the engine.
        let ty = self.column_type(column).unwrap_or(ty);
        let converted = match ty {
            SimpleType::Bool => value.as_bool().map(ExaValue::Bool),
            SimpleType::Integer => value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|u| i64::try_from(u).ok()))
                .map(integer),
            SimpleType::Number => value.as_f64().map(ExaValue::Double),
            SimpleType::String => value
                .as_str()
                .map(|text| ExaValue::String(text.to_string())),
            SimpleType::Null | SimpleType::Object | SimpleType::Array => None,
        };
        match converted {
            Some(converted) => {
                self.set(column, converted);
                Ok(())
            }
            None => Err(CoreError::msg(format!(
                "value {value} does not fit column '{column}' of type {ty}"
            ))),
        }
    }
}

#[cfg(test)]
#[path = "load_tests.rs"]
mod tests;

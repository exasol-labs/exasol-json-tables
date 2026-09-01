//! An in-memory columnar [`RowSink`] — the sink the CLI stages Parquet from.
//!
//! It buffers the whole family, which is fine for a client-side batch load and
//! deliberately not the only option: a streaming sink implementing [`RowSink`]
//! can flush each row as it is produced.

use std::collections::HashMap;

use serde_json::Value;

use crate::contract::{ColumnPlan, PlannedTable, SimpleType, TablePath};
use crate::error::{CoreError, CoreResult};
use crate::sink::{RowRef, RowSink};

/// One column's buffered values.
#[derive(Debug)]
pub enum ColumnValues {
    Bool(Vec<Option<bool>>),
    /// Required bool, defaults to false — used for `<name>|n` masks.
    BoolMask(Vec<bool>),
    Int(Vec<Option<i64>>),
    Double(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl ColumnValues {
    pub fn new(ty: SimpleType, is_null_mask: bool) -> Self {
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

    pub fn push_default(&mut self) {
        match self {
            ColumnValues::Bool(v) => v.push(None),
            ColumnValues::BoolMask(v) => v.push(false),
            ColumnValues::Int(v) => v.push(None),
            ColumnValues::Double(v) => v.push(None),
            ColumnValues::Str(v) => v.push(None),
        }
    }

    pub fn set_bool(&mut self, idx: usize, value: bool) {
        match self {
            ColumnValues::Bool(v) => v[idx] = Some(value),
            ColumnValues::BoolMask(v) => v[idx] = value,
            _ => {}
        }
    }

    pub fn set_int(&mut self, idx: usize, value: i64) {
        if let ColumnValues::Int(v) = self {
            v[idx] = Some(value)
        }
    }

    pub fn set_double(&mut self, idx: usize, value: f64) {
        if let ColumnValues::Double(v) = self {
            v[idx] = Some(value)
        }
    }

    pub fn set_str(&mut self, idx: usize, value: String) {
        if let ColumnValues::Str(v) = self {
            v[idx] = Some(value)
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnValues::Bool(v) => v.len(),
            ColumnValues::BoolMask(v) => v.len(),
            ColumnValues::Int(v) => v.len(),
            ColumnValues::Double(v) => v.len(),
            ColumnValues::Str(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Buffered rows for one table, plus the plan they were built against.
#[derive(Debug)]
pub struct TableBuffer {
    pub plan: PlannedTable,
    pub columns: HashMap<String, ColumnValues>,
    next_id: i64,
    row_count: usize,
}

impl TableBuffer {
    pub fn new(plan: PlannedTable) -> Self {
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

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Append a row, assigning `_id` when the plan has one.
    pub fn push_row(&mut self) -> RowRef {
        for col in self.columns.values_mut() {
            col.push_default();
        }
        let index = self.row_count;
        self.row_count += 1;

        let mut id = None;
        if let Some(pk_col) = self
            .plan
            .columns
            .iter()
            .find(|c| c.name == "_id" && c.is_required)
        {
            let assigned = self.next_id;
            self.next_id += 1;
            if let Some(col) = self.columns.get_mut(&pk_col.name) {
                col.set_int(index, assigned);
            }
            id = Some(assigned);
        }

        RowRef { index, id }
    }

    /// The buffered values for a planned column.
    pub fn column(&self, column: &ColumnPlan) -> CoreResult<&ColumnValues> {
        self.columns
            .get(&column.name)
            .ok_or_else(|| CoreError::msg(format!("No data for column {}", column.name)))
    }
}

/// The whole table family, buffered in memory.
#[derive(Debug)]
pub struct ColumnBuffers {
    tables: HashMap<TablePath, TableBuffer>,
}

impl ColumnBuffers {
    /// Build empty buffers for every planned table.
    pub fn new(plans: &[PlannedTable]) -> Self {
        Self {
            tables: plans
                .iter()
                .map(|p| (p.path.clone(), TableBuffer::new(p.clone())))
                .collect(),
        }
    }

    pub fn tables(&self) -> impl Iterator<Item = (&TablePath, &TableBuffer)> {
        self.tables.iter()
    }

    pub fn table(&self, path: &TablePath) -> Option<&TableBuffer> {
        self.tables.get(path)
    }

    fn table_mut(&mut self, path: &TablePath) -> CoreResult<&mut TableBuffer> {
        self.tables
            .get_mut(path)
            .ok_or_else(|| CoreError::msg(format!("Missing writer for path {path}")))
    }
}

impl RowSink for ColumnBuffers {
    fn plan(&self, path: &TablePath) -> CoreResult<&PlannedTable> {
        self.tables
            .get(path)
            .map(|t| &t.plan)
            .ok_or_else(|| CoreError::msg(format!("Missing writer for path {path}")))
    }

    fn start_row(&mut self, path: &TablePath) -> CoreResult<RowRef> {
        Ok(self.table_mut(path)?.push_row())
    }

    fn set_bool(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: bool,
    ) -> CoreResult<()> {
        let table = self.table_mut(path)?;
        if let Some(col) = table.columns.get_mut(column) {
            col.set_bool(row.index, value);
        }
        Ok(())
    }

    fn set_int(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: i64,
    ) -> CoreResult<()> {
        let table = self.table_mut(path)?;
        if let Some(col) = table.columns.get_mut(column) {
            col.set_int(row.index, value);
        }
        Ok(())
    }

    fn set_scalar(
        &mut self,
        path: &TablePath,
        column: &str,
        ty: SimpleType,
        value: &Value,
        row: RowRef,
    ) -> CoreResult<()> {
        let table = self.table_mut(path)?;
        let col = table
            .columns
            .get_mut(column)
            .ok_or_else(|| CoreError::msg(format!("Missing column data for {column}")))?;

        match ty {
            SimpleType::Bool => {
                let v = value
                    .as_bool()
                    .ok_or_else(|| CoreError::msg(format!("Expected bool for {column}")))?;
                col.set_bool(row.index, v);
            }
            SimpleType::Integer => {
                let v = value
                    .as_i64()
                    .or_else(|| value.as_u64().and_then(|u| i64::try_from(u).ok()))
                    .ok_or_else(|| CoreError::msg(format!("Expected integer for {column}")))?;
                col.set_int(row.index, v);
            }
            SimpleType::Number => {
                let v = value
                    .as_f64()
                    .ok_or_else(|| CoreError::msg(format!("Expected number for {column}")))?;
                col.set_double(row.index, v);
            }
            SimpleType::String => {
                let v = value
                    .as_str()
                    .ok_or_else(|| CoreError::msg(format!("Expected string for {column}")))?;
                col.set_str(row.index, v.to_owned());
            }
            SimpleType::Null | SimpleType::Object | SimpleType::Array => {}
        }

        Ok(())
    }
}

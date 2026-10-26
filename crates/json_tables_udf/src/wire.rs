//! Fitting emitted values to the wire type of their output column.
//!
//! The language container validates every emitted value against the block its
//! column travels in, and that block depends on the declared SQL type: a
//! `DECIMAL(p,0)` column travels as `Int32` for `p <= 9`, as `Int64` for
//! `p <= 18`, and as `Numeric` beyond that. The loader produces integers without
//! knowing the column, so they are fitted here from the metadata the host
//! reports, rather than by repeating the container's mapping rules.

use exasol_udf_sdk::context::UdfContext;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::{Decimal, ExaType, Value};

/// The wire type of each of the first `count` output columns, or `None` for a
/// column the context reports no metadata for (a test double, say). Read once,
/// so the per-row fit does not go back to the host.
pub fn output_types(ctx: &dyn UdfContext, count: usize) -> Vec<Option<ExaType>> {
    (0..count)
        .map(|index| {
            ctx.output_column(index)
                .ok()
                .map(|column| column.typ.clone())
        })
        .collect()
}

/// Fit every value of `row` to its column's wire type.
pub fn fit_row(row: Vec<Value>, types: &[Option<ExaType>]) -> Result<Vec<Value>, UdfError> {
    row.into_iter()
        .enumerate()
        .map(|(index, value)| match types.get(index) {
            Some(Some(typ)) => fit(value, typ),
            _ => Ok(value),
        })
        .collect()
}

/// Fit one value to `typ`. Only integers move between blocks; any other value
/// is passed through, so a genuine type mismatch still surfaces from the host.
pub fn fit(value: Value, typ: &ExaType) -> Result<Value, UdfError> {
    let Some(integer) = as_integer(&value) else {
        return Ok(value);
    };
    match typ {
        ExaType::Int32 => i32::try_from(integer)
            .map(Value::Int32)
            .map_err(|_| out_of_range(integer, typ)),
        ExaType::Int64 => i64::try_from(integer)
            .map(Value::Int64)
            .map_err(|_| out_of_range(integer, typ)),
        ExaType::Numeric { scale, .. } => u8::try_from(*scale)
            .ok()
            .and_then(|scale| {
                let unscaled = integer.checked_mul(10i128.checked_pow(u32::from(scale))?)?;
                Some(Value::Numeric(Decimal { unscaled, scale }))
            })
            .ok_or_else(|| out_of_range(integer, typ)),
        _ => Ok(value),
    }
}

/// The value as a whole number, if it is one in any integral representation.
fn as_integer(value: &Value) -> Option<i128> {
    match value {
        Value::Int32(i) => Some(i128::from(*i)),
        Value::Int64(i) => Some(i128::from(*i)),
        Value::Numeric(decimal) if decimal.scale == 0 => Some(decimal.unscaled),
        _ => None,
    }
}

fn out_of_range(integer: i128, typ: &ExaType) -> UdfError {
    UdfError::Type(format!(
        "integer {integer} does not fit an output column of type {typ:?}"
    ))
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;

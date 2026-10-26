use super::*;

fn numeric(unscaled: i128, scale: u8) -> Value {
    Value::Numeric(Decimal { unscaled, scale })
}

#[test]
fn integers_fit_the_block_their_column_travels_in() {
    // DECIMAL(18,0), the type of every integer column in the contract.
    assert_eq!(
        fit(Value::Int64(7), &ExaType::Int64).unwrap(),
        Value::Int64(7)
    );
    assert_eq!(
        fit(numeric(7, 0), &ExaType::Int64).unwrap(),
        Value::Int64(7)
    );
    // DECIMAL(9,0) and below.
    assert_eq!(
        fit(Value::Int64(7), &ExaType::Int32).unwrap(),
        Value::Int32(7)
    );
    // Beyond 18 digits, or with a scale.
    let wide = ExaType::Numeric {
        precision: 36,
        scale: 0,
    };
    assert_eq!(fit(Value::Int64(7), &wide).unwrap(), numeric(7, 0));
    let scaled = ExaType::Numeric {
        precision: 18,
        scale: 2,
    };
    assert_eq!(fit(Value::Int64(7), &scaled).unwrap(), numeric(700, 2));
}

#[test]
fn an_integer_too_wide_for_its_block_is_an_error_not_a_wrap() {
    let err = fit(Value::Int64(i64::from(i32::MAX) + 1), &ExaType::Int32).unwrap_err();
    assert!(format!("{err:?}").contains("does not fit"), "{err:?}");
    assert!(fit(numeric(i128::from(i64::MAX) + 1, 0), &ExaType::Int64).is_err());
}

#[test]
fn non_integers_pass_through_untouched() {
    for (value, typ) in [
        (Value::Null, ExaType::Int64),
        (Value::Bool(true), ExaType::Boolean),
        (Value::Double(1.5), ExaType::Double),
        (Value::String("x".into()), ExaType::String { size: 10 }),
        (numeric(15, 1), ExaType::Int64),
    ] {
        assert_eq!(fit(value.clone(), &typ).unwrap(), value);
    }
}

#[test]
fn a_row_without_column_metadata_is_left_as_is() {
    let row = vec![Value::Int64(1), Value::Int64(2)];
    let fitted = fit_row(row.clone(), &[Some(ExaType::Int32), None]).unwrap();
    assert_eq!(fitted, vec![Value::Int32(1), Value::Int64(2)]);
    assert_eq!(fit_row(row.clone(), &[]).unwrap(), row);
}

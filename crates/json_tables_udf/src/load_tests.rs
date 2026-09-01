use super::*;
use crate::driver::plan_text;
use json_tables_core::read::{detect_format, for_each_document};
use std::io::Cursor;

/// A context that records what the UDF emitted.
#[derive(Default)]
struct RecordingCtx {
    input: Vec<ExaValue>,
    emitted: Vec<Vec<ExaValue>>,
}

impl UdfContext for RecordingCtx {
    fn num_columns(&self) -> usize {
        self.input.len()
    }

    fn get(&self, col: usize) -> Result<&ExaValue, UdfError> {
        self.input
            .get(col)
            .ok_or_else(|| UdfError::User(format!("no input column {col}")))
    }

    fn emit(&mut self, values: &[ExaValue]) -> Result<(), UdfError> {
        self.emitted.push(values.to_vec());
        Ok(())
    }

    fn next(&mut self) -> Result<bool, UdfError> {
        Ok(false)
    }
}

const INPUT: &str = r#"[
  {"id": 1, "name": "Cafe", "hours": {"mon": "9-5"}, "tags": ["coffee", "wifi"]},
  {"id": 2, "name": "Diner", "note": null, "hours": {"mon": null}, "tags": []}
]"#;

fn emit_table(input: &str, path: &str) -> Vec<Vec<ExaValue>> {
    let plans = plan_text(input).expect("plan");
    let target = plans
        .iter()
        .find(|plan| plan.path.to_string() == path)
        .unwrap_or_else(|| panic!("no table {path}"))
        .path
        .clone();

    let mut ctx = RecordingCtx::default();
    {
        let mut sink = EmitSink::new(&mut ctx, &plans, &target).expect("sink");
        let mut cursor = Cursor::new(input.as_bytes());
        let format = detect_format(&mut cursor).expect("format");
        for_each_document(cursor, format, |_, document| {
            json_tables_core::sink::write_document(&mut sink, document)
        })
        .expect("walk");
        let emitted = sink.finish().expect("finish");
        assert_eq!(emitted as usize, ctx.emitted.len());
    }
    ctx.emitted
}

fn numeric(value: i64) -> ExaValue {
    ExaValue::Numeric(Decimal {
        unscaled: value as i128,
        scale: 0,
    })
}

#[test]
fn emits_only_the_target_tables_rows() {
    assert_eq!(emit_table(INPUT, "root").len(), 2);
    assert_eq!(emit_table(INPUT, "hours").len(), 2);
    // Two elements in the first document, none in the second.
    assert_eq!(emit_table(INPUT, "tags[]").len(), 2);
}

#[test]
fn integers_go_out_as_numeric_not_int64() {
    // Exasol DECIMAL/BIGINT travel in the string block as NUMERIC; emitting
    // Int64 for a DECIMAL column is the classic wire-type mistake.
    let rows = emit_table(INPUT, "root");
    assert_eq!(rows[0][0], numeric(1), "_id");
    assert!(matches!(rows[0][1], ExaValue::Numeric(_)), "id");
}

#[test]
fn identities_are_assigned_per_table_in_document_order() {
    let root = emit_table(INPUT, "root");
    assert_eq!(root[0][0], numeric(1));
    assert_eq!(root[1][0], numeric(2));

    let hours = emit_table(INPUT, "hours");
    assert_eq!(hours[0][0], numeric(1));
    assert_eq!(hours[1][0], numeric(2));
}

/// The property that makes multi-statement loading correct: a parent's object
/// link, emitted by one statement, must match the child's `_id`, emitted by a
/// different statement.
#[test]
fn object_links_agree_across_separate_load_passes() {
    let plans = plan_text(INPUT).expect("plan");
    let root_plan = plans.iter().find(|p| p.path.is_root()).unwrap();
    let fk_index = root_plan
        .columns
        .iter()
        .position(|c| c.name == "hours|object")
        .expect("hours|object");

    let root = emit_table(INPUT, "root");
    let hours = emit_table(INPUT, "hours");

    for (row, child) in root.iter().zip(hours.iter()) {
        assert_eq!(row[fk_index], child[0], "parent link must match child _id");
    }
}

#[test]
fn array_children_carry_parent_and_position() {
    let plans = plan_text(INPUT).expect("plan");
    let tags_plan = plans
        .iter()
        .find(|p| p.path.to_string() == "tags[]")
        .unwrap();
    let names: Vec<&str> = tags_plan.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["_parent", "_pos", "_value"]);

    let rows = emit_table(INPUT, "tags[]");
    assert_eq!(rows[0][0], numeric(1), "_parent");
    assert_eq!(rows[0][1], numeric(0), "_pos");
    assert_eq!(rows[0][2], ExaValue::String("coffee".to_string()));
    assert_eq!(rows[1][1], numeric(1), "_pos of the second element");
}

#[test]
fn explicit_null_masks_default_to_false_and_flip_to_true() {
    let plans = plan_text(INPUT).expect("plan");
    let hours_plan = plans
        .iter()
        .find(|p| p.path.to_string() == "hours")
        .unwrap();
    let mask_index = hours_plan
        .columns
        .iter()
        .position(|c| c.name == "mon|n")
        .expect("mon|n");

    let rows = emit_table(INPUT, "hours");
    // The mask column is NOT NULL, so a row without an explicit null still has
    // to carry FALSE rather than SQL NULL.
    assert_eq!(rows[0][mask_index], ExaValue::Bool(false));
    assert_eq!(rows[1][mask_index], ExaValue::Bool(true));
}

#[test]
fn missing_values_stay_null() {
    let plans = plan_text(INPUT).expect("plan");
    let root_plan = plans.iter().find(|p| p.path.is_root()).unwrap();
    let note_mask = root_plan
        .columns
        .iter()
        .position(|c| c.name == "note|n")
        .expect("note|n");

    let rows = emit_table(INPUT, "root");
    assert_eq!(rows[0][note_mask], ExaValue::Bool(false), "absent in doc 1");
    assert_eq!(
        rows[1][note_mask],
        ExaValue::Bool(true),
        "explicit null in doc 2"
    );
}

#[test]
fn a_target_outside_the_plan_is_rejected() {
    let plans = plan_text(INPUT).expect("plan");
    let mut ctx = RecordingCtx::default();
    let missing = json_tables_core::contract::TablePath::root().child_object("nope");
    let err = match EmitSink::new(&mut ctx, &plans, &missing) {
        Ok(_) => panic!("a target outside the plan must not build a sink"),
        Err(err) => err,
    };
    assert!(format!("{err:?}").contains("no table for path"), "{err:?}");
}

#[test]
fn ndjson_and_array_framing_produce_the_same_rows() {
    let array = r#"[{"id":1,"tags":["x"]},{"id":2,"tags":["y","z"]}]"#;
    let ndjson = "{\"id\":1,\"tags\":[\"x\"]}\n{\"id\":2,\"tags\":[\"y\",\"z\"]}\n";
    assert_eq!(emit_table(array, "root"), emit_table(ndjson, "root"));
    assert_eq!(emit_table(array, "tags[]"), emit_table(ndjson, "tags[]"));
}

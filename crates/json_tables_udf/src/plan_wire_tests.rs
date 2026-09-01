use super::*;
use crate::driver::plan_text;

const INPUT: &str = r#"[
  {"id": 1, "name": "Cafe", "note": null, "value": 42, "hours": {"mon": "9-5"}, "tags": ["a","b"]},
  {"id": 2, "name": "Diner", "value": "many", "hours": {"mon": null}, "tags": []}
]"#;

/// A plan that survives a round trip is what makes multi-statement loading safe:
/// every pass must write against byte-identical columns.
#[test]
fn a_plan_round_trips_without_losing_anything() {
    let plans = plan_text(INPUT).expect("plan");
    let encoded = encode(&plans, "orders");
    let (decoded, stem) = decode(&encoded).expect("decode");

    assert_eq!(stem, "orders");
    assert_eq!(decoded.len(), plans.len());

    for (before, after) in plans.iter().zip(decoded.iter()) {
        assert_eq!(before.path, after.path);
        assert_eq!(before.kind, after.kind);
        assert_eq!(before.has_nested_array, after.has_nested_array);

        let columns = |plan: &PlannedTable| -> Vec<(String, String, bool, bool)> {
            plan.columns
                .iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        c.ty.to_string(),
                        c.is_null_mask,
                        c.is_required,
                    )
                })
                .collect()
        };
        assert_eq!(
            columns(before),
            columns(after),
            "columns for {}",
            before.path
        );

        assert_eq!(before.properties.len(), after.properties.len());
        for (name, expected) in &before.properties {
            let actual = after.properties.get(name).expect(name);
            assert_eq!(expected.main_type, actual.main_type, "mainType of {name}");
            assert_eq!(expected.primary, actual.primary, "primary of {name}");
            assert_eq!(expected.object_fk, actual.object_fk, "objectFk of {name}");
            assert_eq!(expected.null_mask, actual.null_mask, "nullMask of {name}");
            assert_eq!(
                expected.array_count, actual.array_count,
                "arrayCount of {name}"
            );
            assert_eq!(
                expected.alternates, actual.alternates,
                "alternates of {name}"
            );
        }
    }
}

#[test]
fn re_encoding_a_decoded_plan_is_byte_identical() {
    let plans = plan_text(INPUT).expect("plan");
    let once = encode(&plans, "orders");
    let (decoded, stem) = decode(&once).expect("decode");
    assert_eq!(encode(&decoded, &stem), once);
}

#[test]
fn variant_and_mask_columns_keep_their_kinds() {
    let plans = plan_text(INPUT).expect("plan");
    let (decoded, _) = decode(&encode(&plans, "orders")).expect("decode");
    let root = decoded
        .iter()
        .find(|plan| plan.path.is_root())
        .expect("root");

    let kind_of = |name: &str| -> String {
        let column = root
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no column {name}"));
        match &column.kind {
            ColumnKind::Primary { .. } => "primary".to_string(),
            ColumnKind::Alternate { source_ty, .. } => format!("alternate:{source_ty}"),
            ColumnKind::NullBitmask { .. } => "nullMask".to_string(),
        }
    };
    assert_eq!(kind_of("_id"), "primary");
    assert_eq!(kind_of("note|n"), "nullMask");
    // `value` is an integer in one document and a string in the other.
    assert_eq!(kind_of("value|string"), "alternate:string");
}

#[test]
fn a_foreign_plan_is_rejected_rather_than_half_read() {
    assert!(decode("not json").is_err());
    assert!(decode(r#"{"format":"something-else","version":1}"#).is_err());
    assert!(decode(r#"{"format":"exasol-json-tables-plan","version":99}"#).is_err());
    assert!(decode(r#"{"format":"exasol-json-tables-plan","version":1}"#).is_err());
}

#[test]
fn plans_are_stable_across_repeated_inference() {
    // Every load statement re-derives nothing, but the driver's plan must be
    // reproducible for an operator comparing two runs.
    let first = encode(&plan_text(INPUT).expect("plan"), "orders");
    let second = encode(&plan_text(INPUT).expect("plan"), "orders");
    assert_eq!(first, second);
}

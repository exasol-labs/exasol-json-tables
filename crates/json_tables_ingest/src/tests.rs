use super::*;
use arrow::{
    array::{Array, StringArray},
    record_batch::RecordBatch,
};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use std::{
    collections::BTreeMap,
    env,
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::tempdir;

#[test]
fn collects_counts_for_flat_values_and_skips_nested() {
    let mut stats = PropertyStats::default();

    // Basic flat properties and nested values that should be skipped in direct column counts.
    stats.record_value("id", &Value::Number(1.into()));
    stats.record_value("name", &Value::String("alice".into()));
    stats.record_value("name", &Value::String("bob".into()));
    stats.record_value("flag", &Value::Bool(true));
    stats.record_value("meta", &Value::Object(Default::default()));
    stats.record_value("tags", &Value::Array(vec![Value::String("x".into())]));

    let mut entries: Vec<_> = stats.counts.into_iter().collect();
    entries.sort_by(|a, b| a.0.name.cmp(&b.0.name));

    assert_eq!(entries.len(), 5);

    let lookup = |name: &str, ty: SimpleType| -> usize {
        entries
            .iter()
            .find(|(k, _)| k.name == name && k.ty == ty)
            .map(|(_, v)| *v)
            .unwrap_or(0)
    };

    assert_eq!(lookup("flag", SimpleType::Bool), 1);
    assert_eq!(lookup("id", SimpleType::Integer), 1);
    assert_eq!(lookup("name", SimpleType::String), 2);
}

#[test]
fn scans_fixture_file_with_varied_types() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sample.json");

    // Ensure counts reflect mixed numeric types, nulls, and optional fields.
    let stats = scan_all_stats(&path, InputFormat::Array).expect("failed to scan fixture");
    let stats = stats.into_iter().find(|t| t.path.is_root()).unwrap().stats;

    let lookup = |name: &str, ty: SimpleType| -> usize {
        stats
            .counts
            .get(&FieldKey {
                name: name.to_owned(),
                ty,
            })
            .copied()
            .unwrap_or(0)
    };

    assert_eq!(lookup("id", SimpleType::Integer), 3);
    assert_eq!(lookup("name", SimpleType::String), 3);
    assert_eq!(lookup("active", SimpleType::Bool), 3);

    assert_eq!(lookup("score", SimpleType::Number), 1);
    assert_eq!(lookup("score", SimpleType::Integer), 1);
    assert_eq!(lookup("score", SimpleType::Null), 1);

    assert_eq!(lookup("age", SimpleType::Integer), 2);
    assert_eq!(lookup("age", SimpleType::Null), 1);

    assert_eq!(lookup("note", SimpleType::Null), 1);
    assert_eq!(lookup("note", SimpleType::String), 1);

    assert_eq!(lookup("misc", SimpleType::String), 1);
    assert_eq!(lookup("height", SimpleType::Number), 1);
}

#[test]
fn scans_ndjson_fixture() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sample.ndjson");

    // Same as above but for line-delimited JSON.
    let stats = scan_all_stats(&path, InputFormat::Lines).expect("failed to scan ndjson fixture");
    let stats = stats.into_iter().find(|t| t.path.is_root()).unwrap().stats;

    let lookup = |name: &str, ty: SimpleType| -> usize {
        stats
            .counts
            .get(&FieldKey {
                name: name.to_owned(),
                ty,
            })
            .copied()
            .unwrap_or(0)
    };

    assert_eq!(lookup("id", SimpleType::Integer), 3);
    assert_eq!(lookup("name", SimpleType::String), 3);
    assert_eq!(lookup("active", SimpleType::Bool), 3);
    assert_eq!(lookup("score", SimpleType::Number), 1);
    assert_eq!(lookup("score", SimpleType::Integer), 1);
    assert_eq!(lookup("score", SimpleType::Null), 1);
}

#[test]
fn merges_integer_and_number_into_single_number_column() {
    // Mixed integer and floating-point observations should collapse into a single number column.
    let mut stats = PropertyStats::default();
    stats.record_value("value", &Value::Number(1.into())); // int
    stats.record_value(
        "value",
        &Value::Number(serde_json::Number::from_f64(1.5).unwrap()),
    ); // number
    stats.record_value("value", &Value::Number(2.into())); // int

    let (cols, props) = build_schema_plan(&stats, true, false);
    assert_eq!(cols.len(), 2); // _id + value
    assert_eq!(cols[0].name, "_id");
    assert_eq!(cols[1].name, "value");
    assert_eq!(cols[1].ty, SimpleType::Number);

    let prop = props.get("value").expect("property plan");
    assert_eq!(prop.main_type, Some(SimpleType::Number));
    assert!(prop.alternates.is_empty());
    assert!(prop.null_mask.is_none());
}

#[test]
fn collects_nested_object_stats_into_subtables() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nested.json");

    // Validate that nested objects produce separate table stats and parent references.
    let stats = scan_all_stats(&path, InputFormat::Array).expect("scan nested");

    let root = stats.iter().find(|t| t.path.is_root()).unwrap();
    let child = stats
        .iter()
        .find(|t| t.path.to_string() == "child")
        .expect("child table");

    let lookup = |stats: &PropertyStats, name: &str, ty: SimpleType| -> usize {
        stats
            .counts
            .get(&FieldKey {
                name: name.to_owned(),
                ty,
            })
            .copied()
            .unwrap_or(0)
    };

    // Root table sees object occurrence for child and primitive fields.
    assert_eq!(lookup(&root.stats, "child", SimpleType::Object), 2);
    assert_eq!(lookup(&root.stats, "alt_child", SimpleType::Object), 2);
    assert_eq!(lookup(&root.stats, "meta", SimpleType::Object), 3);
    assert_eq!(lookup(&root.stats, "id", SimpleType::Integer), 3);
    assert_eq!(lookup(&root.stats, "name", SimpleType::String), 3);

    // Child table has its own stats.
    assert_eq!(lookup(&child.stats, "a", SimpleType::Integer), 2);
    assert_eq!(lookup(&child.stats, "b", SimpleType::String), 1);
    assert_eq!(lookup(&child.stats, "c", SimpleType::Bool), 1);
}

#[test]
fn collects_array_stats_and_nested_arrays() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("arrays.json");

    // Arrays of primitives, arrays of objects, and nested arrays should each get their own tables.
    let stats = scan_all_stats(&path, InputFormat::Array).expect("scan arrays");

    let table = |name: &str| -> &TableStats {
        stats
            .iter()
            .find(|t| t.path.to_string() == name)
            .unwrap_or_else(|| panic!("missing table {name}"))
    };

    let root = table("root");
    assert_eq!(lookup_count(&root.stats, "nums", SimpleType::Array), 3);
    assert_eq!(lookup_count(&root.stats, "objs", SimpleType::Array), 2);
    assert_eq!(lookup_count(&root.stats, "nested", SimpleType::Array), 2);

    let nums = table("nums[]");
    assert_eq!(lookup_count(&nums.stats, "value", SimpleType::Integer), 2);
    assert_eq!(lookup_count(&nums.stats, "value", SimpleType::Number), 1);
    assert_eq!(lookup_count(&nums.stats, "value", SimpleType::Null), 1);

    let objs = table("objs[]");
    assert_eq!(lookup_count(&objs.stats, "x", SimpleType::Integer), 2);
    assert_eq!(lookup_count(&objs.stats, "y", SimpleType::String), 1);
    assert_eq!(lookup_count(&objs.stats, "inner", SimpleType::Array), 1);

    let inner = table("objs[].inner[]");
    assert_eq!(lookup_count(&inner.stats, "z", SimpleType::Bool), 2);

    let nested = table("nested[]");
    assert_eq!(lookup_count(&nested.stats, "value", SimpleType::Array), 4);

    let nested_values = table("nested[].value[]");
    assert_eq!(lookup_count(&nested_values.stats, "value", SimpleType::Integer), 6);
}

#[test]
fn collects_heterogeneous_array_stats() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("hetero_arrays.json");

    // Heterogeneous arrays (primitives + objects + nested arrays) track per-type counts.
    let stats = scan_all_stats(&path, InputFormat::Array).expect("scan hetero arrays");

    let table = |name: &str| -> &TableStats {
        stats
            .iter()
            .find(|t| t.path.to_string() == name)
            .unwrap_or_else(|| panic!("missing table {name}"))
    };

    let root = table("root");
    assert_eq!(lookup_count(&root.stats, "mixed", SimpleType::Array), 2);

    let mixed = table("mixed[]");
    assert_eq!(lookup_count(&mixed.stats, "value", SimpleType::Integer), 1);
    assert_eq!(lookup_count(&mixed.stats, "value", SimpleType::Number), 1);
    assert_eq!(lookup_count(&mixed.stats, "value", SimpleType::String), 2);
    assert_eq!(lookup_count(&mixed.stats, "value", SimpleType::Bool), 1);
    assert_eq!(lookup_count(&mixed.stats, "value", SimpleType::Null), 1);
    assert_eq!(lookup_count(&mixed.stats, "value", SimpleType::Array), 1);
    assert_eq!(lookup_count(&mixed.stats, "x", SimpleType::Integer), 1);

    let mixed_arr = table("mixed[].value[]");
    assert_eq!(lookup_count(&mixed_arr.stats, "value", SimpleType::Integer), 2);
}

#[test]
fn edge_case_schema_and_arrays() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("edge_cases.json");

    // Edge cases: all-null fields, null vs missing, null/empty/populated arrays, mixed numeric arrays, and arrays of objects with null fields.
    let stats = scan_all_stats(&path, InputFormat::Array).expect("scan edge cases");
    let plans = build_all_schema_plans(&stats);

    let root = plans.iter().find(|p| p.path.is_root()).unwrap();
    // only_null is always null -> should be omitted.
    assert!(root.properties.get("only_null").is_none());

    // missing_vs_null has nulls and ints -> main int + null mask.
    let mvn = root.properties.get("missing_vs_null").expect("missing_vs_null plan");
    assert_eq!(mvn.main_type, Some(SimpleType::Integer));
    assert!(mvn.null_mask.is_some());

    // arr_mix: null + empty + populated -> count + null mask on parent.
    let arr_mix = root.properties.get("arr_mix").expect("arr_mix plan");
    assert!(arr_mix.array_count.is_some());
    assert!(arr_mix.null_mask.is_some());

    // mixed_num_arr array elements promote to number.
    let arr_table = plans
        .iter()
        .find(|p| p.path.to_string() == "mixed_num_arr[]")
        .expect("mixed_num_arr table");
    let val_cols = arr_table.properties.get("value").expect("value cols");
    assert_eq!(val_cols.main_type, Some(SimpleType::Number));
    assert!(val_cols.primary.as_deref() == Some("_value"));

    // obj_arr elements: field a has explicit null -> null mask present.
    let obj_arr_table = plans
        .iter()
        .find(|p| p.path.to_string() == "obj_arr[]")
        .expect("obj_arr table");
    let a_cols = obj_arr_table.properties.get("a").expect("a cols");
    assert!(a_cols.null_mask.is_some());

    // only_null_arr has all nulls; array table should still exist for parent count.
    assert!(plans.iter().any(|p| p.path.to_string() == "only_null_arr[]"));
}

#[test]
fn table_path_suffix_is_unambiguous_for_dotted_keys() {
    let dotted = TablePath::root().child_object("a.b");
    let nested = TablePath::root().child_object("a").child_object("b");

    // Distinct logical paths should never resolve to the same file suffix.
    assert_ne!(dotted.file_suffix(), nested.file_suffix());
}

#[test]
fn sql_table_names_are_unambiguous_for_array_like_keys() {
    let object_like_array = TablePath::root().child_object("items[]");
    let real_array = TablePath::root().child_array("items");

    // Distinct logical paths should never resolve to the same SQL table name.
    assert_ne!(
        table_sql_name(&object_like_array, "dataset"),
        table_sql_name(&real_array, "dataset")
    );
}

#[test]
fn roundtrip_preserves_mixed_object_and_primitive_values() {
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("mixed_object_primitive.json");

    let json = r#"
[
  {"id":1,"mixed":{"a":10}},
  {"id":2,"mixed":"text"}
]
"#;
    fs::write(&input_path, json).expect("write input");

    let format = detect_input_format(&input_path).expect("format");
    let stats = scan_all_stats(&input_path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);
    let root_path = planned.iter().find(|p| p.path.is_root()).unwrap().path.clone();

    write_all_tables(&input_path, format, &planned, dir.path(), "mixed_object_primitive")
        .expect("write parquet");

    let expected: Vec<serde_json::Value> =
        serde_json::from_reader(File::open(&input_path).unwrap()).expect("read expected json");
    let expected_aligned: Vec<_> = expected
        .into_iter()
        .map(|v| align_to_plan(v, &root_path, &planned))
        .map(normalize_value)
        .collect();

    let reconstructed =
        reconstruct_from_dir(&planned, dir.path(), "mixed_object_primitive").expect("reconstruct");
    let reconstructed_norm: Vec<_> = reconstructed.into_iter().map(normalize_value).collect();

    assert_eq!(expected_aligned, reconstructed_norm);
}

#[test]
fn supports_large_unsigned_64_bit_integers() {
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("large_u64.json");

    let json = r#"
[
  {"id":9223372036854775808}
]
"#;
    fs::write(&input_path, json).expect("write input");

    let format = detect_input_format(&input_path).expect("format");
    let stats = scan_all_stats(&input_path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);

    write_all_tables(&input_path, format, &planned, dir.path(), "large_u64")
        .expect("large unsigned integers should be supported");
}

#[test]
fn roundtrip_arrays_and_nested_structures() {
    // Full roundtrip across all fixtures to ensure reconstruction matches originals (order-insensitive).
    roundtrip_fixture("sample.json");
    roundtrip_fixture("nested.json");
    roundtrip_fixture("arrays.json");
    roundtrip_fixture("hetero_arrays.json");
    roundtrip_fixture("edge_cases.json");
}

fn roundtrip_fixture(fixture: &str) {
    let dir = tempdir().expect("tempdir");
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(fixture);
    let stem = input_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "input".to_string());

    let format = detect_input_format(&input_path).expect("format");
    let stats = scan_all_stats(&input_path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);
    let root_plan = planned.iter().find(|p| p.path.is_root()).unwrap().path.clone();

    write_all_tables(&input_path, format, &planned, dir.path(), &stem).expect("write parquet");

    let expected: Vec<serde_json::Value> =
        serde_json::from_reader(File::open(&input_path).unwrap()).expect("read expected json");
    let expected_aligned: Vec<_> = expected
        .into_iter()
        .map(|v| align_to_plan(v, &root_plan, &planned))
        .collect();
    let expected_norm: Vec<_> = expected_aligned.into_iter().map(normalize_value).collect();

    let reconstructed = reconstruct_from_dir(&planned, dir.path(), &stem).expect("reconstruct");
    let reconstructed_norm: Vec<_> = reconstructed.into_iter().map(normalize_value).collect();

    assert_eq!(expected_norm, reconstructed_norm, "fixture {}", fixture);
}

#[derive(Debug)]
struct RowData {
    values: std::collections::HashMap<String, serde_json::Value>,
}

fn read_table_rows(path: &PathBuf) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let iter = reader.get_row_iter(None)?;
    let mut rows = Vec::new();
    for row in iter {
        let row = row?;
        let mut map = std::collections::HashMap::new();
        for (name, field) in row.get_column_iter() {
            map.insert(name.to_string(), field_to_json(field));
        }
        rows.push(RowData { values: map });
    }
    Ok(rows)
}

fn field_to_json(field: &parquet::record::Field) -> serde_json::Value {
    use parquet::record::Field::*;
    match field {
        Null => serde_json::Value::Null,
        Bool(b) => serde_json::Value::Bool(*b),
        Byte(n) => serde_json::Value::Number((*n as i64).into()),
        UByte(n) => serde_json::Value::Number((*n as u64).into()),
        Short(n) => serde_json::Value::Number((*n as i64).into()),
        UShort(n) => serde_json::Value::Number((*n as u64).into()),
        Int(n) => serde_json::Value::Number((*n).into()),
        UInt(n) => serde_json::Value::Number((*n as u64).into()),
        Long(n) => serde_json::Value::Number((*n).into()),
        ULong(n) => serde_json::Value::Number((*n).into()),
        Float(f) => serde_json::json!(f),
        Double(f) => serde_json::json!(f),
        Str(s) => serde_json::Value::String(s.clone()),
        Bytes(b) => serde_json::Value::String(String::from_utf8_lossy(b.data()).to_string()),
        parquet::record::Field::ListInternal(list) => {
            let vals: Vec<_> = list.elements().iter().map(field_to_json).collect();
            serde_json::Value::Array(vals)
        }
        Group(g) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in g.get_column_iter() {
                obj.insert(k.to_string(), field_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    }
}

fn to_json_number(v: f64) -> serde_json::Value {
    if v.fract() == 0.0 {
        serde_json::Value::Number((v as i64).into())
    } else {
        serde_json::json!(v)
    }
}

fn reconstruct_from_dir(
    plans: &[PlannedTable],
    dir: &Path,
    stem: &str,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut table_map = std::collections::HashMap::new();
    let mut id_index: std::collections::HashMap<TablePath, std::collections::HashMap<i64, usize>> =
        std::collections::HashMap::new();
    let mut parent_index: std::collections::HashMap<
        TablePath,
        std::collections::HashMap<i64, Vec<usize>>,
    > = std::collections::HashMap::new();

    for plan in plans {
        let file_name = match plan.path.file_suffix() {
            None => format!("{stem}.parquet"),
            Some(suffix) => format!("{stem}.{suffix}.parquet"),
        };
        let path = dir.join(file_name);
        let rows = read_table_rows(&path)?;
        if let Some(idx) = build_id_index(&rows) {
            id_index.insert(plan.path.clone(), idx);
        }
        if plan.kind == PathKind::Array {
            parent_index.insert(plan.path.clone(), build_parent_index(&rows));
        }
        table_map.insert(plan.path.clone(), rows);
    }

    let root_plan = plans.iter().find(|p| p.path.is_root()).unwrap();
    let root_rows = table_map.get(&root_plan.path).unwrap();

    let mut result = Vec::new();
    for row_idx in 0..root_rows.len() {
        let obj = reconstruct_object(
            &root_plan.path,
            row_idx,
            plans,
            &table_map,
            &id_index,
            &parent_index,
        );
        result.push(obj);
    }
    Ok(result)
}

fn build_id_index(rows: &[RowData]) -> Option<std::collections::HashMap<i64, usize>> {
    let mut idx = std::collections::HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        if let Some(id) = row.values.get("_id").and_then(|v| v.as_i64()) {
            idx.insert(id, i);
        }
    }
    if idx.is_empty() {
        None
    } else {
        Some(idx)
    }
}

fn build_parent_index(rows: &[RowData]) -> std::collections::HashMap<i64, Vec<usize>> {
    let mut idx: std::collections::HashMap<i64, Vec<usize>> = std::collections::HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        if let Some(pid) = row.values.get("_parent").and_then(|v| v.as_i64()) {
            idx.entry(pid).or_default().push(i);
        }
    }
    for vec in idx.values_mut() {
        vec.sort_by_key(|&i| {
            rows[i]
                .values
                .get("_pos")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
        });
    }
    idx
}

fn reconstruct_object(
    path: &TablePath,
    row_idx: usize,
    plans: &[PlannedTable],
    tables: &std::collections::HashMap<TablePath, Vec<RowData>>,
    id_index: &std::collections::HashMap<TablePath, std::collections::HashMap<i64, usize>>,
    parent_index: &std::collections::HashMap<TablePath, std::collections::HashMap<i64, Vec<usize>>>,
) -> serde_json::Value {
    let plan = plans.iter().find(|p| p.path == *path).unwrap();
    let row = &tables[path][row_idx];
    let mut obj = serde_json::Map::new();
    let self_id = row.values.get("_id").and_then(|v| v.as_i64());

    for (prop, cols) in &plan.properties {
        let main_ty = cols.main_type;
        let primary = cols.primary.as_deref();
        let object_fk = cols.object_fk.as_deref();
        let array_count_col = cols.array_count.as_deref();
        let null_mask = cols
            .null_mask
            .as_ref()
            .and_then(|n| row.values.get(n))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let primary_val = primary.and_then(|p| row.values.get(p));
        let mut value: Option<serde_json::Value> = None;

        if let Some(count_col) = array_count_col {
            let count_val = row.values.get(count_col);
            if null_mask {
                value = Some(serde_json::Value::Null);
            } else if count_val.map(|v| !v.is_null()).unwrap_or(false) {
                if let Some(parent_id) = self_id {
                    let child_path = path.child_array(prop);
                    let children = parent_index
                        .get(&child_path)
                        .and_then(|m| m.get(&parent_id))
                        .cloned()
                        .unwrap_or_default();
                    let mut arr = Vec::new();
                    for idx in children {
                        arr.push(reconstruct_array_elem(
                            &child_path,
                            idx,
                            plans,
                            tables,
                            id_index,
                            parent_index,
                        ));
                    }
                    value = Some(serde_json::Value::Array(arr));
                }
            }

            if value.is_some() {
                if let Some(v) = value {
                    obj.insert(prop.clone(), v);
                }
                continue;
            }
        }

        if let Some(object_fk_col) = object_fk {
            if null_mask {
                value = Some(serde_json::Value::Null);
            } else if let Some(fk) = row.values.get(object_fk_col).and_then(|v| v.as_i64()) {
                let child_path = path.child_object(prop);
                if let Some(idx) = id_index
                    .get(&child_path)
                    .and_then(|m| m.get(&fk))
                    .copied()
                {
                    value = Some(reconstruct_object(
                        &child_path,
                        idx,
                        plans,
                        tables,
                        id_index,
                        parent_index,
                    ));
                }
            }
        }

        if value.is_some() {
            if let Some(v) = value {
                obj.insert(prop.clone(), v);
            }
            continue;
        }

        match main_ty {
            Some(SimpleType::Bool) => {
                if null_mask {
                    value = Some(serde_json::Value::Null);
                } else if let Some(v) = primary_val.and_then(|v| v.as_bool()) {
                    value = Some(serde_json::Value::Bool(v));
                }
            }
            Some(SimpleType::Integer) => {
                if null_mask {
                    value = Some(serde_json::Value::Null);
                } else if let Some(v) = primary_val.and_then(|v| v.as_i64()) {
                    value = Some(serde_json::Value::Number(v.into()));
                } else if let Some((ty, col_name)) = cols.alternates.iter().next() {
                    if *ty == SimpleType::Number {
                        if let Some(v) = row.values.get(col_name).and_then(|v| v.as_f64()) {
                            value = Some(to_json_number(v));
                        }
                    }
                }
            }
            Some(SimpleType::Number) => {
                if null_mask {
                    value = Some(serde_json::Value::Null);
                } else if let Some(v) = primary_val.and_then(|v| v.as_f64()) {
                    value = Some(to_json_number(v));
                }
            }
            Some(SimpleType::String) => {
                if null_mask {
                    value = Some(serde_json::Value::Null);
                } else if let Some(v) = primary_val.and_then(|v| v.as_str().map(|s| s.to_string())) {
                    value = Some(serde_json::Value::String(v));
                }
            }
            _ => {}
        };

        if let Some(v) = value {
            obj.insert(prop.clone(), v);
        }
    }

    serde_json::Value::Object(obj)
}

fn reconstruct_array_elem(
    path: &TablePath,
    row_idx: usize,
    plans: &[PlannedTable],
    tables: &std::collections::HashMap<TablePath, Vec<RowData>>,
    id_index: &std::collections::HashMap<TablePath, std::collections::HashMap<i64, usize>>,
    parent_index: &std::collections::HashMap<TablePath, std::collections::HashMap<i64, Vec<usize>>>,
) -> serde_json::Value {
    let plan = plans.iter().find(|p| p.path == *path).unwrap();
    let row = &tables[path][row_idx];
    let self_id = row.values.get("_id").and_then(|v| v.as_i64());

    if plan.properties.is_empty() {
        return serde_json::Value::Null;
    }

    let property_present = |cols: &PropertyColumns, row: &RowData| -> bool {
        let mask = cols
            .null_mask
            .as_ref()
            .and_then(|n| row.values.get(n))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let primary_present = cols
            .primary
            .as_ref()
            .and_then(|p| row.values.get(p))
            .map(|v| !v.is_null())
            .unwrap_or(false);
        let object_present = cols
            .object_fk
            .as_ref()
            .and_then(|p| row.values.get(p))
            .map(|v| !v.is_null())
            .unwrap_or(false);
        let array_present = cols
            .array_count
            .as_ref()
            .and_then(|p| row.values.get(p))
            .map(|v| !v.is_null())
            .unwrap_or(false);
        let alt_present = cols.alternates.values().any(|name| {
            row.values
                .get(name)
                .map(|v| !v.is_null())
                .unwrap_or(false)
        });
        mask || primary_present || object_present || array_present || alt_present
    };

    let has_non_value_data = plan
        .properties
        .iter()
        .filter(|(prop, _)| prop.as_str() != "value")
        .any(|(_, cols)| property_present(cols, row));

    if let Some(prop_cols) = plan.properties.get("value") {
        if !has_non_value_data {
            let null_mask = prop_cols
                .null_mask
                .as_ref()
                .and_then(|n| row.values.get(n))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let primary = prop_cols.primary.as_deref();
            let main_ty = prop_cols.main_type;
            let array_count_col = prop_cols.array_count.as_deref();
            let count_val = array_count_col.and_then(|c| row.values.get(c));
            let is_array_count = count_val.map(|v| !v.is_null()).unwrap_or(false);

            let mut value: Option<serde_json::Value> = None;

            if is_array_count {
                if null_mask {
                    value = Some(serde_json::Value::Null);
                } else if let Some(self_id) = self_id {
                    let child_path = path.child_array("value");
                    let children = parent_index
                        .get(&child_path)
                        .and_then(|m| m.get(&self_id))
                        .cloned()
                        .unwrap_or_default();
                    let mut arr = Vec::new();
                    for idx in children {
                        arr.push(reconstruct_array_elem(
                            &child_path,
                            idx,
                            plans,
                            tables,
                            id_index,
                            parent_index,
                        ));
                    }
                    value = Some(serde_json::Value::Array(arr));
                }
            } else {
                let primary_val = primary.and_then(|p| row.values.get(p));
                value = match main_ty {
                    Some(SimpleType::Bool) => primary_val
                        .and_then(|v| v.as_bool())
                        .map(serde_json::Value::Bool),
                    Some(SimpleType::Integer) => primary_val
                        .and_then(|v| v.as_i64())
                        .map(|v| serde_json::Value::Number(v.into())),
                    Some(SimpleType::Number) => primary_val
                        .and_then(|v| v.as_f64())
                        .map(to_json_number),
                    Some(SimpleType::String) => primary_val
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .map(serde_json::Value::String),
                    _ => None,
                };

                if value.is_none() {
                    for (ty, name) in &prop_cols.alternates {
                        let candidate = row.values.get(name);
                        let alt_val = match ty {
                            SimpleType::Bool => candidate.and_then(|v| v.as_bool()).map(serde_json::Value::Bool),
                            SimpleType::Integer => candidate
                                .and_then(|v| v.as_i64())
                                .map(|v| serde_json::Value::Number(v.into())),
                            SimpleType::Number => candidate
                                .and_then(|v| v.as_f64())
                                .map(to_json_number),
                            SimpleType::String => candidate
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                                .map(serde_json::Value::String),
                            _ => None,
                        };
                        if alt_val.is_some() {
                            value = alt_val;
                            break;
                        }
                    }
                }

                if null_mask && value.is_none() {
                    value = Some(serde_json::Value::Null);
                }
            }

            if let Some(v) = value {
                return v;
            }
        }
    }

    // Otherwise treat as object element.
    let mut obj = serde_json::Map::new();
    for (prop, cols) in &plan.properties {
        if prop == "value" {
            continue;
        }
        if !property_present(cols, row) {
            continue;
        }
        let null_mask = cols
            .null_mask
            .as_ref()
            .and_then(|n| row.values.get(n))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let primary = cols.primary.as_deref();
        let object_fk = cols.object_fk.as_deref();
        let array_count_col = cols.array_count.as_deref();
        let main_ty = cols.main_type;

        if let Some(count_col) = array_count_col {
            let count_val = row.values.get(count_col);
            if null_mask {
                obj.insert(prop.clone(), serde_json::Value::Null);
                continue;
            } else if count_val.map(|v| !v.is_null()).unwrap_or(false) {
                if let Some(parent_id) = self_id {
                    let child_path = path.child_array(prop);
                    let children = parent_index
                        .get(&child_path)
                        .and_then(|m| m.get(&parent_id))
                        .cloned()
                        .unwrap_or_default();
                    let mut arr = Vec::new();
                    for idx in children {
                        arr.push(reconstruct_array_elem(
                            &child_path,
                            idx,
                            plans,
                            tables,
                            id_index,
                            parent_index,
                        ));
                    }
                    obj.insert(prop.clone(), serde_json::Value::Array(arr));
                    continue;
                }
            }
        }

        if let Some(object_fk_col) = object_fk {
            if null_mask {
                obj.insert(prop.clone(), serde_json::Value::Null);
                continue;
            } else if let Some(fk) = row.values.get(object_fk_col).and_then(|v| v.as_i64()) {
                let child_path = path.child_object(prop);
                if let Some(idx) = id_index
                    .get(&child_path)
                    .and_then(|m| m.get(&fk))
                    .copied()
                {
                    obj.insert(
                        prop.clone(),
                        reconstruct_object(&child_path, idx, plans, tables, id_index, parent_index),
                    );
                    continue;
                }
            }
        }

        let value = match main_ty {
            Some(SimpleType::Bool) => primary
                .and_then(|p| row.values.get(p))
                .and_then(|v| v.as_bool())
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            Some(SimpleType::Integer) => primary
                .and_then(|p| row.values.get(p))
                .and_then(|v| v.as_i64())
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            Some(SimpleType::Number) => primary
                .and_then(|p| row.values.get(p))
                .and_then(|v| v.as_f64())
                .map(to_json_number)
                .unwrap_or(serde_json::Value::Null),
            Some(SimpleType::String) => primary
                .and_then(|p| row.values.get(p))
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        };
        if null_mask || !value.is_null() {
            obj.insert(prop.clone(), if null_mask { serde_json::Value::Null } else { value });
        }
    }

    serde_json::Value::Object(obj)
}

fn lookup_count(stats: &PropertyStats, name: &str, ty: SimpleType) -> usize {
    stats
        .counts
        .get(&FieldKey {
            name: name.to_owned(),
            ty,
        })
        .copied()
        .unwrap_or(0)
}

fn normalize_value(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut bmap = BTreeMap::new();
            for (k, v) in map {
                bmap.insert(k, normalize_value(v));
            }
            serde_json::Value::Object(bmap.into_iter().collect())
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(normalize_value).collect())
        }
        other => other,
    }
}

fn align_to_plan(
    value: serde_json::Value,
    path: &TablePath,
    plans: &[PlannedTable],
) -> serde_json::Value {
    let plan = plans.iter().find(|p| p.path == *path).unwrap();
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if let Some(cols) = plan.properties.get(&k) {
                    if cols.object_fk.is_some() && v.is_object() {
                        let child_path = path.child_object(&k);
                        out.insert(k, align_to_plan(v, &child_path, plans));
                    } else if cols
                        .array_count
                        .as_ref()
                        .or_else(|| cols.primary.as_ref().filter(|p| p.ends_with("|array")))
                        .is_some()
                    {
                        match v {
                            serde_json::Value::Array(arr) => {
                                let child_path = path.child_array(&k);
                                let aligned: Vec<_> = arr
                                    .into_iter()
                                    .map(|elem| align_to_plan(elem, &child_path, plans))
                                    .collect();
                                out.insert(k, serde_json::Value::Array(aligned));
                            }
                            serde_json::Value::Null => {
                                out.insert(k, serde_json::Value::Null);
                            }
                            _ => {}
                        }
                    } else {
                        out.insert(k, v);
                    }
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(arr) => {
            let child_path = path.child_array("value");
            let aligned: Vec<_> = arr
                .into_iter()
                .map(|elem| {
                    if elem.is_object() || elem.is_array() {
                        align_to_plan(elem, &child_path, plans)
                    } else {
                        elem
                    }
                })
                .collect();
            serde_json::Value::Array(aligned)
        }
        other => other,
    }
}

#[test]
fn exports_parquet_with_expected_schema_and_values() {
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("input.json");

    // Small golden test to assert column names, null-mask behavior, and value placement.
    let json = r#"
[
  {"id":1,"name":"a","score":1.5,"flag":true,"note":null},
  {"id":2,"name":"b","score":2,"flag":null,"note":"x"}
]
"#;
    fs::write(&input_path, json).expect("write input");

    let format = detect_input_format(&input_path).expect("format");
    let stats = scan_all_stats(&input_path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);

    write_all_tables(&input_path, format, &planned, dir.path(), "input").expect("write parquet");

    let output_path = dir.path().join("input.parquet");

    let reader = SerializedFileReader::new(File::open(&output_path).unwrap()).unwrap();
    let meta = reader.metadata().file_metadata();
    assert_eq!(meta.num_rows(), 2);

    let names: Vec<String> = meta
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.path().string())
        .collect();
    assert_eq!(
        names,
        vec![
            "_id",
            "id",
            "name",
            "score",
            "flag",
            "flag|n",
            "note",
            "note|n"
        ]
    );

    let idx = |col: &str| names.iter().position(|n| n == col).unwrap();

    let mut rows = reader.get_row_iter(None).unwrap();
    let r1 = rows.next().unwrap().unwrap();
    let r2 = rows.next().unwrap().unwrap();
    assert!(rows.next().is_none());

    // Row 1: score is float -> goes to main number column; note is null -> null mask set.
    assert_eq!(r1.get_bool(idx("flag")).ok(), Some(true));
    assert_eq!(r1.get_bool(idx("flag|n")).ok(), Some(false));
    assert_eq!(r1.get_long(idx("id")).ok(), Some(1));
    assert_eq!(r1.get_string(idx("name")).ok().map(String::as_str), Some("a"));
    assert_eq!(r1.get_double(idx("score")).ok(), Some(1.5));
    assert_eq!(r1.get_string(idx("note")).ok().map(String::as_str), None);
    assert_eq!(r1.get_bool(idx("note|n")).ok(), Some(true));

    // Row 2: score is integer -> main column; flag is explicit null -> null mask set.
    // Some readers may decode null bools as false; rely on mask to disambiguate nulls.
    assert!(matches!(r2.get_bool(idx("flag")).ok(), None | Some(false)));
    assert_eq!(r2.get_bool(idx("flag|n")).ok(), Some(true));
    assert_eq!(r2.get_long(idx("id")).ok(), Some(2));
    assert_eq!(r2.get_string(idx("name")).ok().map(String::as_str), Some("b"));
    assert_eq!(r2.get_double(idx("score")).ok(), Some(2.0));
    assert_eq!(r2.get_string(idx("note")).ok().map(String::as_str), Some("x"));
    assert_eq!(r2.get_bool(idx("note|n")).ok(), Some(false));
}

#[test]
fn emits_exasol_schema_with_keys() {
    // Verify Exasol DDL generation includes PK/FK constraints for nested objects.
    let dir = tempdir().expect("tempdir");
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nested.json");
    let stem = "nested";

    let format = detect_input_format(&input_path).expect("format");
    let stats = scan_all_stats(&input_path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);

    write_sql_schema(&planned, dir.path(), stem).expect("schema");
    let ddl = std::fs::read_to_string(dir.path().join(format!("{stem}.sql"))).expect("read ddl");

    assert!(
        ddl.contains("CREATE TABLE \"nested\""),
        "root table missing"
    );
    assert!(
        ddl.contains("CONSTRAINT \"pk_nested\" PRIMARY KEY (\"_id\") DISABLE"),
        "root PK should be emitted as metadata-only in Exasol"
    );
    assert!(
        ddl.contains("FOREIGN KEY (\"child|object\") REFERENCES \"nested_child\"(\"_id\") DISABLE"),
        "FK to child should be emitted as metadata-only in Exasol"
    );
    assert!(
        ddl.contains("CREATE TABLE \"nested_child\""),
        "child table missing"
    );
    assert!(
        ddl.contains("\"child|n\" BOOLEAN NOT NULL"),
        "null mask should be NOT NULL"
    );
}

#[test]
fn exasol_import_uses_schema_table_names() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nested.json");
    let stem = "nested";

    let format = detect_input_format(&path).expect("format");
    let stats = scan_all_stats(&path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);

    let (create_stmts, _) = build_sql_schema(&planned, stem);
    let mut created_tables: Vec<String> = create_stmts
        .iter()
        .filter_map(|stmt| {
            let needle = "CREATE TABLE ";
            stmt.find(needle).map(|idx| {
                let start = idx + needle.len();
                let rest = &stmt[start..];
                rest.split_whitespace().next().unwrap_or("").trim().to_string()
            })
        })
        .collect();
    created_tables.sort();

    let mut import_tables: Vec<String> = planned
        .iter()
        .map(|plan| table_sql_name(&plan.path, stem))
        .collect();
    import_tables.sort();

    assert_eq!(created_tables, import_tables);
}

#[test]
fn exasol_constraints_are_emitted_disabled_explicitly() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nested.json");
    let stem = "nested";

    let format = detect_input_format(&path).expect("format");
    let stats = scan_all_stats(&path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);

    let (_create_stmts, constraint_stmts) = build_sql_schema(&planned, stem);
    assert!(
        constraint_stmts.iter().all(|stmt| stmt.contains(" DISABLE;")),
        "all Exasol PK/FK constraints should have explicit DISABLE semantics"
    );
}

#[test]
fn writes_source_manifest_with_expected_tables_and_relationships() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nested.json");
    let dir = tempdir().expect("tempdir");
    let output_path = dir.path().join("nested.manifest.json");

    let format = detect_input_format(&path).expect("format");
    let stats = scan_all_stats(&path, format).expect("scan");
    let planned = build_all_schema_plans(&stats);
    write_source_manifest(&planned, &output_path, "NESTED").expect("manifest");

    let manifest: Value = serde_json::from_str(&fs::read_to_string(&output_path).expect("read manifest"))
        .expect("parse manifest");
    assert_eq!(manifest["format"], Value::String("exasol-json-tables-source-manifest".to_string()));
    assert_eq!(manifest["stem"], Value::String("NESTED".to_string()));

    let table_names: Vec<String> = manifest["tables"]
        .as_array()
        .expect("tables array")
        .iter()
        .map(|table| table["tableName"].as_str().expect("tableName").to_string())
        .collect();
    assert!(table_names.contains(&"NESTED".to_string()));
    assert!(table_names.contains(&"NESTED_child".to_string()));
    assert!(table_names.contains(&"NESTED_meta".to_string()));
    assert!(table_names.contains(&"NESTED_meta_info".to_string()));

    let relationships = manifest["relationships"].as_array().expect("relationships array");
    assert!(relationships.iter().any(|relationship| {
        relationship["parentTable"] == Value::String("NESTED".to_string())
            && relationship["childTable"] == Value::String("NESTED_child".to_string())
            && relationship["segmentName"] == Value::String("child".to_string())
            && relationship["relationKind"] == Value::String("object".to_string())
    }));
}

const DEFAULT_EXASOL_E2E_BASE_URL: &str =
    "exasol://sys:exasol@127.0.0.1:8563?tls=1&validateservercertificate=0";

struct ExasolE2eSchema {
    base_url: String,
    schema: String,
}

impl ExasolE2eSchema {
    fn create(test_name: &str) -> Result<Self, DynError> {
        let schema = unique_exasol_schema_name(test_name);
        let this = Self {
            base_url: exasol_e2e_base_url(),
            schema,
        };
        exasol_execute_update(&this.base_url, &format!("CREATE SCHEMA {}", this.schema))?;
        Ok(this)
    }

    fn schema_url(&self) -> String {
        exasol_url_with_schema(&self.base_url, &self.schema)
    }
}

impl Drop for ExasolE2eSchema {
    fn drop(&mut self) {
        let _ = exasol_execute_update(&self.base_url, &format!("DROP SCHEMA {} CASCADE", self.schema));
    }
}

fn exasol_e2e_base_url() -> String {
    env::var("JSON_TO_PARQUET_EXASOL_BASE_URL")
        .unwrap_or_else(|_| DEFAULT_EXASOL_E2E_BASE_URL.to_string())
}

fn exasol_url_with_schema(base_url: &str, schema: &str) -> String {
    let (head, query) = base_url
        .split_once('?')
        .map(|(h, q)| (h.trim_end_matches('/'), Some(q)))
        .unwrap_or((base_url.trim_end_matches('/'), None));
    match query {
        Some(query) => format!("{head}/{schema}?{query}"),
        None => format!("{head}/{schema}"),
    }
}

fn unique_exasol_schema_name(test_name: &str) -> String {
    let mut token: String = test_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    while token.contains("__") {
        token = token.replace("__", "_");
    }
    let token = token.trim_matches('_');
    let token = if token.is_empty() { "E2E" } else { token };
    let token: String = token.chars().take(24).collect();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("JTP_{}_{}_{}", token, std::process::id(), nonce)
}

fn exasol_execute_update(url: &str, sql: &str) -> Result<i64, DynError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut connection = connect_exasol(url).await?;
        let row_count = connection.execute_update(sql.to_owned()).await?;
        connection.close().await?;
        Ok::<i64, DynError>(row_count)
    })
}

fn exasol_query_string_rows(url: &str, sql: &str) -> Result<Vec<Vec<Option<String>>>, DynError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut connection = connect_exasol(url).await?;
        let batches = connection.query(sql.to_owned()).await?;
        connection.close().await?;
        Ok::<Vec<Vec<Option<String>>>, DynError>(record_batches_to_string_rows(&batches))
    })
}

fn record_batches_to_string_rows(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns: Vec<&StringArray> = batch
            .columns()
            .iter()
            .map(|col| {
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .expect("queries must cast selected columns to VARCHAR for e2e assertions")
            })
            .collect();

        for row_idx in 0..batch.num_rows() {
            rows.push(
                columns
                    .iter()
                    .map(|col| {
                        if col.is_null(row_idx) {
                            None
                        } else {
                            Some(col.value(row_idx).to_string())
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

#[test]
#[ignore = "requires a running Exasol instance; uses JSON_TO_PARQUET_EXASOL_BASE_URL or local ExaNano defaults"]
fn exasol_e2e_imports_nested_objects_into_related_tables() {
    let schema = ExasolE2eSchema::create("nested_objects").expect("create e2e schema");
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("nested.json");
    let temp = tempdir().expect("tempdir");
    let staging_dir = temp.path().join("staging");

    run(Args {
        input: input_path,
        output_dir: None,
        schema_sql: false,
        manifest_output: None,
        exasol: Some(schema.schema_url()),
        exasol_temp_dir: Some(staging_dir.clone()),
        exasol_cleanup: true,
    })
    .expect("import into exasol");

    assert!(
        !staging_dir.exists(),
        "expected Exasol staging dir to be cleaned up after successful import"
    );

    let counts = exasol_query_string_rows(
        &schema.schema_url(),
        r#"
        SELECT
          CAST((SELECT COUNT(*) FROM "nested") AS VARCHAR(20)),
          CAST((SELECT COUNT(*) FROM "nested_child") AS VARCHAR(20)),
          CAST((SELECT COUNT(*) FROM "nested_meta") AS VARCHAR(20)),
          CAST((SELECT COUNT(*) FROM "nested_meta_info") AS VARCHAR(20)),
          CAST((SELECT COUNT(*) FROM "nested_alt_child") AS VARCHAR(20))
        FROM DUAL
        "#,
    )
    .expect("query imported table counts");
    assert_eq!(
        counts,
        vec![vec![
            Some("3".to_string()),
            Some("2".to_string()),
            Some("3".to_string()),
            Some("1".to_string()),
            Some("2".to_string()),
        ]]
    );

    let rows = exasol_query_string_rows(
        &schema.schema_url(),
        r#"
        SELECT
          CAST(r."id" AS VARCHAR(20)),
          r."name",
          CAST(c."a" AS VARCHAR(20)),
          c."b",
          CAST(c."c" AS VARCHAR(10)),
          CAST(a."d" AS VARCHAR(20)),
          i."note"
        FROM "nested" r
        LEFT JOIN "nested_child" c
          ON r."child|object" = c."_id"
        LEFT JOIN "nested_alt_child" a
          ON r."alt_child|object" = a."_id"
        LEFT JOIN "nested_meta" m
          ON r."meta|object" = m."_id"
        LEFT JOIN "nested_meta_info" i
          ON m."info|object" = i."_id"
        ORDER BY CAST(r."id" AS DECIMAL(18,0))
        "#,
    )
    .expect("query nested import results");
    assert_eq!(
        rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("parent1".to_string()),
                Some("10".to_string()),
                Some("x".to_string()),
                None,
                None,
                None,
            ],
            vec![
                Some("2".to_string()),
                Some("parent2".to_string()),
                Some("20".to_string()),
                None,
                Some("TRUE".to_string()),
                Some("3.14".to_string()),
                None,
            ],
            vec![
                Some("3".to_string()),
                Some("parent3".to_string()),
                None,
                None,
                None,
                None,
                Some("deep".to_string()),
            ],
        ]
    );
}

#[test]
#[ignore = "requires a running Exasol instance; uses JSON_TO_PARQUET_EXASOL_BASE_URL or local ExaNano defaults"]
fn exasol_e2e_imports_ndjson_and_preserves_array_order() {
    let schema = ExasolE2eSchema::create("sample_ndjson").expect("create e2e schema");
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sample.ndjson");
    let out_dir = tempdir().expect("tempdir");

    run(Args {
        input: input_path,
        output_dir: Some(out_dir.path().to_path_buf()),
        schema_sql: false,
        manifest_output: None,
        exasol: Some(schema.schema_url()),
        exasol_temp_dir: None,
        exasol_cleanup: false,
    })
    .expect("import ndjson into exasol");

    let root_rows = exasol_query_string_rows(
        &schema.schema_url(),
        r#"
        SELECT
          CAST("id" AS VARCHAR(20)),
          "name",
          CASE WHEN "score" IS NULL THEN '0' ELSE '1' END,
          CASE WHEN COALESCE("age|n", FALSE) THEN '1' ELSE '0' END,
          CAST("tags|array" AS VARCHAR(20)),
          CASE WHEN COALESCE("note|n", FALSE) THEN '1' ELSE '0' END
        FROM "sample"
        ORDER BY CAST("id" AS DECIMAL(18,0))
        "#,
    )
    .expect("query ndjson root table");
    assert_eq!(
        root_rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("Alice".to_string()),
                Some("1".to_string()),
                Some("0".to_string()),
                Some("2".to_string()),
                Some("1".to_string()),
            ],
            vec![
                Some("2".to_string()),
                Some("Bob".to_string()),
                Some("1".to_string()),
                Some("1".to_string()),
                Some("0".to_string()),
                Some("0".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("Carol".to_string()),
                Some("0".to_string()),
                Some("0".to_string()),
                Some("1".to_string()),
                Some("0".to_string()),
            ],
        ]
    );

    let tag_rows = exasol_query_string_rows(
        &schema.schema_url(),
        r#"
        SELECT
          CAST(r."id" AS VARCHAR(20)),
          CAST(t."_pos" AS VARCHAR(20)),
          t."_value"
        FROM "sample" r
        JOIN "sample_tags_arr" t
          ON r."_id" = t."_parent"
        ORDER BY CAST(r."id" AS DECIMAL(18,0)), CAST(t."_pos" AS DECIMAL(18,0))
        "#,
    )
    .expect("query imported array rows");
    assert_eq!(
        tag_rows,
        vec![
            vec![
                Some("1".to_string()),
                Some("0".to_string()),
                Some("a".to_string()),
            ],
            vec![
                Some("1".to_string()),
                Some("1".to_string()),
                Some("b".to_string()),
            ],
            vec![
                Some("3".to_string()),
                Some("0".to_string()),
                Some("c".to_string()),
            ],
        ]
    );
}

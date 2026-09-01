//! Unit tests for the seams this crate exists to provide: reading from a stream,
//! and writing through a sink that is not the in-memory one.

use std::collections::HashMap;
use std::io::Cursor;

use serde_json::{json, Value};

use crate::buffer::{ColumnBuffers, ColumnValues};
use crate::contract::{PlannedTable, SimpleType, TablePath};
use crate::error::CoreResult;
use crate::infer::{build_all_schema_plans, StatsCollector};
use crate::read::{detect_format, for_each_document, InputFormat};
use crate::sink::{write_document, RowRef, RowSink};

const ARRAY_INPUT: &str = r#"[
  {"id": 1, "name": "Cafe", "hours": {"mon": "9-5"}, "tags": ["coffee", "wifi"]},
  {"id": 2, "name": "Diner", "hours": {"mon": null}, "tags": []}
]"#;

const NDJSON_INPUT: &str = concat!(
    "{\"id\": 1, \"tags\": [\"a\"]}\n",
    "\n",
    "{\"id\": 2, \"tags\": [\"b\", \"c\"]}\n"
);

fn plan_for(input: &str) -> Vec<PlannedTable> {
    let mut reader = Cursor::new(input.as_bytes());
    let format = detect_format(&mut reader).expect("format");
    let mut stats = StatsCollector::new();
    for_each_document(reader, format, |_, doc| {
        stats.record_document(doc);
        Ok(())
    })
    .expect("scan");
    build_all_schema_plans(&stats.finish())
}

fn load(input: &str, plans: &[PlannedTable]) -> ColumnBuffers {
    let mut reader = Cursor::new(input.as_bytes());
    let format = detect_format(&mut reader).expect("format");
    let mut sink = ColumnBuffers::new(plans);
    for_each_document(reader, format, |_, doc| write_document(&mut sink, doc)).expect("write");
    sink
}

#[test]
fn detects_array_and_line_framing_from_a_stream() {
    let mut array = Cursor::new(b"  \n [ {\"a\": 1} ]".as_slice());
    assert_eq!(detect_format(&mut array).unwrap(), InputFormat::Array);

    let mut lines = Cursor::new(b"{\"a\": 1}\n".as_slice());
    assert_eq!(detect_format(&mut lines).unwrap(), InputFormat::Lines);
}

#[test]
fn detect_format_leaves_the_stream_readable() {
    // The reader is not rewound or reopened, so a stream can be framed and then
    // consumed by the same pass — the property the tunnel/HTTP source needs.
    let mut reader = Cursor::new(b"\n\n  [{\"id\": 7}]".as_slice());
    let format = detect_format(&mut reader).expect("format");

    let mut seen = Vec::new();
    for_each_document(reader, format, |idx, doc| {
        seen.push((idx, doc.get("id").cloned().unwrap()));
        Ok(())
    })
    .expect("documents");

    assert_eq!(seen, vec![(0, json!(7))]);
}

#[test]
fn empty_input_is_rejected() {
    let mut reader = Cursor::new(b"   \n".as_slice());
    let err = detect_format(&mut reader).unwrap_err();
    assert!(err.to_string().contains("empty"), "{err}");
}

#[test]
fn ndjson_skips_blank_lines_and_reports_positions() {
    let mut reader = Cursor::new(NDJSON_INPUT.as_bytes());
    let format = detect_format(&mut reader).expect("format");
    let mut positions = Vec::new();
    for_each_document(reader, format, |idx, _| {
        positions.push(idx);
        Ok(())
    })
    .expect("documents");
    // Blank line 1 is skipped; positions stay line-based.
    assert_eq!(positions, vec![0, 2]);
}

#[test]
fn non_object_entries_are_rejected_with_position() {
    let mut reader = Cursor::new(b"[{\"a\": 1}, 42]".as_slice());
    let format = detect_format(&mut reader).expect("format");
    let err = for_each_document(reader, format, |_, _| Ok(())).unwrap_err();
    assert!(err.to_string().contains("index 1"), "{err}");
}

#[test]
fn planning_from_a_stream_matches_the_documented_shape() {
    let plans = plan_for(ARRAY_INPUT);
    let mut paths: Vec<String> = plans.iter().map(|p| p.path.to_string()).collect();
    paths.sort();
    assert_eq!(paths, vec!["hours", "root", "tags[]"]);

    let root = plans.iter().find(|p| p.path.is_root()).unwrap();
    let names: Vec<&str> = root.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["_id", "id", "name", "hours|object", "tags|array"]
    );

    let hours = plans
        .iter()
        .find(|p| p.path.to_string() == "hours")
        .unwrap();
    let names: Vec<&str> = hours.columns.iter().map(|c| c.name.as_str()).collect();
    // Explicit null keeps its mask column alongside the value column.
    assert_eq!(names, vec!["_id", "mon", "mon|n"]);
}

#[test]
fn buffered_sink_assigns_identities_and_links_children() {
    let plans = plan_for(ARRAY_INPUT);
    let buffers = load(ARRAY_INPUT, &plans);

    let root = buffers.table(&TablePath::root()).unwrap();
    assert_eq!(root.row_count(), 2);
    match root.columns.get("_id").unwrap() {
        ColumnValues::Int(v) => assert_eq!(v, &vec![Some(1), Some(2)]),
        other => panic!("unexpected column kind: {other:?}"),
    }
    match root.columns.get("tags|array").unwrap() {
        ColumnValues::Int(v) => assert_eq!(v, &vec![Some(2), Some(0)]),
        other => panic!("unexpected column kind: {other:?}"),
    }

    let tags = buffers
        .table(&TablePath::root().child_array("tags"))
        .unwrap();
    assert_eq!(tags.row_count(), 2);
    match tags.columns.get("_parent").unwrap() {
        ColumnValues::Int(v) => assert_eq!(v, &vec![Some(1), Some(1)]),
        other => panic!("unexpected column kind: {other:?}"),
    }
    match tags.columns.get("_pos").unwrap() {
        ColumnValues::Int(v) => assert_eq!(v, &vec![Some(0), Some(1)]),
        other => panic!("unexpected column kind: {other:?}"),
    }

    // Explicit null sets the mask rather than leaving the value merely absent.
    let hours = buffers
        .table(&TablePath::root().child_object("hours"))
        .unwrap();
    match hours.columns.get("mon|n").unwrap() {
        ColumnValues::BoolMask(v) => assert_eq!(v, &vec![false, true]),
        other => panic!("unexpected column kind: {other:?}"),
    }
}

/// A sink that keeps one open row per table and flushes it when the next row for
/// that table starts — the shape a streaming loader (CSV, `EMITS`) would use.
/// Its presence is the point: it proves the traversal never writes backwards.
#[derive(Default)]
struct StreamingSink {
    plans: HashMap<TablePath, PlannedTable>,
    open: HashMap<TablePath, (RowRef, HashMap<String, Value>)>,
    next_id: HashMap<TablePath, i64>,
    flushed: Vec<(String, HashMap<String, Value>)>,
    max_open_rows: usize,
}

impl StreamingSink {
    fn new(plans: &[PlannedTable]) -> Self {
        Self {
            plans: plans.iter().map(|p| (p.path.clone(), p.clone())).collect(),
            ..Default::default()
        }
    }

    fn flush_open(&mut self, path: &TablePath) {
        if let Some((_, values)) = self.open.remove(path) {
            self.flushed.push((path.to_string(), values));
        }
    }

    fn finish(mut self) -> Vec<(String, HashMap<String, Value>)> {
        let paths: Vec<TablePath> = self.open.keys().cloned().collect();
        for path in paths {
            self.flush_open(&path);
        }
        self.flushed
    }

    fn put(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: Value,
        strict: bool,
    ) -> CoreResult<()> {
        let plan = self
            .plans
            .get(path)
            .ok_or_else(|| crate::CoreError::msg(format!("no plan for {path}")))?;
        if !plan.columns.iter().any(|c| c.name == column) {
            if strict {
                return Err(crate::CoreError::msg(format!(
                    "Missing column data for {column}"
                )));
            }
            return Ok(());
        }
        let (open_row, values) = self
            .open
            .get_mut(path)
            .ok_or_else(|| crate::CoreError::msg(format!("no open row for {path}")))?;
        // The invariant the sink contract promises: writes only ever target the
        // row this table most recently started.
        assert_eq!(*open_row, row, "write to a row that is no longer open");
        values.insert(column.to_string(), value);
        Ok(())
    }
}

impl RowSink for StreamingSink {
    fn plan(&self, path: &TablePath) -> CoreResult<&PlannedTable> {
        self.plans
            .get(path)
            .ok_or_else(|| crate::CoreError::msg(format!("no plan for {path}")))
    }

    fn start_row(&mut self, path: &TablePath) -> CoreResult<RowRef> {
        self.flush_open(path);
        let has_id = self
            .plan(path)?
            .columns
            .iter()
            .any(|c| c.name == "_id" && c.is_required);
        let counter = self.next_id.entry(path.clone()).or_insert(1);
        let id = if has_id {
            let id = *counter;
            *counter += 1;
            Some(id)
        } else {
            None
        };
        let row = RowRef {
            index: (*counter - 1) as usize,
            id,
        };
        let mut values = HashMap::new();
        if let Some(id) = id {
            values.insert("_id".to_string(), json!(id));
        }
        self.open.insert(path.clone(), (row, values));
        self.max_open_rows = self.max_open_rows.max(self.open.len());
        Ok(row)
    }

    fn set_bool(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: bool,
    ) -> CoreResult<()> {
        self.put(path, column, row, json!(value), false)
    }

    fn set_int(
        &mut self,
        path: &TablePath,
        column: &str,
        row: RowRef,
        value: i64,
    ) -> CoreResult<()> {
        self.put(path, column, row, json!(value), false)
    }

    fn set_scalar(
        &mut self,
        path: &TablePath,
        column: &str,
        _ty: SimpleType,
        value: &Value,
        row: RowRef,
    ) -> CoreResult<()> {
        self.put(path, column, row, value.clone(), true)
    }
}

#[test]
fn a_streaming_sink_can_back_the_same_traversal() {
    let plans = plan_for(ARRAY_INPUT);
    let mut sink = StreamingSink::new(&plans);
    let mut reader = Cursor::new(ARRAY_INPUT.as_bytes());
    let format = detect_format(&mut reader).expect("format");
    for_each_document(reader, format, |_, doc| write_document(&mut sink, doc)).expect("write");

    // At most one open row per table in the family — never more.
    assert!(sink.max_open_rows <= plans.len());
    let rows = sink.finish();

    let root_rows: Vec<_> = rows.iter().filter(|(path, _)| path == "root").collect();
    assert_eq!(root_rows.len(), 2);
    assert_eq!(root_rows[0].1.get("name"), Some(&json!("Cafe")));
    assert_eq!(root_rows[0].1.get("tags|array"), Some(&json!(2)));
    assert_eq!(root_rows[0].1.get("hours|object"), Some(&json!(1)));

    let tag_rows: Vec<_> = rows.iter().filter(|(path, _)| path == "tags[]").collect();
    assert_eq!(tag_rows.len(), 2);
    assert_eq!(tag_rows[1].1.get("_pos"), Some(&json!(1)));
    assert_eq!(tag_rows[1].1.get("_value"), Some(&json!("wifi")));
}

#[test]
fn streaming_and_buffered_sinks_agree_on_row_counts() {
    for input in [ARRAY_INPUT, NDJSON_INPUT] {
        let plans = plan_for(input);
        let buffers = load(input, &plans);

        let mut streaming = StreamingSink::new(&plans);
        let mut reader = Cursor::new(input.as_bytes());
        let format = detect_format(&mut reader).expect("format");
        for_each_document(reader, format, |_, doc| write_document(&mut streaming, doc))
            .expect("write");
        let streamed = streaming.finish();

        for (path, buffer) in buffers.tables() {
            let streamed_rows = streamed
                .iter()
                .filter(|(p, _)| *p == path.to_string())
                .count();
            assert_eq!(
                streamed_rows,
                buffer.row_count(),
                "row count mismatch for {path}"
            );
        }
    }
}

#[test]
fn plan_from_documents_matches_the_two_step_form() {
    let docs: Vec<serde_json::Map<String, Value>> = serde_json::from_str::<Vec<Value>>(ARRAY_INPUT)
        .unwrap()
        .into_iter()
        .map(|v| v.as_object().unwrap().clone())
        .collect();

    let one_step = StatsCollector::plan_from_documents(docs.iter());
    let two_step = plan_for(ARRAY_INPUT);

    let names = |plans: &[PlannedTable]| -> Vec<String> {
        let mut out: Vec<String> = plans
            .iter()
            .map(|p| format!("{}:{}", p.path, p.columns.len()))
            .collect();
        out.sort();
        out
    };
    assert_eq!(names(&one_step), names(&two_step));
}

#[test]
fn missing_plan_for_a_path_is_an_error_not_a_panic() {
    let plans = plan_for(ARRAY_INPUT);
    // Drop the array child table, then feed a document that needs it.
    let trimmed: Vec<PlannedTable> = plans
        .into_iter()
        .filter(|p| p.path.to_string() != "tags[]")
        .collect();
    let mut sink = ColumnBuffers::new(&trimmed);
    let doc = json!({"id": 1, "tags": ["x"]});
    let err = write_document(&mut sink, doc.as_object().unwrap()).unwrap_err();
    assert!(err.to_string().contains("Missing writer"), "{err}");
}

#[test]
fn integer_values_route_to_a_merged_number_column() {
    let input = r#"[{"v": 1}, {"v": 2.5}, {"v": 3}]"#;
    let plans = plan_for(input);
    let root = plans.iter().find(|p| p.path.is_root()).unwrap();
    let v = root.columns.iter().find(|c| c.name == "v").unwrap();
    assert_eq!(v.ty, SimpleType::Number);

    let buffers = load(input, &plans);
    match buffers
        .table(&TablePath::root())
        .unwrap()
        .columns
        .get("v")
        .unwrap()
    {
        ColumnValues::Double(values) => {
            assert_eq!(values, &vec![Some(1.0), Some(2.5), Some(3.0)])
        }
        other => panic!("unexpected column kind: {other:?}"),
    }
}

/// BUG-134: the generated DDL was unreproducible because the object-link foreign
/// keys came out of a `HashMap` in whatever order that process's hasher seed
/// produced. Ten runs of one binary on one input gave ten different files.
///
/// A test that simply builds twice in one process cannot catch this — the seed is
/// fixed for a process, so both builds agree. The invariant to assert is the one
/// that makes the artefact diffable: within a table, foreign keys are emitted in
/// constraint-name order.
#[test]
fn foreign_keys_are_emitted_in_a_stable_order() {
    // Sibling object properties whose alphabetical order differs from their
    // first-seen order, so an accidental "insertion order" fix would fail too.
    let input = r#"[{"id": 1, "zulu": {"v": 1}, "alpha": {"v": 1}, "mike": {"v": 1},
                     "bravo": {"v": 1}, "yankee": {"v": 1}}]"#;
    let plans = plan_for(input);
    let (_, constraints) = crate::ddl::build_sql_schema(&plans, "doc");

    let fks: Vec<&String> = constraints
        .iter()
        .filter(|stmt| stmt.contains("FOREIGN KEY"))
        .collect();
    assert_eq!(fks.len(), 5, "one per object property: {fks:#?}");

    let names: Vec<&str> = fks
        .iter()
        .map(|stmt| {
            stmt.split("ADD CONSTRAINT ")
                .nth(1)
                .and_then(|rest| rest.split(' ').next())
                .expect("constraint name")
        })
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "foreign keys must be emitted in name order");
}

/// The whole statement list must be reproducible, not just the foreign keys.
#[test]
fn the_generated_ddl_is_identical_for_identical_input() {
    let input = r#"[{"id": 1, "zulu": {"v": 1}, "alpha": {"v": 1},
                     "items": [{"sku": "a", "meta": {"m": 1}}]}]"#;
    let first = crate::ddl::build_sql_schema(&plan_for(input), "doc");
    let second = crate::ddl::build_sql_schema(&plan_for(input), "doc");
    assert_eq!(first, second);
}

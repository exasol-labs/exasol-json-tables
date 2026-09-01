use super::*;
use exasol_udf_sdk::value::Value;

/// A connection that records every statement and answers the few queries the
/// driver makes. Enough to assert the whole SQL sequence without a database.
#[derive(Default)]
struct FakeConnection {
    statements: Vec<String>,
    /// Text returned for a `SELECT <column> FROM <table>` read of a landing table.
    table_text: Option<String>,
    fail_on: Option<String>,
}

impl ExaConnection for FakeConnection {
    fn query_for_each(
        &mut self,
        sql: &str,
        f: &mut dyn FnMut(Vec<Value>) -> Result<(), UdfError>,
    ) -> Result<(), UdfError> {
        self.statements.push(sql.to_string());
        if sql.starts_with("SELECT CURRENT_TIMESTAMP") {
            return f(vec![Value::String("2026-09-01T10:00:00Z".to_string())]);
        }
        if sql.contains("EXA_ALL_COLUMNS") {
            return catalog_row(f);
        }
        let text = self
            .table_text
            .clone()
            .ok_or_else(|| UdfError::User(format!("unexpected query: {sql}")))?;
        f(vec![Value::String(text)])
    }

    fn execute(&mut self, sql: &str) -> Result<u64, UdfError> {
        if let Some(needle) = &self.fail_on {
            if sql.contains(needle.as_str()) {
                return Err(UdfError::User(format!("boom on {needle}")));
            }
        }
        self.statements.push(sql.to_string());
        Ok(if sql.starts_with("INSERT INTO") { 7 } else { 0 })
    }
}

/// The catalog answer for the landing table these tests read from
/// (`table://LAND.DOCS.CHUNK`), so the source pre-check passes.
fn catalog_row(f: &mut dyn FnMut(Vec<Value>) -> Result<(), UdfError>) -> Result<(), UdfError> {
    f(vec![
        Value::String("DOCS".to_string()),
        Value::String("CHUNK".to_string()),
        Value::String("VARCHAR(2000000) UTF8".to_string()),
    ])
}

const DOCS: &str =
    r#"[{"id":1,"hours":{"mon":"9-5"},"tags":["a"]},{"id":2,"hours":{"mon":null},"tags":[]}]"#;

fn options() -> Options {
    Options {
        stem: "orders".to_string(),
        loader: "\"JSON_TABLES\".\"LOAD_TABLE\"".to_string(),
        connection_name: "JSON_TABLES_SELF".to_string(),
        replace: false,
        keep_landing: false,
    }
}

fn table_source() -> Source {
    Source::Table {
        schema: "LAND".to_string(),
        table: "DOCS".to_string(),
        column: "CHUNK".to_string(),
        order_by: Some("SEQ".to_string()),
    }
}

/// Drive the ingest and keep the fake so its statement log can be inspected.
fn ingest_recording(source: &Source, opts: Options, table_text: Option<String>) -> Vec<String> {
    struct Shared(
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        Option<String>,
    );
    impl ExaConnection for Shared {
        fn query_for_each(
            &mut self,
            sql: &str,
            f: &mut dyn FnMut(Vec<Value>) -> Result<(), UdfError>,
        ) -> Result<(), UdfError> {
            self.0.lock().expect("log").push(sql.to_string());
            if sql.starts_with("SELECT CURRENT_TIMESTAMP") {
                return f(vec![Value::String("2026-09-01T10:00:00Z".to_string())]);
            }
            if sql.contains("EXA_ALL_COLUMNS") {
                return catalog_row(f);
            }
            let text = self
                .1
                .clone()
                .ok_or_else(|| UdfError::User(format!("unexpected query: {sql}")))?;
            f(vec![Value::String(text)])
        }

        fn execute(&mut self, sql: &str) -> Result<u64, UdfError> {
            self.0.lock().expect("log").push(sql.to_string());
            Ok(if sql.starts_with("INSERT INTO") { 7 } else { 0 })
        }
    }

    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut connection: Box<dyn ExaConnection> = Box::new(Shared(log.clone(), table_text));
    ingest(&mut connection, source, "EJT_ORDERS_SRC", &opts).expect("ingest");
    let statements = log.lock().expect("log").clone();
    statements
}

#[test]
fn the_statement_sequence_is_schema_then_tables_then_loads_then_constraints() {
    let log = ingest_recording(&table_source(), options(), Some(DOCS.to_string()));

    let position = |needle: &str| {
        log.iter()
            .position(|sql| sql.contains(needle))
            .unwrap_or_else(|| panic!("no statement containing {needle}\n{log:#?}"))
    };

    let schema = position("CREATE SCHEMA IF NOT EXISTS \"EJT_ORDERS_SRC\"");
    let create = position("CREATE TABLE \"EJT_ORDERS_SRC\".\"orders\"");
    let load = position("INSERT INTO \"EJT_ORDERS_SRC\".\"orders\"");
    let constraint = position("ADD CONSTRAINT");
    let comment = position("COMMENT ON TABLE");

    assert!(schema < create, "schema before tables");
    assert!(create < load, "tables before loads");
    assert!(load < constraint, "loads before constraints");
    assert!(constraint < comment, "constraints before provenance");
}

#[test]
fn every_planned_table_gets_created_and_loaded() {
    let log = ingest_recording(&table_source(), options(), Some(DOCS.to_string()));
    for table in ["orders", "orders_hours", "orders_tags_arr"] {
        assert!(
            log.iter()
                .any(|sql| sql.contains(&format!("CREATE TABLE \"EJT_ORDERS_SRC\".\"{table}\""))),
            "missing CREATE for {table}"
        );
        assert!(
            log.iter()
                .any(|sql| sql.contains(&format!("INSERT INTO \"EJT_ORDERS_SRC\".\"{table}\""))),
            "missing INSERT for {table}"
        );
    }
}

#[test]
fn the_load_statements_carry_the_plan_and_the_source() {
    let log = ingest_recording(&table_source(), options(), Some(DOCS.to_string()));
    let insert = log
        .iter()
        .find(|sql| sql.contains("INSERT INTO \"EJT_ORDERS_SRC\".\"orders_tags_arr\""))
        .expect("insert");

    assert!(insert.contains("'table://LAND.DOCS.CHUNK'"), "{insert}");
    assert!(insert.contains("'tags[]'"), "{insert}");
    assert!(insert.contains("exasol-json-tables-plan"), "{insert}");
    assert!(insert.contains("'JSON_TABLES_SELF'"), "{insert}");
    // The engine infers the columns from the insert target, so the statement
    // must not declare them.
    assert!(!insert.contains("EMITS"), "{insert}");
}

#[test]
fn the_report_counts_the_rows_the_engine_reported() {
    let mut connection: Box<dyn ExaConnection> = Box::new(FakeConnection {
        table_text: Some(DOCS.to_string()),
        ..Default::default()
    });
    let report = ingest(
        &mut connection,
        &table_source(),
        "EJT_ORDERS_SRC",
        &options(),
    )
    .expect("ingest");

    assert_eq!(report.len(), 3);
    assert!(report.iter().all(|line| line.rows_loaded == 7));
    assert!(report.iter().all(|line| line.status == "loaded"));
    let names: Vec<&str> = report.iter().map(|line| line.table_name.as_str()).collect();
    assert!(names.contains(&"orders"), "{names:?}");
}

#[test]
fn replace_drops_existing_tables_first() {
    let mut opts = options();
    opts.replace = true;
    let log = ingest_recording(&table_source(), opts, Some(DOCS.to_string()));

    let drop = log
        .iter()
        .position(|sql| sql.contains("DROP TABLE IF EXISTS \"EJT_ORDERS_SRC\".\"orders\""))
        .expect("drop");
    let create = log
        .iter()
        .position(|sql| sql.contains("CREATE TABLE \"EJT_ORDERS_SRC\".\"orders\""))
        .expect("create");
    assert!(drop < create, "drop must precede create");
}

#[test]
fn provenance_records_the_original_source_not_the_landing_table() {
    let log = ingest_recording(&table_source(), options(), Some(DOCS.to_string()));
    let comment = log
        .iter()
        .find(|sql| sql.contains("COMMENT ON TABLE"))
        .expect("comment");
    assert!(comment.contains("table://LAND.DOCS.CHUNK"), "{comment}");
    assert!(
        comment.contains("\"sourceConnection\":\"table\""),
        "{comment}"
    );
    assert!(comment.contains("2026-09-01T10:00:00Z"), "{comment}");
}

#[test]
fn a_failed_create_aborts_before_any_load() {
    let mut connection: Box<dyn ExaConnection> = Box::new(FakeConnection {
        table_text: Some(DOCS.to_string()),
        fail_on: Some("CREATE TABLE".to_string()),
        ..Default::default()
    });
    let err = ingest(
        &mut connection,
        &table_source(),
        "EJT_ORDERS_SRC",
        &options(),
    )
    .unwrap_err();
    assert!(
        format!("{err:?}").contains("boom on CREATE TABLE"),
        "{err:?}"
    );
}

#[test]
fn an_empty_source_is_a_user_error() {
    let mut connection: Box<dyn ExaConnection> = Box::new(FakeConnection {
        table_text: Some("   ".to_string()),
        ..Default::default()
    });
    let err = ingest(
        &mut connection,
        &table_source(),
        "EJT_ORDERS_SRC",
        &options(),
    )
    .unwrap_err();
    assert!(matches!(err, UdfError::User(_)), "{err:?}");
}

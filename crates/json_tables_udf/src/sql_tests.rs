use super::*;
use crate::driver::plan_text;

fn plan_of(input: &str, path: &str) -> PlannedTable {
    plan_text(input)
        .expect("plan")
        .into_iter()
        .find(|plan| plan.path.to_string() == path)
        .expect("table")
}

#[test]
fn identifiers_and_literals_survive_quotes() {
    assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
    assert_eq!(quote_literal("it's"), "'it''s'");
    // The contract's own column names contain a pipe, which needs quoting.
    assert_eq!(quote_ident("note|n"), r#""note|n""#);
}

#[test]
fn the_emits_clause_matches_the_plan_column_order() {
    let plan = plan_of(r#"[{"id":1,"note":null,"tags":["a"]}]"#, "root");
    assert_eq!(
        emits_clause(&plan),
        r#"EMITS ("_id" DECIMAL(18,0), "id" DECIMAL(18,0), "note|n" BOOLEAN, "tags|array" DECIMAL(18,0))"#
    );
}

#[test]
fn the_load_statement_targets_the_right_table_and_escapes_its_arguments() {
    let statement = load_statement(
        "EJT_ORDERS_SRC",
        "orders",
        "\"JSON_TABLES\".\"LOAD_TABLE\"",
        &[
            "table://LAND.DOCS".to_string(),
            "root".to_string(),
            "{\"it's\":1}".to_string(),
        ],
    );
    assert_eq!(
        statement,
        r#"INSERT INTO "EJT_ORDERS_SRC"."orders" SELECT "JSON_TABLES"."LOAD_TABLE"('table://LAND.DOCS', 'root', '{"it''s":1}')"#
    );
}

/// The engine derives an emitting function's columns from the insert target and
/// rejects a statement that also declares them, so the INSERT form must not
/// carry an EMITS clause.
#[test]
fn the_load_statement_carries_no_emits_clause() {
    let statement = load_statement("SRC", "orders", "L", &["x".to_string()]);
    assert!(!statement.contains("EMITS"), "{statement}");
}

#[test]
fn core_ddl_is_requalified_into_the_target_schema() {
    assert_eq!(
        qualify_ddl(
            "CREATE TABLE \"orders\" (\n  \"id\" DECIMAL(18,0)\n)",
            "SRC"
        ),
        "CREATE TABLE \"SRC\".\"orders\" (\n  \"id\" DECIMAL(18,0)\n)"
    );
    assert_eq!(
        qualify_ddl(
            "ALTER TABLE \"orders\" ADD CONSTRAINT \"pk_orders\" PRIMARY KEY (\"_id\") DISABLE",
            "SRC"
        ),
        "ALTER TABLE \"SRC\".\"orders\" ADD CONSTRAINT \"pk_orders\" PRIMARY KEY (\"_id\") DISABLE"
    );
    // Anything else is left alone rather than mangled.
    assert_eq!(qualify_ddl("COMMIT", "SRC"), "COMMIT");
}

#[test]
fn trailing_semicolons_are_stripped_for_single_statement_execution() {
    assert_eq!(
        without_trailing_semicolon("CREATE TABLE x ();"),
        "CREATE TABLE x ()"
    );
    assert_eq!(
        without_trailing_semicolon("CREATE TABLE x ()"),
        "CREATE TABLE x ()"
    );
}

#[test]
fn comments_are_quoted_for_sql() {
    let statement = comment_statement("SRC", "orders", r#"COPY provenance {"source":"it's"}"#);
    assert_eq!(
        statement,
        r#"COMMENT ON TABLE "SRC"."orders" IS 'COPY provenance {"source":"it''s"}'"#
    );
}

//! SQL text the loader builds for its connect-back session.

use json_tables_core::contract::{column_sql_type, PlannedTable};

/// Quote an identifier.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a string literal.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The `EMITS (...)` column list for one planned table, in plan order.
///
/// Used for a standalone `SELECT loader(...) EMITS (...)` call; an `INSERT`
/// must not carry it (see [`load_statement`]).
///
/// The loader UDF is told its column order through its plan argument rather than
/// reading it back from this clause, because a UDF cannot see its own declared
/// output columns (see language-container-rs issue #89). Both come from the same
/// plan, so they cannot drift.
pub fn emits_clause(plan: &PlannedTable) -> String {
    let columns: Vec<String> = plan
        .columns
        .iter()
        .filter_map(|column| {
            column_sql_type(column.ty).map(|ty| format!("{} {}", quote_ident(&column.name), ty))
        })
        .collect();
    format!("EMITS ({})", columns.join(", "))
}

/// The `INSERT INTO ... SELECT loader(...)` statement that fills one table.
///
/// The rows travel the engine's own emit channel; only this statement crosses the
/// connect-back session.
///
/// Note the deliberate absence of an `EMITS` clause. Inside an `INSERT`, Exasol
/// infers an emitting function's output columns from the target table and rejects
/// a call that also specifies them:
///
/// > The return arguments for EMITS functions are inferred from the table to
/// > insert into. Specification of EMITS is not allowed in this case.
///
/// The loader therefore has to emit values in the target table's column order,
/// which holds because the table's DDL and the loader's plan come from the same
/// inferred family.
pub fn load_statement(
    target_schema: &str,
    table_name: &str,
    loader: &str,
    args: &[String],
) -> String {
    let arguments: Vec<String> = args.iter().map(|arg| quote_literal(arg)).collect();
    format!(
        "INSERT INTO {}.{} SELECT {}({})",
        quote_ident(target_schema),
        quote_ident(table_name),
        loader,
        arguments.join(", ")
    )
}

/// `COMMENT ON TABLE` for a provenance comment, qualified to the target schema.
pub fn comment_statement(target_schema: &str, table_name: &str, comment: &str) -> String {
    format!(
        "COMMENT ON TABLE {}.{} IS {}",
        quote_ident(target_schema),
        quote_ident(table_name),
        quote_literal(comment)
    )
}

/// Re-qualify a core DDL statement into the target schema.
///
/// The core emits `CREATE TABLE "name" (...)` and `ALTER TABLE "name" ...`
/// without a schema, which is what the CLI wants. In the database the loader
/// runs against a named schema, so the leading identifier is qualified rather
/// than relying on session state.
pub fn qualify_ddl(statement: &str, target_schema: &str) -> String {
    for keyword in ["CREATE TABLE ", "ALTER TABLE "] {
        if let Some(rest) = statement.strip_prefix(keyword) {
            return format!("{keyword}{}.{rest}", quote_ident(target_schema));
        }
    }
    statement.to_string()
}

/// Strip the trailing semicolon the core's DDL carries; the connect-back session
/// executes one statement per call.
pub fn without_trailing_semicolon(statement: &str) -> &str {
    statement.trim_end().trim_end_matches(';')
}

#[cfg(test)]
#[path = "sql_tests.rs"]
mod tests;

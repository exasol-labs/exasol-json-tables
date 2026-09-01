//! Exasol DDL for a planned table family.
//!
//! Shared by the CLI (which writes a `.sql` artifact and runs the statements
//! over a driver) and by any in-database loader that has to create its own
//! tables through a connect-back session.

use std::collections::HashMap;

use crate::contract::{
    column_sql_type, sanitize_ident, table_sql_name, table_token, PathKind, PlannedTable,
    PropertyColumns,
};

/// `CREATE TABLE` statements plus the constraint statements to apply afterwards.
///
/// Constraints are emitted `DISABLE` so they stay relationship metadata instead
/// of depending on the session's `CONSTRAINT_STATE_DEFAULT`.
pub fn build_sql_schema(plans: &[PlannedTable], stem: &str) -> (Vec<String>, Vec<String>) {
    let mut name_map = HashMap::new();
    let mut token_map = HashMap::new();
    for plan in plans {
        name_map.insert(plan.path.clone(), table_sql_name(&plan.path, stem));
        token_map.insert(plan.path.clone(), table_token(&plan.path, stem));
    }

    let mut create_stmts = Vec::new();
    let mut pk_stmts = Vec::new();
    let mut fk_stmts = Vec::new();

    for plan in plans {
        let table_name = name_map
            .get(&plan.path)
            .cloned()
            .unwrap_or_else(|| sanitize_ident("table"));
        let table_token = token_map
            .get(&plan.path)
            .cloned()
            .unwrap_or_else(|| "table".to_string());

        let mut columns: Vec<String> = Vec::new();
        for col in &plan.columns {
            if let Some(sql_ty) = column_sql_type(col.ty) {
                let nn = if col.is_required || col.is_null_mask {
                    " NOT NULL"
                } else {
                    ""
                };
                columns.push(format!("  {} {}{}", sanitize_ident(&col.name), sql_ty, nn));
            }
        }

        let create_stmt = format!("CREATE TABLE {} (\n{}\n);", table_name, columns.join(",\n"));
        create_stmts.push(create_stmt);

        let mut pk_cols: Vec<String> = Vec::new();
        let has_id = plan.columns.iter().any(|c| c.name == "_id");
        if has_id {
            pk_cols.push(sanitize_ident("_id"));
        } else if plan.kind == PathKind::Array {
            pk_cols.push(sanitize_ident("_parent"));
            pk_cols.push(sanitize_ident("_pos"));
        }
        if !pk_cols.is_empty() {
            let pk_name = sanitize_ident(&format!("pk_{}", table_token));
            pk_stmts.push(format!(
                "ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({}) DISABLE;",
                table_name,
                pk_name,
                pk_cols.join(", ")
            ));
        }

        if plan.columns.iter().any(|c| c.name == "_parent") {
            if let Some(parent_path) = plan.path.parent() {
                if let Some(parent_name) = name_map.get(&parent_path) {
                    let fk_name = sanitize_ident(&format!("fk_{}_parent", table_token));
                    fk_stmts.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) DISABLE;",
                        table_name,
                        fk_name,
                        sanitize_ident("_parent"),
                        parent_name,
                        sanitize_ident("_id")
                    ));
                }
            }
        }

        // Sorted, because `properties` is a `HashMap` and Rust randomises its
        // iteration order per process. Emitting in that order made the generated
        // DDL unreproducible: the same input produced a differently-ordered file
        // on every run, so the artefact could not be checksummed or diffed even
        // though the schema it describes was identical. Constraint names are
        // `fk_<table>_<property>` and the table part is fixed inside this loop, so
        // ordering by property name orders the statements by constraint name.
        let mut properties: Vec<(&String, &PropertyColumns)> = plan.properties.iter().collect();
        properties.sort_by(|(left, _), (right, _)| left.cmp(right));

        for (prop, cols) in properties {
            if let Some(object_fk) = cols.object_fk.as_ref() {
                let child_path = plan.path.child_object(prop);
                if let Some(child_name) = name_map.get(&child_path) {
                    let fk_name = sanitize_ident(&format!("fk_{}_{}", table_token, prop));
                    fk_stmts.push(format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) DISABLE;",
                        table_name,
                        fk_name,
                        sanitize_ident(object_fk),
                        child_name,
                        sanitize_ident("_id")
                    ));
                }
            }
        }
    }

    let mut constraint_stmts = pk_stmts;
    constraint_stmts.extend(fk_stmts);
    (create_stmts, constraint_stmts)
}

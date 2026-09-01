//! Pass 1: observe documents, then derive the table family from what was observed.
//!
//! [`StatsCollector`] takes documents one at a time, so the caller owns the input:
//! a file, an HTTP stream, or rows already inside the database.

use std::collections::HashMap;

use serde_json::Value;

use crate::contract::{
    classify_value, ColumnKind, ColumnPlan, FieldKey, PathKind, PlannedTable, PropertyColumns,
    SimpleType, TablePath,
};

/// Observed property/type counts for one table, in first-seen property order.
#[derive(Debug, Default, Clone)]
pub struct PropertyStats {
    pub counts: HashMap<FieldKey, usize>,
    pub order: Vec<String>,
}

impl PropertyStats {
    pub fn record_value(&mut self, name: &str, value: &Value) {
        let ty = match classify_value(value) {
            Some(t) => t,
            None => return,
        };

        if !self.order.iter().any(|n| n == name) {
            self.order.push(name.to_owned());
        }

        let key = FieldKey {
            name: name.to_owned(),
            ty,
        };
        *self.counts.entry(key).or_insert(0) += 1;
    }

    /// How often `name` was observed with type `ty`.
    pub fn count_of(&self, name: &str, ty: SimpleType) -> usize {
        self.counts
            .get(&FieldKey {
                name: name.to_owned(),
                ty,
            })
            .copied()
            .unwrap_or(0)
    }
}

/// Everything observed about one table in the family.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub path: TablePath,
    pub stats: PropertyStats,
    pub kind: PathKind,
    pub has_nested_array: bool,
}

/// Accumulates statistics across a stream of documents.
///
/// Feed every document through [`StatsCollector::record_document`], then call
/// [`StatsCollector::finish`] for the per-table statistics in stable path order.
#[derive(Debug, Default)]
pub struct StatsCollector {
    tables: HashMap<TablePath, TableStats>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe one root document.
    pub fn record_document(&mut self, document: &serde_json::Map<String, Value>) {
        accumulate_object_stats(&mut self.tables, &TablePath::root(), document);
    }

    /// Per-table statistics, sorted by path so output is deterministic.
    pub fn finish(self) -> Vec<TableStats> {
        let mut result: Vec<TableStats> = self.tables.into_values().collect();
        result.sort_by(|a, b| a.path.to_string().cmp(&b.path.to_string()));
        result
    }

    /// Observe every document and derive the plan in one step.
    pub fn plan_from_documents<'a, I>(documents: I) -> Vec<PlannedTable>
    where
        I: IntoIterator<Item = &'a serde_json::Map<String, Value>>,
    {
        let mut collector = Self::new();
        for document in documents {
            collector.record_document(document);
        }
        build_all_schema_plans(&collector.finish())
    }
}

pub(crate) fn get_or_create_table<'a>(
    tables: &'a mut HashMap<TablePath, TableStats>,
    path: &TablePath,
) -> &'a mut TableStats {
    tables.entry(path.clone()).or_insert_with(|| TableStats {
        path: path.clone(),
        stats: PropertyStats::default(),
        kind: if path.is_root() {
            PathKind::Object
        } else {
            path.segments
                .last()
                .map(|s| s.kind.clone())
                .unwrap_or(PathKind::Object)
        },
        has_nested_array: false,
    })
}

pub fn accumulate_object_stats(
    tables: &mut HashMap<TablePath, TableStats>,
    path: &TablePath,
    obj: &serde_json::Map<String, Value>,
) {
    let mut current = get_or_create_table(tables, path);

    for (name, value) in obj {
        match value {
            Value::Object(map) => {
                current.stats.record_value(name, value);
                let child_path = path.child_object(name);
                let _ = current;
                accumulate_object_stats(tables, &child_path, map);
                current = get_or_create_table(tables, path);
            }
            Value::Array(arr) => {
                current.stats.record_value(name, value);
                let child_path = path.child_array(name);
                let _ = current;
                accumulate_array_stats(tables, &child_path, arr);
                current = get_or_create_table(tables, path);
            }
            _ => {
                current.stats.record_value(name, value);
            }
        }
    }
}

pub fn accumulate_array_stats(
    tables: &mut HashMap<TablePath, TableStats>,
    path: &TablePath,
    arr: &[Value],
) {
    let mut current = get_or_create_table(tables, path);
    current.kind = PathKind::Array;

    for value in arr {
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    match v {
                        Value::Object(child) => {
                            current.stats.record_value(k, v);
                            let child_path = path.child_object(k);
                            let _ = current;
                            accumulate_object_stats(tables, &child_path, child);
                            current = get_or_create_table(tables, path);
                        }
                        Value::Array(child_arr) => {
                            current.has_nested_array = true;
                            current.stats.record_value(k, v);
                            let child_path = path.child_array(k);
                            let _ = current;
                            accumulate_array_stats(tables, &child_path, child_arr);
                            current = get_or_create_table(tables, path);
                        }
                        _ => {
                            current.stats.record_value(k, v);
                        }
                    }
                }
            }
            Value::Array(child_arr) => {
                current.has_nested_array = true;
                current.stats.record_value("value", value);
                let child_path = path.child_array("value");
                let _ = current;
                accumulate_array_stats(tables, &child_path, child_arr);
                current = get_or_create_table(tables, path);
            }
            _ => {
                current.stats.record_value("value", value);
            }
        }
    }
}

/// Turn observed statistics into the planned table family.
pub fn build_all_schema_plans(table_stats: &[TableStats]) -> Vec<PlannedTable> {
    table_stats
        .iter()
        .map(|t| {
            let needs_child_id = t.kind == PathKind::Array && t.has_nested_array;
            let include_id = match t.kind {
                PathKind::Object => true,
                PathKind::Array => needs_child_id,
            };
            let (columns, properties) =
                build_schema_plan(&t.stats, include_id, t.kind == PathKind::Array);
            PlannedTable {
                path: t.path.clone(),
                kind: t.kind.clone(),
                columns,
                properties,
                has_nested_array: t.has_nested_array,
            }
        })
        .collect()
}

pub fn build_schema_plan(
    stats: &PropertyStats,
    include_id: bool,
    is_array_table: bool,
) -> (Vec<ColumnPlan>, HashMap<String, PropertyColumns>) {
    // Heuristics:
    // 1) Single observed non-null type -> main column with original name.
    // 2) Multiple non-null types -> most frequent becomes main column; others become "<name>|<type>" alternates.
    // 3) If explicit nulls are observed -> add "<name>|n" bool column to mark them.
    //    In JSON there is a distinction between non-existant values and explict nulls, so we need to record this,
    //    even when a property is observed only as explicit null.
    // 4) If both integer and number are observed for a property, merge them into a single number column.
    // Nested/array values are dropped earlier in `classify_value` (objects only trigger subtable creation).
    let mut per_property: HashMap<String, HashMap<SimpleType, usize>> = HashMap::new();
    for (key, count) in &stats.counts {
        per_property
            .entry(key.name.clone())
            .or_default()
            .insert(key.ty, *count);
    }

    let mut columns = Vec::new();
    let mut properties: HashMap<String, PropertyColumns> = HashMap::new();

    // Add identifiers first: for array tables, include required _parent/_pos, and optional _id if requested.
    if include_id {
        columns.push(ColumnPlan {
            name: "_id".to_string(),
            ty: SimpleType::Integer,
            is_null_mask: false,
            is_required: true,
            kind: ColumnKind::Primary {
                property: "_id".to_string(),
                main_type: SimpleType::Integer,
            },
        });
    }
    if is_array_table {
        columns.push(ColumnPlan {
            name: "_parent".to_string(),
            ty: SimpleType::Integer,
            is_null_mask: false,
            is_required: true,
            kind: ColumnKind::Primary {
                property: "_parent".to_string(),
                main_type: SimpleType::Integer,
            },
        });
        columns.push(ColumnPlan {
            name: "_pos".to_string(),
            ty: SimpleType::Integer,
            is_null_mask: false,
            is_required: true,
            kind: ColumnKind::Primary {
                property: "_pos".to_string(),
                main_type: SimpleType::Integer,
            },
        });
    }

    // Respect first-seen property order as best we can (PK already added first).
    let mut properties_in_order: Vec<(String, HashMap<SimpleType, usize>)> = Vec::new();
    for name in &stats.order {
        if let Some(counts) = per_property.remove(name) {
            properties_in_order.push((name.clone(), counts));
        }
    }
    // Any remaining (unlikely) go at the end in name order for determinism.
    let mut remaining: Vec<_> = per_property.into_iter().collect();
    remaining.sort_by(|a, b| a.0.cmp(&b.0));
    properties_in_order.extend(remaining);

    for (property, mut type_counts) in properties_in_order {
        let base_name = if is_array_table && property == "value" {
            "_value".to_string()
        } else {
            property.clone()
        };
        let object_count = type_counts.remove(&SimpleType::Object).unwrap_or(0);
        let array_count = type_counts.remove(&SimpleType::Array).unwrap_or(0);
        // Merge integer + number into number without losing either count.
        if type_counts.contains_key(&SimpleType::Integer)
            && type_counts.contains_key(&SimpleType::Number)
        {
            let merged = type_counts[&SimpleType::Integer] + type_counts[&SimpleType::Number];
            type_counts.insert(SimpleType::Number, merged);
            type_counts.remove(&SimpleType::Integer);
        }

        let null_count = type_counts.get(&SimpleType::Null).copied().unwrap_or(0);
        let mut prop_columns = PropertyColumns {
            array_count: None,
            ..Default::default()
        };

        let mut has_any = false;

        if object_count > 0 {
            let object_col = format!("{base_name}|object");
            columns.push(ColumnPlan {
                name: object_col.clone(),
                ty: SimpleType::Integer,
                is_null_mask: false,
                is_required: false,
                kind: ColumnKind::Primary {
                    property: property.clone(),
                    main_type: SimpleType::Integer,
                },
            });
            prop_columns.object_fk = Some(object_col);
            has_any = true;
        }

        if array_count > 0 {
            let primary_name = format!("{base_name}|array");
            columns.push(ColumnPlan {
                name: primary_name.clone(),
                ty: SimpleType::Integer,
                is_null_mask: false,
                is_required: false,
                kind: ColumnKind::Primary {
                    property: property.clone(),
                    main_type: SimpleType::Integer,
                },
            });

            prop_columns.array_count = Some(primary_name);
            has_any = true;
        }

        // Exclude null when choosing a main type; if only nulls exist, keep only the null mask.
        let mut typed: Vec<(SimpleType, usize)> = type_counts
            .iter()
            .filter(|(ty, _)| **ty != SimpleType::Null && **ty != SimpleType::Object)
            .map(|(ty, count)| (*ty, *count))
            .collect();

        if !typed.is_empty() {
            typed.sort_by(|(ty_a, count_a), (ty_b, count_b)| {
                count_b.cmp(count_a).then_with(|| ty_a.cmp(ty_b))
            });

            let main_type = typed[0].0;

            columns.push(ColumnPlan {
                name: base_name.clone(),
                ty: main_type,
                is_null_mask: false,
                is_required: false,
                kind: ColumnKind::Primary {
                    property: property.clone(),
                    main_type,
                },
            });

            prop_columns.main_type = Some(main_type);
            prop_columns.primary = Some(base_name.clone());
            has_any = true;

            for (ty, _) in typed.into_iter().skip(1) {
                let alt_name = format!("{base_name}|{ty}");
                columns.push(ColumnPlan {
                    name: alt_name.clone(),
                    ty,
                    is_null_mask: false,
                    is_required: false,
                    kind: ColumnKind::Alternate {
                        property: property.clone(),
                        source_ty: ty,
                    },
                });
                prop_columns.alternates.insert(ty, alt_name);
            }
        }

        if null_count > 0 {
            let null_mask_name = format!("{base_name}|n");
            columns.push(ColumnPlan {
                name: null_mask_name.clone(),
                ty: SimpleType::Bool,
                is_null_mask: true,
                is_required: false,
                kind: ColumnKind::NullBitmask {
                    property: property.clone(),
                },
            });
            prop_columns.null_mask = Some(null_mask_name);
        }

        if has_any || prop_columns.null_mask.is_some() {
            properties.insert(property, prop_columns);
        }
    }

    (columns, properties)
}

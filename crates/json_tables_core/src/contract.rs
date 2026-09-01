//! The shared table contract: paths, types, and the column plan every stage agrees on.
//!
//! These types are the seam between ingest, query and reshape. See
//! [`docs/architecture.md`](../../../docs/architecture.md) for the contract itself.

use std::collections::HashMap;
use std::fmt;

use serde_json::Value;

/// The scalar families the contract distinguishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SimpleType {
    Null,
    Bool,
    Integer,
    Number,
    String,
    Object,
    Array,
}

impl fmt::Display for SimpleType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            SimpleType::Null => "null",
            SimpleType::Bool => "bool",
            SimpleType::Integer => "integer",
            SimpleType::Number => "number",
            SimpleType::String => "string",
            SimpleType::Object => "object",
            SimpleType::Array => "array",
        };
        write!(f, "{label}")
    }
}

/// Classify a JSON value into its contract type.
pub fn classify_value(value: &Value) -> Option<SimpleType> {
    match value {
        Value::Null => Some(SimpleType::Null),
        Value::Bool(_) => Some(SimpleType::Bool),
        Value::Number(n) => {
            if n.is_i64() {
                Some(SimpleType::Integer)
            } else if let Some(u) = n.as_u64() {
                if i64::try_from(u).is_ok() {
                    Some(SimpleType::Integer)
                } else {
                    // Preserve large unsigned values by routing them to DOUBLE-backed columns.
                    Some(SimpleType::Number)
                }
            } else {
                Some(SimpleType::Number)
            }
        }
        Value::String(_) => Some(SimpleType::String),
        Value::Object(_) => Some(SimpleType::Object),
        Value::Array(_) => Some(SimpleType::Array),
    }
}

/// One observed property name paired with one observed type.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FieldKey {
    pub name: String,
    pub ty: SimpleType,
}

/// A physical column in the generated table.
#[derive(Debug, Clone)]
pub struct ColumnPlan {
    pub name: String,
    pub ty: SimpleType,
    pub is_null_mask: bool,
    pub is_required: bool,
    pub kind: ColumnKind,
}

/// What a column represents in the contract.
#[derive(Debug, Clone)]
pub enum ColumnKind {
    /// The main column for a property, or a structural column (`_id`, `_parent`, `_pos`).
    Primary {
        property: String,
        main_type: SimpleType,
    },
    /// A `<name>|<type>` sibling holding a non-majority scalar variant.
    Alternate {
        property: String,
        source_ty: SimpleType,
    },
    /// A `<name>|n` explicit-null mask.
    NullBitmask { property: String },
}

/// Every column a single JSON property maps onto.
#[derive(Debug, Default, Clone)]
pub struct PropertyColumns {
    pub main_type: Option<SimpleType>,
    pub primary: Option<String>,
    pub object_fk: Option<String>,
    pub null_mask: Option<String>,
    pub array_count: Option<String>,
    pub alternates: HashMap<SimpleType, String>,
}

/// Whether a table came from an object property or an array property.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathKind {
    Object,
    Array,
}

impl PathKind {
    /// The label used in manifests and provenance comments.
    pub fn label(&self) -> &'static str {
        match self {
            PathKind::Object => "object",
            PathKind::Array => "array",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathSegment {
    pub name: String,
    pub kind: PathKind,
}

/// Where a table sits in the document tree. The root document is the empty path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TablePath {
    pub segments: Vec<PathSegment>,
}

/// Percent-encode anything that is not safe in a file-name path component, so
/// dotted and bracketed JSON keys stay unambiguous.
pub fn encode_path_component(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        let ch = byte as char;
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

impl TablePath {
    pub fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn child_object(&self, segment: &str) -> Self {
        let mut segments = self.segments.clone();
        segments.push(PathSegment {
            name: segment.to_owned(),
            kind: PathKind::Object,
        });
        Self { segments }
    }

    pub fn child_array(&self, segment: &str) -> Self {
        let mut segments = self.segments.clone();
        segments.push(PathSegment {
            name: segment.to_owned(),
            kind: PathKind::Array,
        });
        Self { segments }
    }

    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn parent(&self) -> Option<Self> {
        if self.segments.is_empty() {
            None
        } else {
            let mut segments = self.segments.clone();
            segments.pop();
            Some(Self { segments })
        }
    }

    /// The encoded suffix used for staging file names; `None` for the root table.
    pub fn file_suffix(&self) -> Option<String> {
        if self.segments.is_empty() {
            None
        } else {
            let parts: Vec<String> = self
                .segments
                .iter()
                .map(|seg| match seg.kind {
                    PathKind::Object => encode_path_component(&seg.name),
                    PathKind::Array => format!("{}[]", encode_path_component(&seg.name)),
                })
                .collect();
            Some(parts.join("."))
        }
    }
}

impl fmt::Display for TablePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.segments.is_empty() {
            write!(f, "root")
        } else {
            let parts: Vec<String> = self
                .segments
                .iter()
                .map(|seg| match seg.kind {
                    PathKind::Object => seg.name.clone(),
                    PathKind::Array => format!("{}[]", seg.name),
                })
                .collect();
            write!(f, "{}", parts.join("."))
        }
    }
}

/// The inferred layout of one table: its columns and how properties map onto them.
#[derive(Debug, Clone)]
pub struct PlannedTable {
    pub path: TablePath,
    pub kind: PathKind,
    pub columns: Vec<ColumnPlan>,
    pub properties: HashMap<String, PropertyColumns>,
    pub has_nested_array: bool,
}

impl PlannedTable {
    /// The columns a property writes to, if the property is part of the plan.
    pub fn property_columns(&self, property: &str) -> Option<&PropertyColumns> {
        self.properties.get(property)
    }
}

/// Quote an identifier for Exasol SQL.
pub fn sanitize_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

/// A constraint-name-safe token derived from a table path.
pub fn table_token(path: &TablePath, stem: &str) -> String {
    let raw = match path.file_suffix() {
        None => stem.to_string(),
        Some(suffix) => format!("{}_{}", stem, suffix),
    };
    let mut token: String = raw
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    while token.contains("__") {
        token = token.replace("__", "_");
    }
    token.trim_matches('_').to_string()
}

/// The quoted SQL name for a table path.
pub fn table_sql_name(path: &TablePath, stem: &str) -> String {
    sanitize_ident(&table_raw_name(path, stem))
}

/// The unquoted table name for a table path.
pub fn table_raw_name(path: &TablePath, stem: &str) -> String {
    let raw = path
        .file_suffix()
        .map(|s| s.replace("[]", "_arr").replace('.', "_"))
        .unwrap_or_default();
    if raw.is_empty() {
        stem.to_string()
    } else {
        format!("{}_{}", stem, raw)
    }
}

/// The Exasol column type for a scalar family, or `None` for non-physical types.
pub fn column_sql_type(ty: SimpleType) -> Option<&'static str> {
    match ty {
        SimpleType::Bool => Some("BOOLEAN"),
        SimpleType::Integer => Some("DECIMAL(18,0)"),
        SimpleType::Number => Some("DOUBLE"),
        SimpleType::String => Some("VARCHAR(2000000)"),
        SimpleType::Null | SimpleType::Object | SimpleType::Array => None,
    }
}

/// Manifest-shaped type metadata for a column: `(type_name, size, precision, scale)`.
pub fn column_type_metadata(
    column: &ColumnPlan,
) -> (String, Option<u32>, Option<u32>, Option<u32>) {
    match column.ty {
        SimpleType::Bool => ("BOOLEAN".to_string(), None, None, None),
        SimpleType::Integer => ("DECIMAL(18,0)".to_string(), None, Some(18), Some(0)),
        SimpleType::Number => ("DOUBLE".to_string(), None, None, None),
        SimpleType::String => ("VARCHAR(2000000)".to_string(), Some(2_000_000), None, None),
        SimpleType::Null | SimpleType::Object | SimpleType::Array => {
            unreachable!("column plans only contain physical columns")
        }
    }
}

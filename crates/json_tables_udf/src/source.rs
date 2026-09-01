//! Where the JSON comes from.
//!
//! Every source resolves to something the core can frame into documents. The
//! distinction that matters is whether a source can be read **more than once**:
//! the loader needs one pass to infer the schema and one pass per target table to
//! write it. A single-read stream is therefore materialised into a landing table
//! first, and the loads read that instead.

use std::fmt;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::TcpStream;

use exasol_udf_sdk::connect_back::ExaConnection;
use exasol_udf_sdk::error::UdfError;
use exasol_udf_sdk::value::Value;

use crate::sql::{quote_ident, quote_literal};

/// How many characters of source text one landing-table row holds. Comfortably
/// inside Exasol's 2,000,000-character `VARCHAR` ceiling.
const LANDING_CHUNK_CHARS: usize = 1_000_000;

/// A parsed source locator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A file on the BucketFS mount, e.g. `bfs:/buckets/bfsdefault/default/data.json`.
    BucketFs { path: String },
    /// Text held in a table, one document (or one chunk) per row:
    /// `table://SCHEMA.TABLE` or `table://SCHEMA.TABLE.COLUMN`.
    Table {
        schema: String,
        table: String,
        column: String,
        order_by: Option<String>,
    },
    /// A plain-HTTP stream, including Exasol's own bulk tunnel:
    /// `http://host:port/path` or `exatunnel://host:port`.
    Http {
        host: String,
        port: u16,
        path: String,
    },
    /// An object in cloud storage, fetched by the database itself using a named
    /// `CONNECTION`: `s3://bucket/key`, or any `https://` object store URL.
    Cloud { url: String, file: String },
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::BucketFs { path } => write!(f, "{path}"),
            Source::Table {
                schema,
                table,
                column,
                ..
            } => write!(f, "table://{schema}.{table}.{column}"),
            Source::Http { host, port, path } => write!(f, "http://{host}:{port}{path}"),
            Source::Cloud { url, file } => {
                if file.is_empty() {
                    write!(f, "{url}")
                } else {
                    write!(f, "{url}/{file}")
                }
            }
        }
    }
}

impl Source {
    /// The `sourceConnection` kind recorded in provenance comments.
    pub fn connection_kind(&self) -> &'static str {
        match self {
            Source::BucketFs { .. } => "bucketfs",
            Source::Table { .. } => "table",
            Source::Http { .. } => "http-stream",
            Source::Cloud { .. } => "cloud-storage",
        }
    }

    /// Whether the source can be opened again for each load pass.
    pub fn is_rereadable(&self) -> bool {
        match self {
            Source::BucketFs { .. } | Source::Table { .. } => true,
            // A tunnel address serves its bytes once; a cloud object needs the
            // database to fetch it, which is a materialisation either way.
            Source::Http { .. } | Source::Cloud { .. } => false,
        }
    }

    /// Parse a locator.
    pub fn parse(locator: &str) -> Result<Self, UdfError> {
        let locator = locator.trim();
        if locator.is_empty() {
            return Err(user("source must not be empty"));
        }

        if let Some(rest) = locator.strip_prefix("bfs:") {
            return Ok(Source::BucketFs {
                path: rest.to_string(),
            });
        }
        if locator.starts_with("/buckets/") {
            return Ok(Source::BucketFs {
                path: locator.to_string(),
            });
        }
        if let Some(rest) = locator.strip_prefix("table://") {
            return Self::parse_table(rest);
        }
        if let Some(rest) = locator.strip_prefix("exatunnel://") {
            let (host, port) = split_host_port(rest)?;
            return Ok(Source::Http {
                host,
                port,
                path: "/".to_string(),
            });
        }
        if let Some(rest) = locator.strip_prefix("http://") {
            let (authority, path) = match rest.find('/') {
                Some(idx) => (&rest[..idx], &rest[idx..]),
                None => (rest, "/"),
            };
            let (host, port) = split_host_port(authority)?;
            return Ok(Source::Http {
                host,
                port,
                path: path.to_string(),
            });
        }
        if locator.starts_with("s3://") || locator.starts_with("https://") {
            return Ok(Self::parse_cloud(locator));
        }

        Err(user(format!(
            "unsupported source '{locator}': expected bfs:, table://, http://, exatunnel://, s3:// or https://"
        )))
    }

    fn parse_table(rest: &str) -> Result<Self, UdfError> {
        let parts: Vec<&str> = rest.split('.').collect();
        let (schema, table, column) = match parts.as_slice() {
            [schema, table] => (*schema, *table, "DOC"),
            [schema, table, column] => (*schema, *table, *column),
            _ => {
                return Err(user(
                    "table source must be table://SCHEMA.TABLE or table://SCHEMA.TABLE.COLUMN",
                ))
            }
        };
        if schema.is_empty() || table.is_empty() || column.is_empty() {
            return Err(user("table source has an empty identifier"));
        }
        Ok(Source::Table {
            schema: schema.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            order_by: None,
        })
    }

    /// Split a cloud locator into the bucket URL the engine connects to and the
    /// object key it reads, which is how Exasol's `IMPORT ... AT ... FILE ...`
    /// wants them.
    fn parse_cloud(locator: &str) -> Self {
        if let Some(rest) = locator.strip_prefix("s3://") {
            let (bucket, key) = match rest.find('/') {
                Some(idx) => (&rest[..idx], &rest[idx + 1..]),
                None => (rest, ""),
            };
            return Source::Cloud {
                url: format!("https://{bucket}.s3.amazonaws.com"),
                file: key.to_string(),
            };
        }
        // https://host/path/object -> AT 'https://host' FILE 'path/object'
        let rest = locator.trim_start_matches("https://");
        match rest.find('/') {
            Some(idx) => Source::Cloud {
                url: format!("https://{}", &rest[..idx]),
                file: rest[idx + 1..].to_string(),
            },
            None => Source::Cloud {
                url: locator.to_string(),
                file: String::new(),
            },
        }
    }
}

/// Read a source's whole text.
///
/// Re-readable sources are opened on demand; single-read sources must be
/// materialised first (see [`materialise_into_landing`]).
pub fn read_to_string(
    source: &Source,
    connection: Option<&mut Box<dyn ExaConnection>>,
) -> Result<String, UdfError> {
    match source {
        Source::BucketFs { path } => std::fs::read_to_string(path).map_err(|err| {
            user(format!(
                "cannot read '{path}' from the BucketFS mount: {err}"
            ))
        }),
        Source::Http { host, port, path } => http_get(host, *port, path),
        Source::Table {
            schema,
            table,
            column,
            order_by,
        } => {
            let connection = connection.ok_or_else(|| {
                user("a table source needs a database connection; pass a CONNECTION name")
            })?;
            read_table_text(connection, schema, table, column, order_by.as_deref())
        }
        Source::Cloud { .. } => Err(user(
            "a cloud source must be materialised into a landing table before it can be read",
        )),
    }
}

/// Frame a source's text into documents, reusing the core's reader.
pub fn documents_of(
    text: &str,
) -> Result<(json_tables_core::read::InputFormat, Cursor<&[u8]>), UdfError> {
    let mut cursor = Cursor::new(text.as_bytes());
    let format = json_tables_core::read::detect_format(&mut cursor)
        .map_err(|err| user(format!("cannot determine input format: {err}")))?;
    Ok((format, cursor))
}

fn read_table_text(
    connection: &mut Box<dyn ExaConnection>,
    schema: &str,
    table: &str,
    column: &str,
    order_by: Option<&str>,
) -> Result<String, UdfError> {
    let order = match order_by {
        Some(order) => format!(" ORDER BY {}", quote_ident(order)),
        None => String::new(),
    };
    let sql = format!(
        "SELECT {} FROM {}.{}{}",
        quote_ident(column),
        quote_ident(schema),
        quote_ident(table),
        order
    );

    let mut text = String::new();
    let mut rows = 0usize;
    connection.query_for_each(&sql, &mut |row: Vec<Value>| {
        rows += 1;
        match row.first() {
            Some(Value::String(chunk)) => {
                text.push_str(chunk);
                // A row per document needs the separator NDJSON expects; a row
                // per byte-chunk already carries its own newlines, and an extra
                // one between chunks is harmless because blank lines are skipped.
                if !chunk.ends_with('\n') {
                    text.push('\n');
                }
                Ok(())
            }
            Some(Value::Null) | None => Ok(()),
            Some(other) => Err(user(format!(
                "column {column} must be a string, got {other:?}"
            ))),
        }
    })?;

    if rows == 0 {
        return Err(user(format!("table source {schema}.{table} is empty")));
    }
    Ok(text)
}

/// Minimal HTTP/1.1 GET, enough for Exasol's bulk tunnel and internal HTTP
/// sources. Deliberately no TLS: a cloud object goes through the engine instead,
/// which already owns credentials and certificate handling.
fn http_get(host: &str, port: u16, path: &str) -> Result<String, UdfError> {
    let mut stream = TcpStream::connect((host, port))
        .map_err(|err| user(format!("cannot connect to {host}:{port}: {err}")))?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| user(format!("cannot send request to {host}:{port}: {err}")))?;

    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader
        .read_line(&mut status)
        .map_err(|err| user(format!("no response from {host}:{port}: {err}")))?;
    if !status.contains(" 200") {
        return Err(user(format!(
            "{host}:{port} answered '{}'",
            status.trim_end()
        )));
    }

    let mut chunked = false;
    loop {
        let mut header = String::new();
        let read = reader
            .read_line(&mut header)
            .map_err(|err| user(format!("truncated response headers: {err}")))?;
        if read == 0 || header == "\r\n" || header == "\n" {
            break;
        }
        if header
            .to_ascii_lowercase()
            .starts_with("transfer-encoding:")
            && header.to_ascii_lowercase().contains("chunked")
        {
            chunked = true;
        }
    }

    let body = if chunked {
        read_chunked_body(&mut reader)?
    } else {
        let mut body = Vec::new();
        reader
            .read_to_end(&mut body)
            .map_err(|err| user(format!("truncated response body: {err}")))?;
        body
    };

    String::from_utf8(body).map_err(|err| user(format!("source is not valid UTF-8: {err}")))
}

fn read_chunked_body<R: BufRead>(reader: &mut R) -> Result<Vec<u8>, UdfError> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        if reader
            .read_line(&mut size_line)
            .map_err(|err| user(format!("truncated chunk header: {err}")))?
            == 0
        {
            break;
        }
        let size_text = size_line.trim();
        if size_text.is_empty() {
            continue;
        }
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or(""), 16)
            .map_err(|_| user(format!("invalid chunk size '{size_text}'")))?;
        if size == 0 {
            break;
        }
        let mut chunk = vec![0u8; size];
        reader
            .read_exact(&mut chunk)
            .map_err(|err| user(format!("truncated chunk body: {err}")))?;
        body.extend_from_slice(&chunk);
        let mut crlf = String::new();
        let _ = reader.read_line(&mut crlf);
    }
    Ok(body)
}

/// A landing table a single-read source was materialised into.
#[derive(Debug, Clone)]
pub struct Landing {
    pub schema: String,
    pub table: String,
}

impl Landing {
    pub fn as_source(&self) -> Source {
        Source::Table {
            schema: self.schema.clone(),
            table: self.table.clone(),
            column: "CHUNK".to_string(),
            order_by: Some("SEQ".to_string()),
        }
    }

    pub fn drop_statement(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}.{} CASCADE",
            quote_ident(&self.schema),
            quote_ident(&self.table)
        )
    }
}

/// Copy a single-read source into a landing table so every load pass can read it.
///
/// Text streams are chunked and inserted directly — a 50 MB source is ~50 rows,
/// so even the slow SQL-text path costs milliseconds. A cloud object is fetched
/// by the **database**, through the named `CONNECTION`, so no object-store
/// credentials or signing code live in the UDF.
pub fn materialise_into_landing(
    connection: &mut Box<dyn ExaConnection>,
    source: &Source,
    schema: &str,
    table: &str,
    cloud_connection: Option<&str>,
) -> Result<Landing, UdfError> {
    let landing = Landing {
        schema: schema.to_string(),
        table: table.to_string(),
    };
    let qualified = format!(
        "{}.{}",
        quote_ident(&landing.schema),
        quote_ident(&landing.table)
    );
    connection.execute(&landing.drop_statement())?;
    connection.execute(&format!(
        "CREATE TABLE {qualified} (SEQ DECIMAL(18,0), CHUNK VARCHAR(2000000))"
    ))?;

    match source {
        Source::Cloud { url, file } => {
            let connection_name = cloud_connection.ok_or_else(|| {
                user("a cloud source needs a CONNECTION name for its credentials")
            })?;
            // The engine reads the object; 0x01 as a separator that JSON never
            // contains keeps every line one row, and no enclosure means no CSV
            // quoting to undo.
            let file_clause = if file.is_empty() {
                String::new()
            } else {
                format!(" FILE {}", quote_literal(file))
            };
            connection
                .execute(&format!(
                    "IMPORT INTO {qualified} (CHUNK) FROM CSV AT {} USER '' IDENTIFIED BY ''{} \
                 COLUMN SEPARATOR = '0x01' COLUMN DELIMITER = ''",
                    quote_ident(connection_name),
                    file_clause
                ))
                .map_err(|err| {
                    user(format!(
                        "the database could not read {url}{}: {err}",
                        if file.is_empty() {
                            String::new()
                        } else {
                            format!("/{file}")
                        }
                    ))
                })?;
            // Line order is the engine's; give the reader a deterministic order.
            connection.execute(&format!("UPDATE {qualified} SET SEQ = 0 WHERE SEQ IS NULL"))?;
        }
        _ => {
            let text = read_to_string(source, None)?;
            let mut seq = 0i64;
            let mut remaining = text.as_str();
            while !remaining.is_empty() {
                let take = char_boundary(remaining, LANDING_CHUNK_CHARS);
                let (chunk, rest) = remaining.split_at(take);
                connection.execute(&format!(
                    "INSERT INTO {qualified} VALUES ({seq}, {})",
                    quote_literal(chunk)
                ))?;
                seq += 1;
                remaining = rest;
            }
            if seq == 0 {
                return Err(user("source produced no bytes"));
            }
        }
    }

    Ok(landing)
}

/// The largest byte offset up to `max_chars` that lands on a char boundary.
fn char_boundary(text: &str, max_chars: usize) -> usize {
    match text.char_indices().nth(max_chars) {
        Some((idx, _)) => idx,
        None => text.len(),
    }
}

fn split_host_port(authority: &str) -> Result<(String, u16), UdfError> {
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| user(format!("'{authority}' must be host:port")))?;
    let port: u16 = port
        .parse()
        .map_err(|_| user(format!("'{port}' is not a port number")))?;
    if host.is_empty() {
        return Err(user("host must not be empty"));
    }
    Ok((host.to_string(), port))
}

fn user(message: impl Into<String>) -> UdfError {
    UdfError::User(message.into())
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;

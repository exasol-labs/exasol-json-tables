//! Framing a byte stream into JSON documents.
//!
//! The reader is any [`BufRead`], so the same code serves a local file, an HTTP
//! body, or bytes already held in memory.

use std::io::BufRead;

use serde_json::Value;

use crate::error::{CoreError, CoreResult};

/// How the input frames its documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    /// One top-level JSON array of objects.
    Array,
    /// One JSON object per line (NDJSON).
    Lines,
}

/// Detect the framing from the first non-whitespace byte.
///
/// Only the leading whitespace is consumed, so the same reader can be handed
/// straight to [`for_each_document`] — which matters for a stream that cannot be
/// rewound or reopened.
pub fn detect_format<R: BufRead>(reader: &mut R) -> CoreResult<InputFormat> {
    loop {
        let (first_non_ws, consumed) = {
            let buffer = reader.fill_buf()?;
            if buffer.is_empty() {
                return Err(CoreError::msg("Input file is empty"));
            }
            match buffer.iter().position(|b| !b.is_ascii_whitespace()) {
                Some(idx) => (Some(buffer[idx]), idx),
                None => (None, buffer.len()),
            }
        };
        reader.consume(consumed);
        if let Some(byte) = first_non_ws {
            return Ok(match byte {
                b'[' => InputFormat::Array,
                _ => InputFormat::Lines,
            });
        }
    }
}

/// Call `f` once per document, with the document's zero-based position.
///
/// NDJSON is streamed line by line. A top-level array is parsed as a whole
/// before iteration, so array input is bounded by available memory; NDJSON is
/// the shape to prefer for large inputs.
pub fn for_each_document<R, F>(reader: R, format: InputFormat, mut f: F) -> CoreResult<()>
where
    R: BufRead,
    F: FnMut(usize, &serde_json::Map<String, Value>) -> CoreResult<()>,
{
    match format {
        InputFormat::Array => {
            let payload: Value = serde_json::from_reader(reader)?;
            let entries = payload
                .as_array()
                .ok_or_else(|| CoreError::msg("Expected top-level JSON array"))?;

            for (idx, entry) in entries.iter().enumerate() {
                let obj = entry.as_object().ok_or_else(|| {
                    CoreError::msg(format!("Entry at index {idx} is not an object"))
                })?;
                f(idx, obj)?;
            }
        }
        InputFormat::Lines => {
            for (line_num, line) in reader.lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(&line)
                    .map_err(|e| CoreError::msg(format!("Line {}: {}", line_num + 1, e)))?;
                let obj = value.as_object().ok_or_else(|| {
                    CoreError::msg(format!("Line {} is not an object", line_num + 1))
                })?;
                f(line_num, obj)?;
            }
        }
    }

    Ok(())
}

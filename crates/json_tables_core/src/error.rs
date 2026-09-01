use std::fmt;

/// Errors produced by the normalisation core.
///
/// Deliberately concrete and `Send + Sync` so a caller can map it onto its own
/// error type — a CLI's `Box<dyn Error>`, or a UDF runtime's error enum.
#[derive(Debug)]
pub enum CoreError {
    /// A structural problem in the input or the plan.
    Message(String),
    /// The input was not valid JSON.
    Json(serde_json::Error),
    /// The input could not be read.
    Io(std::io::Error),
}

impl CoreError {
    pub fn msg(message: impl Into<String>) -> Self {
        CoreError::Message(message.into())
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoreError::Message(message) => write!(f, "{message}"),
            CoreError::Json(err) => write!(f, "invalid JSON: {err}"),
            CoreError::Io(err) => write!(f, "read failed: {err}"),
        }
    }
}

impl std::error::Error for CoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CoreError::Message(_) => None,
            CoreError::Json(err) => Some(err),
            CoreError::Io(err) => Some(err),
        }
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(err: serde_json::Error) -> Self {
        CoreError::Json(err)
    }
}

impl From<std::io::Error> for CoreError {
    fn from(err: std::io::Error) -> Self {
        CoreError::Io(err)
    }
}

pub type CoreResult<T> = Result<T, CoreError>;

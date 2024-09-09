use std::fmt;
use std::io;

#[derive(Debug, PartialEq)]
pub enum MiniSQLError {
    InvalidTable(String),
    InvalidColumn(String),
    InvalidSyntax(String),
    Generic(String),
}

impl fmt::Display for MiniSQLError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MiniSQLError::InvalidTable(msg) => write!(f, "[INVALID_TABLE]: [{}]", msg),
            MiniSQLError::InvalidColumn(msg) => write!(f, "[INVALID_COLUMN]: [{}]", msg),
            MiniSQLError::InvalidSyntax(msg) => write!(f, "[INVALID_SYNTAX]: [{}]", msg),
            MiniSQLError::Generic(msg) => write!(f, "[ERROR]: [{}]", msg),
        }
    }
}

impl From<io::Error> for MiniSQLError {
    fn from(error: io::Error) -> Self {
        MiniSQLError::Generic(error.to_string())
    }
}

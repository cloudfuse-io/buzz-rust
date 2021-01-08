use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use parquet::errors::ParquetError;

/// Result type for operations that could result in an [BuzzError]
pub type Result<T> = result::Result<T, BuzzError>;

/// Buzz error
#[derive(Debug)]
pub enum BuzzError {
    /// Error returned by arrow.
    ArrowError(ArrowError),
    /// Wraps an error from the Parquet crate
    ParquetError(ParquetError),
    /// Wraps an error from the DataFusion crate
    DataFusionError(DataFusionError),
    /// Error associated to I/O operations and associated traits.
    IoError(io::Error),
    /// Error returned on a branch that we know it is possible
    /// but to which we still have no implementation for.
    /// Often, these errors are tracked in our issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in Buzz.
    /// This error should not happen in normal usage of Buzz.
    // BuzzError has internal invariants that we are unable to ask the compiler to check for us.
    // This error is raised when one of those invariants is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid. Examples include
    /// impossible casts, schema inference not possible and non-unique column names.
    Plan(String),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
    /// Error returned when hbee failed
    HBee(String),
    /// Client error
    CloudClient(String),
    /// Error when downloading data from an external source
    Download(String),
}

/// Creates an Internal error from a formatted string
#[macro_export]
macro_rules! internal_err {
    ($($arg:tt)*) => {{
        let reason = format!($($arg)*);
        crate::error::BuzzError::Internal(reason)
    }}
}

/// Creates a NotImplemented error from a formatted string
#[macro_export]
macro_rules! not_impl_err {
    ($($arg:tt)*) => {{
        let reason = format!($($arg)*);
        crate::error::BuzzError::NotImplemented(reason)
    }}
}

/// Checks the predicate, if false return the formatted string
#[macro_export]
macro_rules! ensure {
    ($predicate:expr, $($arg:tt)*) => {
        if !$predicate {
            let reason = format!($($arg)*);
            return Err(crate::error::BuzzError::Internal(reason));
        }
    };
}

impl BuzzError {
    /// Wraps this [BuzzError] as an [Arrow::error::ArrowError].
    pub fn into_arrow_external_error(self) -> ArrowError {
        ArrowError::from_external_error(Box::new(self))
    }

    pub fn internal(reason: &'static str) -> Self {
        return Self::Internal(reason.to_owned());
    }

    pub fn reason(&self) -> String {
        match *self {
            BuzzError::ArrowError(ref desc) => format!("{}", desc),
            BuzzError::ParquetError(ref desc) => format!("{}", desc),
            BuzzError::DataFusionError(ref desc) => format!("{}", desc),
            BuzzError::IoError(ref desc) => format!("{}", desc),
            BuzzError::NotImplemented(ref desc) => format!("{}", desc),
            BuzzError::Internal(ref desc) => format!("{}", desc),
            BuzzError::Plan(ref desc) => format!("{}", desc),
            BuzzError::Execution(ref desc) => format!("{}", desc),
            BuzzError::HBee(ref desc) => format!("{}", desc),
            BuzzError::Download(ref desc) => format!("{}", desc),
            BuzzError::CloudClient(ref desc) => format!("{}", desc),
        }
    }
}

impl From<io::Error> for BuzzError {
    fn from(e: io::Error) -> Self {
        BuzzError::IoError(e)
    }
}

impl From<ArrowError> for BuzzError {
    fn from(e: ArrowError) -> Self {
        BuzzError::ArrowError(e)
    }
}

impl From<ParquetError> for BuzzError {
    fn from(e: ParquetError) -> Self {
        BuzzError::ParquetError(e)
    }
}

impl From<DataFusionError> for BuzzError {
    fn from(e: DataFusionError) -> Self {
        BuzzError::DataFusionError(e)
    }
}

impl Display for BuzzError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            BuzzError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            BuzzError::ParquetError(ref desc) => {
                write!(f, "Parquet error: {}", desc)
            }
            BuzzError::DataFusionError(ref desc) => {
                write!(f, "DataFusion error: {}", desc)
            }
            BuzzError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            BuzzError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            BuzzError::Internal(ref desc) => {
                write!(f, "Internal error: {}", desc)
            }
            BuzzError::Plan(ref desc) => {
                write!(f, "Error during planning: {}", desc)
            }
            BuzzError::Execution(ref desc) => {
                write!(f, "Execution error: {}", desc)
            }
            BuzzError::HBee(ref desc) => {
                // TODO the actual error from the bee should be transfered instead
                // e.g the Download error
                write!(f, "HBee error: {}", desc)
            }
            BuzzError::Download(ref desc) => {
                write!(f, "Download error: {}", desc)
            }
            BuzzError::CloudClient(ref desc) => {
                write!(f, "Cloud client error: {}", desc)
            }
        }
    }
}

impl error::Error for BuzzError {}

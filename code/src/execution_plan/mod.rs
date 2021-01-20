//! Execution plans are implementations of DataFusion's ExecutionPlan trait

mod parquet;
mod stream;

pub use parquet::ParquetExec;
pub use stream::StreamExec;

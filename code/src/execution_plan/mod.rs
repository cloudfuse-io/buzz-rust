//! Execution plans are implementations of DataFusion's ExecutionPlan trait

mod s3_parquet;
mod stream;

pub use s3_parquet::ParquetExec;
pub use stream::StreamExec;

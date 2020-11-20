mod empty;
mod s3_parquet;
mod stream;

pub use empty::EmptyTable;
pub use s3_parquet::ParquetTable;
pub use stream::StreamTable;

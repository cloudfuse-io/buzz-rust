//! Parquet data source

use std::sync::Arc;

use arrow::datatypes::*;

use crate::execution_plan::ParquetExec;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::s3::S3FileAsync;

/// Table-based representation of a `ParquetFile`.
pub struct ParquetTable {
  // path: String,
  // length: u64,
  file: S3FileAsync,
  schema: SchemaRef,
}

impl ParquetTable {
  /// Attempt to initialize a new `ParquetTable` from an s3 file.
  pub fn try_new(file: S3FileAsync) -> Result<Self> {
    let parquet_exec = ParquetExec::try_new(file.clone(), None, 0)?;
    let schema = parquet_exec.schema();
    Ok(Self { schema, file })
  }
}

impl TableProvider for ParquetTable {
  /// Get the schema for this parquet file.
  fn schema(&self) -> SchemaRef {
    self.schema.clone()
  }

  /// Scan the file(s), using the provided projection, and return one BatchIterator per
  /// partition.
  fn scan(
    &self,
    projection: &Option<Vec<usize>>,
    batch_size: usize,
  ) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(ParquetExec::try_new(
      self.file.clone(),
      projection.clone(),
      batch_size,
    )?))
  }
}

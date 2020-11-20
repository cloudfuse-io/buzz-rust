use std::sync::Arc;

use arrow::datatypes::*;

use crate::execution_plan::ParquetExec;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;

use crate::s3::S3FileAsync;

/// Table-based representation of a `ParquetFile`.
pub struct ParquetTable {
    file: S3FileAsync,
    schema: SchemaRef,
}

impl ParquetTable {
    /// Initialize a new `ParquetTable` from an s3 file and an expected schema.
    pub fn new(file: S3FileAsync, schema: SchemaRef) -> Self {
        Self { schema, file }
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
            Arc::clone(&self.schema),
        )?))
    }
}

pub mod s3_parquet;

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use s3_parquet::S3ParquetTable;

/// A table that can be distributed to hbees
/// Implemented as an enum because serialization must be mapped for new implems
pub enum HBeeTable {
    S3Parquet(S3ParquetTable),
}

impl HBeeTable {
    pub fn new_s3_parquet(table: S3ParquetTable) -> Self {
        Self::S3Parquet(table)
    }

    pub async fn start_download(&self) {
        match self {
            HBeeTable::S3Parquet(table) => table.start_download().await,
        }
    }
}

impl TableProvider for HBeeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match self {
            HBeeTable::S3Parquet(table) => table.schema(),
        }
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            HBeeTable::S3Parquet(table) => table.scan(projection, batch_size),
        }
    }

    fn statistics(&self) -> Statistics {
        match self {
            HBeeTable::S3Parquet(table) => table.statistics(),
        }
    }
}

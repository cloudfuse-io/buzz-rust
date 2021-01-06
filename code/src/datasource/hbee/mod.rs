pub mod s3_parquet;

use std::any::Any;
use std::sync::Arc;

use crate::clients::RangeCache;
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use s3_parquet::S3ParquetTable;

/// A table that can be distributed to hbees
/// Implemented as an enum because serialization must be mapped for new implems
pub enum HBeeTable {
    S3Parquet(S3ParquetTable),
}

impl HBeeTable {
    pub fn set_cache(&self, cache: Arc<RangeCache>) {
        match self {
            HBeeTable::S3Parquet(table) => table.set_cache(cache),
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
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            HBeeTable::S3Parquet(table) => table.scan(projection, batch_size, filters),
        }
    }

    fn statistics(&self) -> Statistics {
        match self {
            HBeeTable::S3Parquet(table) => table.statistics(),
        }
    }
}

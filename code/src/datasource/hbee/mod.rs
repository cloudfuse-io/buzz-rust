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

/// Implemented as an enum because serialization must be mapped for new implems
#[derive(Debug)]
pub enum HBeeTableDesc {
    S3Parquet(S3ParquetTable),
}

impl HBeeTableDesc {
    pub fn schema(&self) -> SchemaRef {
        match self {
            HBeeTableDesc::S3Parquet(table) => table.schema(),
        }
    }
}

/// A table that can be distributed to hbees
pub struct HBeeTable {
    desc: Arc<HBeeTableDesc>,
    cache: Arc<RangeCache>,
}

impl HBeeTable {
    pub fn new(desc: Arc<HBeeTableDesc>, cache: Arc<RangeCache>) -> Self {
        Self { desc, cache }
    }

    pub fn description(&self) -> Arc<HBeeTableDesc> {
        Arc::clone(&self.desc)
    }
}

impl TableProvider for HBeeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.desc.schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.desc.as_ref() {
            HBeeTableDesc::S3Parquet(table) => table.scan(
                Arc::clone(&self.cache),
                projection,
                batch_size,
                filters,
                limit,
            ),
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

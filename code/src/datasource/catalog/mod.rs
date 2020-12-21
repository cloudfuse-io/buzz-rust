use std::any::Any;
use std::sync::Arc;

use crate::datasource::HBeeTable;
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

#[derive(Clone)]
pub struct SizedFile {
    pub key: String,
    pub length: u64,
}

/// A specific type of TableProvider that cannot be converted to a physical plan
/// but can be splitted to be distributed to hbees
trait SplittableTable {
    fn split(&self) -> Vec<HBeeTable>;
    fn schema(&self) -> SchemaRef;
    fn statistics(&self) -> Statistics;
}

/// A generic catalog table that wraps splittable tables
pub struct CatalogTable {
    source_table: Box<dyn SplittableTable + Send + Sync>,
}

impl CatalogTable {
    pub fn new(source_table: Box<dyn SplittableTable + Send + Sync>) -> Self {
        Self { source_table }
    }

    pub fn split(&self) -> Vec<HBeeTable> {
        self.source_table.split()
    }
}

impl TableProvider for CatalogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.source_table.schema()
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Catalog table cannot generate an execution plan".to_owned(),
        ))
    }

    fn statistics(&self) -> Statistics {
        self.source_table.statistics()
    }
}

//// Implems ////

pub mod static_catalog;

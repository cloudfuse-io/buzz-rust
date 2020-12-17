use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

/// An expression that defines a date range
/// Should be replaced by a regular DataFusion expr once expression folding is implemented
// pub struct DateExpr {
//     min_date: u64,
//     max_date: u64,
// }

// impl DateExpr {
//     fn overlaps(&self, other: &Self) -> bool {
//         self.min_date <= other.max_date && other.min_date <= self.max_date
//     }
// }

#[derive(Clone)]
pub struct CatalogFile {
    key: String,
    length: u64,
    // expr: DateExpr,
}

/// A catalog table that contains a static list of files.
pub struct StaticCatalogTable {
    schema: SchemaRef,
    region: String,
    bucket: String,
    files: Vec<CatalogFile>,
}

impl StaticCatalogTable {
    /// Initialize a new `StaticCatalogTable` from a schema.
    pub fn new(
        schema: SchemaRef,
        region: String,
        bucket: String,
        files: Vec<CatalogFile>,
    ) -> Self {
        Self {
            schema,
            region,
            bucket,
            files,
        }
    }

    // TODO move this into a trait + add DataFusion expr
    pub fn split(&self) -> Vec<Arc<dyn TableProvider + Send + Sync>> {
        self.files
            .iter()
            .map(|file| -> Arc<dyn TableProvider + Send + Sync> {
                Arc::new(StaticCatalogTable {
                    schema: self.schema().clone(),
                    region: self.region.clone(),
                    bucket: self.bucket.clone(),
                    files: vec![file.clone()],
                })
            })
            .collect::<Vec<_>>()
    }
}

impl TableProvider for StaticCatalogTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        Statistics::default()
    }
}

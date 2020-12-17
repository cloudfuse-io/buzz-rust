use std::any::Any;
use std::sync::Arc;

use crate::catalog::SizedFile;
use arrow::datatypes::*;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

/// A catalog table that contains a static list of files.
pub struct StaticCatalogTable {
    schema: SchemaRef,
    region: String,
    bucket: String,
    files: Vec<SizedFile>,
}

impl StaticCatalogTable {
    /// Initialize a new `StaticCatalogTable` from a schema.
    pub fn new(
        schema: SchemaRef,
        region: String,
        bucket: String,
        files: Vec<SizedFile>,
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

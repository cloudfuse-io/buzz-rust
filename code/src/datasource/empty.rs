use std::sync::Arc;

use arrow::datatypes::*;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

/// A table with a schema but no data.
pub struct EmptyTable {
    schema: SchemaRef,
}

impl EmptyTable {
    /// Initialize a new `EmptyTable` from a schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl TableProvider for EmptyTable {
    /// Get the schema for this parquet file.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Scan the file(s), using the provided projection, and return one BatchIterator per
    /// partition.
    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Empty table cannot generate an execution plan".to_owned(),
        ))
    }
}

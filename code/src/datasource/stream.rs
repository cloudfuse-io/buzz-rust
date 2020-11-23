use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::execution_plan::StreamExec;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;

pub struct StreamTable {
    stream: Mutex<Option<Pin<Box<dyn Stream<Item = RecordBatch> + Send>>>>,
    schema: SchemaRef,
}

impl StreamTable {
    pub fn try_new(
        stream: Pin<Box<dyn Stream<Item = RecordBatch> + Send>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            stream: Mutex::new(Some(stream)),
            schema,
        })
    }
}

impl TableProvider for StreamTable {
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
        match self.stream.lock().unwrap().take() {
            Some(stream) => Ok(Arc::new(StreamExec::try_new(
                stream,
                self.schema(),
                projection.clone(),
                batch_size,
            )?)),
            None => Err(datafusion::error::DataFusionError::Execution(
                "Cannot scan stream source more than once".to_owned(),
            )),
        }
    }
}

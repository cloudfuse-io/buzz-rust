use std::any::Any;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::execution_plan::StreamExec;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;

pub struct ResultTable {
    stream: Mutex<Option<Pin<Box<dyn Stream<Item = RecordBatch> + Send>>>>,
    query_id: String,
    nb_hbee: usize,
    schema: SchemaRef,
}

impl ResultTable {
    pub fn new(query_id: String, nb_hbee: usize, schema: SchemaRef) -> Self {
        Self {
            stream: Mutex::new(None),
            schema,
            nb_hbee,
            query_id,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn nb_hbee(&self) -> usize {
        self.nb_hbee
    }

    pub fn set(&self, stream: Pin<Box<dyn Stream<Item = RecordBatch> + Send>>) {
        self.stream.lock().unwrap().replace(stream);
    }
}

impl TableProvider for ResultTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

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

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

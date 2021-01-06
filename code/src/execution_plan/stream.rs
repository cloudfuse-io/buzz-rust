use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};

use async_trait::async_trait;
use futures::stream::Stream;
use pin_project::pin_project;

pub struct StreamExec {
    stream: Mutex<Option<Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send>>>>,
    schema: SchemaRef,

    projection: Vec<usize>,
    batch_size: usize,
}

impl fmt::Debug for StreamExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamExec")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl StreamExec {
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        Self {
            stream: Mutex::new(Some(stream)),
            schema: Arc::new(projected_schema),
            projection,
            batch_size,
        }
    }
}

#[async_trait]
impl ExecutionPlan for StreamExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        match self.stream.lock().unwrap().take() {
            Some(stream) => Ok(Box::pin(StreamStream {
                schema: self.schema.clone(),
                stream,
            })),
            None => Err(datafusion::error::DataFusionError::Execution(
                "Cannot execute stream execution plan more than once".to_owned(),
            )),
        }
    }
}

#[pin_project]
struct StreamStream<St> {
    schema: SchemaRef,
    #[pin]
    stream: St,
}

impl<St: Stream<Item = ArrowResult<RecordBatch>>> Stream for StreamStream<St> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let inner_res = this.stream.as_mut().poll_next(ctx);
        inner_res
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<St: Stream<Item = ArrowResult<RecordBatch>>> RecordBatchStream for StreamStream<St> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::HCombTable;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableProvider;

    #[tokio::test]
    async fn test_not_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let hcomb_table = HCombTable::new("mock_query_id".to_owned(), 1, schema.clone());
        let batches = vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?];
        hcomb_table.set(Box::pin(futures::stream::iter(
            batches.clone().into_iter().map(|b| Ok(b)),
        )));

        let exec_plan = hcomb_table.scan(&None, 1024)?;

        let results = datafusion::physical_plan::collect(exec_plan).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(format!("{:?}", results), format!("{:?}", batches));
        Ok(())
    }

    #[tokio::test]
    async fn test_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let hcomb_table = HCombTable::new("mock_query_id".to_owned(), 1, schema.clone());
        hcomb_table.set(Box::pin(futures::stream::iter(vec![])));

        let exec_plan = hcomb_table.scan(&Some(vec![0]), 2048)?;

        let results = datafusion::physical_plan::collect(exec_plan).await?;
        assert_eq!(results.len(), 0);
        Ok(())
    }
}

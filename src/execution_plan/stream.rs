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
    stream: Mutex<Option<Pin<Box<dyn Stream<Item = RecordBatch> + Send>>>>,
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
    pub fn try_new(
        stream: Pin<Box<dyn Stream<Item = RecordBatch> + Send>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
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

        Ok(Self {
            stream: Mutex::new(Some(stream)),
            schema: Arc::new(projected_schema),
            projection,
            batch_size,
        })
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

impl<St: Stream<Item = RecordBatch>> Stream for StreamStream<St> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let inner_res = this.stream.as_mut().poll_next(ctx);
        inner_res.map(|opt| opt.map(|item| Ok(item)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<St: Stream<Item = RecordBatch>> RecordBatchStream for StreamStream<St> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        Ok(())
    }
}

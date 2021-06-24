use std::any::Any;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::execution_plan::StreamExec;
use arrow::datatypes::*;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;

#[derive(Debug, Clone)]
pub struct HCombTableDesc {
    query_id: String,
    nb_hbee: usize,
    schema: SchemaRef,
}

impl HCombTableDesc {
    pub fn new(query_id: String, nb_hbee: usize, schema: SchemaRef) -> Self {
        Self {
            query_id,
            nb_hbee,
            schema,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn nb_hbee(&self) -> usize {
        self.nb_hbee
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// A table from a stream of batches that can be executed only once
pub struct HCombTable {
    stream: Mutex<
        Option<Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send + Sync>>>,
    >,
    desc: HCombTableDesc,
}

impl HCombTable {
    pub fn new(
        desc: HCombTableDesc,
        stream: Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send + Sync>>,
    ) -> Self {
        Self {
            stream: Mutex::new(Some(stream)),
            desc,
        }
    }

    pub fn new_empty(desc: HCombTableDesc) -> Self {
        Self::new(desc, Box::pin(futures::stream::iter(vec![])))
    }

    pub fn query_id(&self) -> &str {
        &self.desc.query_id
    }
}

impl TableProvider for HCombTable {
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.stream.lock().unwrap().take() {
            Some(stream) => Ok(Arc::new(StreamExec::new(
                stream,
                self.schema(),
                projection.clone(),
                batch_size,
            ))),
            None => Err(datafusion::error::DataFusionError::Execution(
                "Cannot scan stream source more than once".to_owned(),
            )),
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableProvider;

    #[tokio::test]
    async fn test_not_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let hcomb_table_desc =
            HCombTableDesc::new("mock_query_id".to_owned(), 1, schema.clone());
        let batches = vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?];
        let hcomb_table = HCombTable::new(
            hcomb_table_desc,
            Box::pin(futures::stream::iter(
                batches.clone().into_iter().map(|b| Ok(b)),
            )),
        );

        let exec_plan = hcomb_table.scan(&None, 1024, &[], None)?;

        let results = datafusion::physical_plan::collect(exec_plan).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(format!("{:?}", results), format!("{:?}", batches));
        Ok(())
    }

    #[tokio::test]
    async fn test_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let hcomb_table_desc =
            HCombTableDesc::new("mock_query_id".to_owned(), 1, schema.clone());
        let hcomb_table = HCombTable::new_empty(hcomb_table_desc);

        let exec_plan = hcomb_table.scan(&Some(vec![0]), 2048, &[], None)?;

        let results = datafusion::physical_plan::collect(exec_plan).await?;
        assert_eq!(results.len(), 0);
        Ok(())
    }
}

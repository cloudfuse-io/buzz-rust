use std::sync::Arc;

use crate::datasource::ResultTable;
use crate::error::Result;
use crate::not_impl_err;
use crate::results_service::ResultsService;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan;
use futures::{Stream, StreamExt};

pub struct HCombService {
    results_service: Arc<ResultsService>,
    execution_context: ExecutionContext,
}

impl HCombService {
    pub fn new() -> Self {
        let config = ExecutionConfig::new()
            .with_batch_size(2048)
            .with_concurrency(1);
        Self {
            results_service: Arc::new(ResultsService::new()),
            execution_context: ExecutionContext::with_config(config),
        }
    }

    pub async fn execute_query(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
        println!("HCombService.execute_query()\n{:?}", plan);
        let result_table = Self::find_result_table(&plan)?;
        let batch_stream = self
            .results_service
            .new_query(result_table.query_id().to_owned(), result_table.nb_hbee());
        result_table.set(Box::pin(batch_stream));
        let physical_plan = self.execution_context.create_physical_plan(&plan).unwrap();
        let result_batches = physical_plan::collect(physical_plan).await?;
        Ok(result_batches)
    }

    pub async fn add_results(
        &self,
        query_id: &str,
        batches: impl Stream<Item = RecordBatch>,
    ) {
        let mut batches = Box::pin(batches);
        while let Some(batch) = batches.next().await {
            self.results_service.add_result(&query_id, batch);
        }
        self.results_service.task_finished(&query_id);
    }

    fn find_result_table(plan: &LogicalPlan) -> Result<&ResultTable> {
        let new_inputs = datafusion::optimizer::utils::inputs(&plan);
        if new_inputs.len() > 1 {
            Err(not_impl_err!(
                "Operations with more than one inputs are not supported",
            ))
        } else if new_inputs.len() == 1 {
            // recurse
            Self::find_result_table(new_inputs[0])
        } else {
            if let Some(result_table) = Self::as_result_table(&plan) {
                Ok(result_table)
            } else {
                Err(not_impl_err!("Expected root to be a ResultTable",))
            }
        }
    }

    fn as_result_table<'a>(plan: &'a LogicalPlan) -> Option<&'a ResultTable> {
        if let LogicalPlan::TableScan { source: table, .. } = plan {
            table.as_any().downcast_ref::<ResultTable>()
        } else {
            None
        }
    }
}

use std::sync::Arc;

use crate::datasource::ResultTable;
use crate::error::Result;
use crate::internal_err;
use crate::results_service::ResultsService;
use crate::utils::find_table;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{
    merge::MergeExec, ExecutionPlan, SendableRecordBatchStream,
};
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

    pub async fn execute_query(
        &self,
        plan: LogicalPlan,
    ) -> Result<SendableRecordBatchStream> {
        println!("[hcomb] execute query...");
        let result_table = find_table::<ResultTable>(&plan)?;
        let batch_stream = self
            .results_service
            .new_query(result_table.query_id().to_owned(), result_table.nb_hbee());
        result_table.set(Box::pin(batch_stream));
        let physical_plan = self.execution_context.create_physical_plan(&plan).unwrap();

        // if necessary, merge the partitions
        match physical_plan.output_partitioning().partition_count() {
            0 => Err(internal_err!("Should have at least one partition")),
            1 => physical_plan.execute(0).await.map_err(|e| e.into()),
            _ => {
                // merge into a single partition
                let physical_plan = MergeExec::new(physical_plan.clone());
                assert_eq!(1, physical_plan.output_partitioning().partition_count());
                physical_plan.execute(0).await.map_err(|e| e.into())
            }
        }
    }

    pub async fn add_results(
        &self,
        query_id: &str,
        batches: impl Stream<Item = RecordBatch>,
    ) {
        let mut batches = Box::pin(batches);
        while let Some(batch) = batches.next().await {
            self.results_service.add_result(&query_id, batch);
            println!("self.results_service.add_result(&query_id, batch);");
        }
        self.results_service.task_finished(&query_id);
    }
}
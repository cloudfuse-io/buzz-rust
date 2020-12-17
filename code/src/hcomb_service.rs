use std::sync::Arc;

use crate::results_service::ResultsService;
use arrow::record_batch::RecordBatch;
use datafusion::logical_plan::LogicalPlan;
use futures::{Stream, StreamExt};

pub struct HCombService {
    results_service: Arc<ResultsService>,
}

impl HCombService {
    pub fn new() -> Self {
        Self {
            results_service: Arc::new(ResultsService::new()),
        }
    }

    pub async fn execute_query(&self, plan: LogicalPlan) -> Vec<RecordBatch> {
        println!("HCombService.execute_query()\n{:?}", plan);
        vec![]
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
}

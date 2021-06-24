use std::process::exit;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::results_service::ResultsService;
use crate::datasource::{HCombTable, HCombTableDesc};
use crate::error::{BuzzError, Result};
use crate::internal_err;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::physical_plan::{
    merge::MergeExec, ExecutionPlan, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

pub struct HCombService {
    results_service: Arc<ResultsService>,
    execution_config: ExecutionConfig,
    last_query: Arc<AtomicI64>, // timestamp of the last query in seconds
}

const TASK_EXPIRATION_SEC: i64 = 120;

impl HCombService {
    pub fn new() -> Self {
        let execution_config = ExecutionConfig::new()
            .with_batch_size(2048)
            .with_concurrency(1);
        let last_query = Arc::new(AtomicI64::new(chrono::Utc::now().timestamp()));
        let last_query_ref = Arc::clone(&last_query);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let elapsed = chrono::Utc::now().timestamp()
                    - last_query_ref.load(Ordering::Relaxed);
                if elapsed >= TASK_EXPIRATION_SEC {
                    println!(
                        "[hcomb] task expired after {}s of inactivity, shutting down...",
                        elapsed
                    );
                    exit(0);
                }
            }
        });
        Self {
            results_service: Arc::new(ResultsService::new()),
            execution_config,
            last_query,
        }
    }

    /// Executes the hcomb plan
    /// Returns the query id and the result stream
    pub async fn execute_query(
        &self,
        provider_desc: HCombTableDesc,
        sql: String,
        source: String,
    ) -> Result<(String, SendableRecordBatchStream)> {
        println!("[hcomb] execute query...");
        let mut execution_context =
            ExecutionContext::with_config(self.execution_config.clone());
        self.last_query
            .store(chrono::Utc::now().timestamp(), Ordering::Relaxed);
        let query_id = provider_desc.query_id().to_owned();
        let batch_stream = self
            .results_service
            .new_query(query_id.clone(), provider_desc.nb_hbee());
        let provider = HCombTable::new(provider_desc, Box::pin(batch_stream));
        let physical_plan;
        {
            // limit scope of df because not send so should not overlab await
            let source_ref: &str = &source;
            execution_context.register_table(source_ref, Arc::new(provider));
            let df = execution_context.sql(&sql)?;
            let plan = df.to_logical_plan();
            physical_plan = execution_context.create_physical_plan(&plan)?;
        }

        // if necessary, merge the partitions
        let query_res = match physical_plan.output_partitioning().partition_count() {
            0 => Err(internal_err!("Should have at least one partition")),
            1 => physical_plan.execute(0).await.map_err(|e| e.into()),
            _ => {
                // merge into a single partition
                let physical_plan = MergeExec::new(physical_plan.clone());
                assert_eq!(1, physical_plan.output_partitioning().partition_count());
                physical_plan.execute(0).await.map_err(|e| e.into())
            }
        };
        query_res.map(|res| (query_id.clone(), res))
    }

    pub async fn add_results(
        &self,
        query_id: &str,
        batches: impl Stream<Item = ArrowResult<RecordBatch>>,
    ) {
        let mut batches = Box::pin(batches);
        let mut has_err = false;
        while let Some(batch) = batches.next().await {
            if batch.is_err() {
                has_err = true;
            }
            self.results_service.add_result(&query_id, batch);
        }
        if !has_err {
            self.results_service.task_finished(&query_id);
        }
    }

    pub fn fail(&self, query_id: &str, err: BuzzError) {
        self.results_service.add_result(
            query_id,
            Err(ArrowError::from_external_error(Box::new(err))),
        );
    }
}

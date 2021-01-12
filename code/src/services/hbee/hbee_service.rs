use std::sync::Arc;
use std::time::Instant;

use crate::clients::flight_client;
use crate::clients::RangeCache;
use crate::datasource::HBeeTable;
use crate::error::Result;
use crate::internal_err;
use crate::models::HCombAddress;
use crate::services::utils;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::{merge::MergeExec, ExecutionPlan};

pub struct HBeeService {
    execution_context: ExecutionContext,
    range_cache: Arc<RangeCache>,
}

impl HBeeService {
    pub async fn new() -> Self {
        let config = ExecutionConfig::new()
            .with_batch_size(2048)
            .with_concurrency(1);
        Self {
            execution_context: ExecutionContext::with_config(config),
            range_cache: Arc::new(RangeCache::new().await),
        }
    }
}

impl HBeeService {
    pub async fn execute_query(
        &self,
        query_id: String,
        plan: LogicalPlan,
        address: HCombAddress,
    ) -> Result<()> {
        println!("[hbee] execute query");
        let start = Instant::now();

        let exec_res = match self.query(plan).await {
            Ok(query_res) => flight_client::call_do_put(query_id, &address, query_res)
                .await
                .map_err(|e| internal_err!("Could not do_put hbee result: {}", e)),
            Err(query_err) => {
                flight_client::call_fail_action(
                    query_id,
                    &address,
                    format!("{}", query_err),
                )
                .await
                .map_err(|e| internal_err!("Could not do_action(FAIL) hbee: {}", e))?;
                Err(query_err)
            }
        };
        println!("[hbee] run duration: {}", start.elapsed().as_millis());
        exec_res
    }

    /// Execute the logical plan and collect the results
    /// Collecting the results might increase latency and mem consumption but:
    /// - reduces connection duration from hbee to hcomb, thus decreasing load on hcomb
    /// - allows to collect exec errors at once, effectively choosing between do_put and FAIL action
    async fn query(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
        let plan = self.execution_context.optimize(&plan)?;
        let hbee_table = utils::find_table::<HBeeTable>(&plan)?;
        hbee_table.set_cache(Arc::clone(&self.range_cache));
        let physical_plan = self.execution_context.create_physical_plan(&plan)?;
        // if necessary, merge the partitions
        println!(
            "[hbee] partitions: {}",
            physical_plan.output_partitioning().partition_count()
        );
        let merged_plan = match physical_plan.output_partitioning().partition_count() {
            0 => Err(internal_err!("Should have at least one partition"))?,
            1 => physical_plan,
            _ => {
                // merge into a single partition
                let physical_plan = MergeExec::new(physical_plan.clone());
                assert_eq!(1, physical_plan.output_partitioning().partition_count());
                Arc::new(physical_plan)
            }
        };
        datafusion::physical_plan::collect(merged_plan)
            .await
            .map_err(|e| e.into())
    }
}

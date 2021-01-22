use std::sync::Arc;
use std::time::Instant;

use super::Collector;
use crate::clients::RangeCache;
use crate::datasource::{HBeeTableDesc, HBeeTable};
use crate::error::Result;
use crate::internal_err;
use crate::models::HCombAddress;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::physical_plan::{merge::MergeExec, ExecutionPlan};

pub struct HBeeService {
    execution_context: ExecutionContext,
    range_cache: Arc<RangeCache>,
    collector: Box<dyn Collector>,
}

impl HBeeService {
    pub async fn new(collector: Box<dyn Collector>) -> Self {
        let config = ExecutionConfig::new()
            .with_batch_size(2048)
            .with_concurrency(1);
        Self {
            execution_context: ExecutionContext::with_config(config),
            range_cache: Arc::new(RangeCache::new().await),
            collector,
        }
    }
}

impl HBeeService {
    pub async fn execute_query(
        &mut self,
        query_id: String,
        table: HBeeTableDesc, 
        sql: String,
        source: String,
        address: HCombAddress,
    ) -> Result<()> {
        println!("[hbee] execute query");
        let start = Instant::now();
        let query_res = self.query(table, sql, source).await;
        let cache_stats = self.range_cache.statistics();
        println!("[hbee] query_duration={}, waiting_download_ms={}, downloaded_bytes={}, processed_bytes={}, download_count={}",
            start.elapsed().as_millis(), 
            cache_stats.waiting_download_ms(),
            cache_stats.downloaded_bytes(),
            cache_stats.processed_bytes(),
            cache_stats.download_count(),
        );
        let start = Instant::now();
        let exec_res = self.collector.send_back(query_id, query_res, address).await;
        println!("[hbee] collector duration: {}", start.elapsed().as_millis());
        exec_res
    }

    /// Execute the logical plan and collect the results
    /// Collecting the results might increase latency and mem consumption but:
    /// - reduces connection duration from hbee to hcomb, thus decreasing load on hcomb
    /// - allows to collect exec errors at once, effectively choosing between do_put and FAIL action
    async fn query(&mut self,table: HBeeTableDesc, sql: String, source: String) -> Result<Vec<RecordBatch>> {
        let start = Instant::now();
        let provider = HBeeTable::new(Arc::new(table), Arc::clone(&self.range_cache));
        self.execution_context
            .register_table(&source, Box::new(provider));
        let physical_plan;
        {
            let df = self.execution_context.sql(&sql)?;
            let plan = df.to_logical_plan();
            let plan = self.execution_context.optimize(&plan)?;
            physical_plan = self.execution_context.create_physical_plan(&plan)?;
        }
        println!(
            "[hbee] planning duration: {}, partitions: {}",
            start.elapsed().as_millis(),
            physical_plan.output_partitioning().partition_count()
        );
        // if necessary, merge the partitions
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

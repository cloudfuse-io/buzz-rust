use std::pin::Pin;

use crate::clients::flight_client;
use crate::error::Result;
use crate::internal_err;
use crate::models::HCombAddress;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::logical_plan::LogicalPlan;
use tokio::stream::Stream;

#[async_trait]
pub trait HCombScheduler {
    /// Notifies the hcomb that a query is starting and opens a stream of results.
    async fn schedule(
        &self,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<Pin<Box<dyn Stream<Item = RecordBatch>>>>;
}

pub struct HttpHCombScheduler;

#[async_trait]
impl HCombScheduler for HttpHCombScheduler {
    async fn schedule(
        &self,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<Pin<Box<dyn Stream<Item = RecordBatch>>>> {
        flight_client::call_do_get(address, plan)
            .await
            .map_err(|e| internal_err!("Could not get result from HComb: {}", e))
    }
}

use crate::hcomb_manager::HCombAddress;
use async_trait::async_trait;
use datafusion::logical_plan::LogicalPlan;

#[async_trait]
pub trait HBeeScheduler {
    async fn schedule(&self, address: &HCombAddress, plan: LogicalPlan);
}

// TODO implementations:
// TODO - for docker test
// TODO - for lambd

use crate::clients::lambda::LambdaInvokeClient;
use crate::error::Result;
use crate::internal_err;
use crate::models::{HBeeEvent, HCombAddress, LogicalPlanBytes};
use async_trait::async_trait;
use datafusion::logical_plan::LogicalPlan;
use hyper::{Body, Client, Request};

#[async_trait]
pub trait HBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<()>;
}

pub struct TestHBeeScheduler {
    pub domain: String,
}

#[async_trait]
impl HBeeScheduler for TestHBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<()> {
        let client = Client::new();

        let req_body = serde_json::to_string(&HBeeEvent {
            query_id,
            hcomb_address: address.clone(),
            plan: LogicalPlanBytes::try_new(&plan)?,
        })
        .map_err(|_| internal_err!("failed to serialize to json"))?;

        let req = Request::builder()
            .method("POST")
            .uri(format!("http://{}:3000", self.domain))
            .body(Body::from(req_body))
            .map_err(|_| internal_err!("failed to build hbee request"))?;

        client
            .request(req)
            .await
            .map_err(|e| internal_err!("hbee scheduling failed: {}", e))?;

        Ok(())
    }
}

pub struct LambdaHBeeScheduler {
    client: LambdaInvokeClient,
}

impl LambdaHBeeScheduler {
    pub fn new(region: &str) -> Self {
        Self {
            client: LambdaInvokeClient::new(region),
        }
    }
}

#[async_trait]
impl HBeeScheduler for LambdaHBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        plan: LogicalPlan,
    ) -> Result<()> {
        let req_body = serde_json::to_vec(&HBeeEvent {
            query_id,
            hcomb_address: address.clone(),
            plan: LogicalPlanBytes::try_new(&plan)?,
        })
        .map_err(|_| internal_err!("failed to serialize to json"))?;

        self.client.invoke(req_body).await?;

        Ok(())
    }
}

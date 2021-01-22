use crate::clients::lambda::LambdaInvokeClient;
use crate::datasource::HBeeTableDesc;
use crate::error::Result;
use crate::internal_err;
use crate::models::{HBeeEvent, HBeePlanBytes, HCombAddress};
use async_trait::async_trait;
use hyper::{Body, Client, Request};

#[async_trait]
pub trait HBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        table: &HBeeTableDesc,
        sql: String,
        source: String,
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
        table: &HBeeTableDesc,
        sql: String,
        source: String,
    ) -> Result<()> {
        let client = Client::new();

        let req_body = serde_json::to_string(&HBeeEvent {
            query_id,
            hcomb_address: address.clone(),
            plan: HBeePlanBytes::try_new(&table, sql, source)?,
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
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            client: LambdaInvokeClient::try_new()?,
        })
    }
}

#[async_trait]
impl HBeeScheduler for LambdaHBeeScheduler {
    async fn schedule(
        &self,
        query_id: String,
        address: &HCombAddress,
        table: &HBeeTableDesc,
        sql: String,
        source: String,
    ) -> Result<()> {
        let req_body = serde_json::to_vec(&HBeeEvent {
            query_id,
            hcomb_address: address.clone(),
            plan: HBeePlanBytes::try_new(&table, sql, source)?,
        })
        .map_err(|_| internal_err!("failed to serialize to json"))?;

        self.client.invoke(req_body).await?;

        Ok(())
    }
}
